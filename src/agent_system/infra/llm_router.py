from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable
from urllib import request

from ..config import SETTINGS
from ..infra.observability import get_logger

ToolFn = Callable[[dict[str, Any]], dict[str, Any]]


@dataclass
class LLMRouter:
    provider: str = SETTINGS.llm_provider
    conversation_model: str = SETTINGS.llm_model_conversation
    campaign_model: str = SETTINGS.llm_model_campaign
    summary_model: str = SETTINGS.llm_model_summary

    def __post_init__(self) -> None:
        self.logger = get_logger("infra.llm_router")
        self.safe_fallback_reply = "Recebi sua mensagem. Vou acionar um especialista para te ajudar agora."
        self.last_conversation_failed = False

    def conversation_reply(
        self,
        *,
        intent: str,
        customer_name: str | None,
        context_summary: str,
        message: str,
        tools: dict[str, ToolFn] | None = None,
    ) -> str:
        self.last_conversation_failed = False
        fallback = self.safe_fallback_reply
        if not SETTINGS.llm_api_key:
            self.last_conversation_failed = True
            return fallback
        prompt = (
            "Você é um agente comercial. Regras: nunca afirme disponibilidade sem usar stock_lookup; "
            "nunca mencione impostos/fiscal; resposta curta e objetiva."
        )
        try:
            output = self._chat_with_tools(
                model=self.conversation_model,
                system_prompt=prompt,
                user_prompt=f"cliente={customer_name or 'cliente'}\nintent={intent}\ncontext={context_summary}\nmsg={message}",
                tools=tools or {},
            )
            return output or fallback
        except Exception as exc:  # noqa: BLE001
            self.logger.error("llm_conversation_failed", extra={"extra": {"error": str(exc)}})
            self.last_conversation_failed = True
            return fallback

    def campaign_message(self, *, customer_name: str, machine_type: str | None, category: str | None) -> str:
        default = (
            f"Oi {customer_name}, temos oportunidades para {category or 'peças'} em {machine_type or 'seu equipamento'}. "
            "Se não quiser mais mensagens, responda STOP."
        )
        if not SETTINGS.llm_api_key:
            return default
        try:
            message = self._chat_completion(
                model=self.campaign_model,
                system_prompt="Gere mensagem comercial curta em PT-BR e SEMPRE inclua instrução de opt-out STOP.",
                user_prompt=f"cliente={customer_name}; maquina={machine_type or 'N/A'}; categoria={category or 'N/A'}",
            )
            if "STOP" not in message.upper():
                return f"{message.strip()} Responda STOP para não receber novas mensagens."
            return message
        except Exception:
            return default

    def summarize_context(self, *, history: list[dict[str, Any]], previous_summary: str | None = None) -> str:
        base = previous_summary or ""
        if not SETTINGS.llm_api_key:
            return f"{base[:500]} | mensagens={len(history)}"
        try:
            payload = json.dumps(history[-10:], ensure_ascii=True)
            return self._chat_completion(
                model=self.summary_model,
                system_prompt="Resuma em uma linha os pontos de contexto para próxima interação.",
                user_prompt=f"resumo_anterior={base}\nhistorico={payload}",
            )
        except Exception:
            return f"{base[:500]} | mensagens={len(history)}"

    def _chat_with_tools(self, *, model: str, system_prompt: str, user_prompt: str, tools: dict[str, ToolFn]) -> str:
        tool_specs = [
            {
                "type": "function",
                "function": {
                    "name": "stock_lookup",
                    "description": "Consulta estoque por SKU e local.",
                    "parameters": {
                        "type": "object",
                        "properties": {"sku": {"type": "string"}, "location": {"type": "string"}},
                        "required": ["sku"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "crm_upsert_deal",
                    "description": "Cria negócio no CRM",
                    "parameters": {"type": "object", "properties": {"notes": {"type": "string"}}},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "handoff",
                    "description": "Solicita handoff humano",
                    "parameters": {"type": "object", "properties": {"reason": {"type": "string"}}, "required": ["reason"]},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "crm_log_activity",
                    "description": "Registra atividade",
                    "parameters": {"type": "object", "properties": {"message": {"type": "string"}}, "required": ["message"]},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "send_message",
                    "description": "Envia mensagem para OpenLines/CRM",
                    "parameters": {"type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"]},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "schedule_followup",
                    "description": "Agenda follow-up para um alvo",
                    "parameters": {
                        "type": "object",
                        "properties": {"target_id": {"type": "string"}, "when": {"type": "string"}},
                        "required": ["target_id", "when"],
                    },
                },
            },
        ]
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        first = self._chat_completion_raw(model=model, messages=messages, tools=tool_specs)
        choice = first["choices"][0]["message"]
        tool_calls = choice.get("tool_calls", [])
        if not tool_calls:
            return choice.get("content", "")
        for call in tool_calls:
            name = call["function"]["name"]
            args = json.loads(call["function"]["arguments"] or "{}")
            if name in tools:
                result = tools[name](args)
            else:
                result = {"status": "ignored", "reason": "tool_not_bound"}
            messages.append(choice)
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": call["id"],
                    "name": name,
                    "content": json.dumps(result, ensure_ascii=True),
                }
            )
        second = self._chat_completion_raw(model=model, messages=messages, tools=tool_specs)
        return second["choices"][0]["message"].get("content", "")

    def _chat_completion(self, *, model: str, system_prompt: str, user_prompt: str) -> str:
        payload = self._chat_completion_raw(
            model=model,
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            tools=None,
        )
        return payload["choices"][0]["message"]["content"]

    def _chat_completion_raw(self, *, model: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]] | None) -> dict[str, Any]:
        if self.provider != "openai":
            raise RuntimeError(f"unsupported_provider:{self.provider}")
        body = {"model": model, "messages": messages, "temperature": 0.1}
        if tools:
            body["tools"] = tools
            body["tool_choice"] = "auto"
        data = json.dumps(body).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {SETTINGS.llm_api_key}",
            "Content-Type": "application/json",
        }
        req = request.Request(f"{SETTINGS.llm_base_url.rstrip('/')}/chat/completions", data=data, headers=headers, method="POST")
        with request.urlopen(req, timeout=SETTINGS.llm_timeout_seconds) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw)
