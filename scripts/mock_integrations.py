from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        size = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(size) if size else b"{}"
        body = json.loads(raw.decode("utf-8"))
        path = self.path

        if path.startswith("/bitrix/"):
            method = path.split("/bitrix/", 1)[1].strip("/")
            payload = {"result": {}}
            if method == "crm.deal.get":
                payload["result"] = {
                    "ID": body.get("id", "0"),
                    "TITLE": "Cliente Prova",
                    "UF_CRM_CPF_CNPJ": "12345678909",
                    "UF_CRM_PAYMENT_TERMS": "000",
                    "UF_CRM_BRANCH": "MAIN",
                    "UF_CRM_DELIVERY_CITY": "Cuiaba",
                    "UF_CRM_DELIVERY_UF": "MT",
                }
            if method in {"imopenlines.crm.message.add", "imbot.message.add"}:
                payload["result"] = {"message_id": "MSG-1"}
            if method == "crm.deal.update":
                payload["result"] = True
            if method in {"crm.activity.add", "tasks.task.add"}:
                payload["result"] = {"id": "1"}
            self._send(200, payload)
            return

        if path.startswith("/omie/"):
            call = body.get("call")
            if call == "ListarProdutos":
                self._send(
                    200,
                    {
                        "total_de_paginas": 1,
                        "produto_servico_cadastro": [
                            {
                                "codigo": "ABC123",
                                "descricao": "Kit A",
                                "descricao_familia": "parts",
                                "local_estoque": "MAIN",
                                "quantidade_estoque": 8,
                            }
                        ],
                    },
                )
                return
            if call == "ListarPosicoesEstoque":
                self._send(200, {"total_de_paginas": 1, "estoque": [{"codigo": "ABC123", "local_estoque": "MAIN", "quantidade": 8}]})
                return
            if call == "ListarClientes":
                self._send(200, {"clientes_cadastro": []})
                return
            if call == "UpsertCliente":
                self._send(200, {"codigo_cliente_omie": "CLI-1"})
                return
            if call == "IncluirPedido":
                self._send(200, {"codigo_pedido": "PED-1"})
                return
            if call == "FaturarPedido":
                self._send(200, {"codigo_nfe": "NFE-1"})
                return
            self._send(200, {"codigo_status": "0"})
            return

        self._send(404, {"error": "not_found"})

    def _send(self, status: int, payload: dict) -> None:
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format: str, *args) -> None:  # noqa: A003, ANN001
        _ = format, args


if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", 19090), Handler)
    server.serve_forever()
