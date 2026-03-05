# BRIEF.md

## Objective

Build a proactive AI commercial agent operating through Bitrix24 WhatsApp conversations that can:

* Offer products based on real stock mirrored from Omie
* Run automated outreach campaigns
* Assist sales conversations
* Create deals automatically
* Trigger ERP actions when deals are won
* Report its activity daily

## Business Context

* All customer interaction occurs via WhatsApp inside Bitrix24
* Omie is the ERP responsible for:

  * Stock
  * Financial
  * Fiscal
  * Accounting
* Bitrix24 is responsible for:

  * Commercial pipeline
  * Communication
  * Sales activity

## Success Metrics

* Outreach contacts per day
* Reply rate
* Qualification rate
* Deal conversion
* Revenue generated
* Telemetry activation rate

## Constraints

* Bitrix must not manage taxes or fiscal logic
* Omie remains source of truth for inventory and billing
* Agent must respect opt‑out and contact frequency limits

---

# PRD.md

## Core Capabilities

### 1 Outreach Campaign Engine

The agent runs daily campaigns targeting specific customer lists.

Features:

* Segment by machine type
* Segment by region/store
* Segment by product category

Outputs:

* WhatsApp outreach
* CRM activity logs

### 2 Conversation Sales Agent

Capabilities:

* Understand product catalog
* Check real stock
* Suggest alternatives
* Create deals automatically

### 3 Telemetry Activation Campaign

Input:

* List of machines without telemetry activation

Agent actions:

* Contact customer
* Request required data
* Update CRM
* Move pipeline stages

### 4 Deal Closing Automation

When deal marked WON:

System must:
1 Create order in Omie
2 Trigger invoice
3 Receive invoice ID
4 Update Bitrix deal

### 5 Human Handoff

Triggers:

* Pricing exception
* Customer complaint
* Complex request

System creates:

* Task for salesperson
* Summary
* Conversation context

### 6 Daily Reporting

Every day the agent posts a report:

Metrics:

* Contacts made
* Replies
* Deals created
* Deals won
* Telemetry activations

---

# ARCHITECTURE.md

## System Overview

Bitrix24 = Commercial hub
Omie = ERP operations
AI Agent = Outreach + conversation
Integration Gateway = Data orchestration

### Core Components

1 Campaign Orchestrator
Schedules daily outreach

2 Conversation Agent
Handles WhatsApp interactions

3 CRM Writer
Creates and updates Bitrix entities

4 ERP Sync
Handles Omie communication

5 Reporting Engine
Creates daily reports

### Data Flow

Customer message
→ Bitrix
→ Agent
→ Stock query
→ Offer
→ Deal creation

Deal WON
→ Gateway
→ Omie order
→ Invoice
→ Bitrix update

### Stock Mirror

Nightly full sync
Incremental updates
Local cache for fast lookup

### Database Tables

id_map
Stores entity relationships

stock_snapshot
Cached inventory

campaign_log
Outreach history

conversation_log
Agent messages

### Cronjobs

Daily outreach campaign
Telemetry activation campaign
Stock sync
Daily report generation

### Observability

All events logged
Retry system
Dead letter queue

---

# BACKLOG.md

## Epic 1 Inventory Mirror

* Implement stock sync
* Expose stock query API

## Epic 2 Campaign Engine

* Customer segmentation
* Outreach scheduler
* Template engine

## Epic 3 Sales Agent

* Product recommendations
* Deal creation

## Epic 4 ERP Integration

* Order creation
* Invoice trigger

## Epic 5 Reporting

* Daily metrics
* Campaign dashboards

---

# TEST_PLAN.md

## Test Scenarios

1 Deal Won Integration
Ensure order created once

2 Stock Consistency
Agent must not sell unavailable stock

3 Outreach Limits
Respect contact frequency

4 Handoff
Ensure salesperson receives context

5 Reporting
Daily report must match activity logs

## Failure Handling

Retries with exponential backoff
Dead letter queue for manual inspection

## Acceptance Criteria

System must:

* avoid duplicate ERP orders
* maintain CRM consistency
* produce auditable logs

---

# AGENTS.md

## Overview

This document defines the autonomous agents that operate inside the system.

### Sales Agent

Purpose:
Handle WhatsApp conversations and assist customers during sales.

Capabilities:

* Understand product catalog
* Query stock mirror
* Recommend products
* Create CRM deals
* Advance pipeline stages

Triggers:

* Incoming WhatsApp messages
* Campaign replies

Outputs:

* CRM activities
* Deals
* Handoff requests

### Campaign Agent

Purpose:
Execute proactive outreach campaigns.

Capabilities:

* Segment customer lists
* Generate outreach messages
* Schedule campaign waves
* Track responses

Cronjobs:

* Daily outreach campaigns
* Follow‑up messages

### Telemetry Activation Agent

Purpose:
Activate Case telematics for machines not yet connected.

Workflow:
1 Identify machines without activation
2 Contact customer
3 Request required information
4 Register responses
5 Move CRM pipeline stage

Metrics:

* Customers contacted
* Responses received
* Activations completed

### ERP Agent

Purpose:
Interface with Omie ERP.

Responsibilities:

* Validate mandatory fields
* Create orders
* Trigger invoicing
* Retrieve invoice ID
* Sync status back to CRM

### Reporting Agent

Purpose:
Generate operational visibility.

Daily Tasks:

* Count outreach contacts
* Count replies
* Count deals created
* Count deals won
* Count telemetry activations

Outputs:

* Daily CRM report
* Campaign performance logs

---

# WORKFLOWS.md

## Outreach Campaign Workflow

Cronjob triggers campaign.

Steps:
1 Load segmented customer list
2 Validate opt‑out and frequency rules
3 Send WhatsApp message
4 Log activity in CRM
5 Wait for response

If response received:
→ Sales Agent continues conversation

If no response:
→ Schedule follow‑up

## Sales Conversation Workflow

Customer message received

Agent actions:
1 Identify intent
2 Query product catalog
3 Check stock
4 Recommend item
5 Create deal

If customer accepts:
→ Request required closing data

## Deal Closing Workflow

Deal marked WON

System actions:
1 Validate required fields
2 Send order request to ERP
3 Trigger invoice
4 Receive invoice ID
5 Update CRM deal

## Telemetry Activation Workflow

Cronjob loads machines without activation.

Agent:
1 Contacts customer
2 Requests activation data
3 Updates CRM fields
4 Moves pipeline stage

---

# DATA_MODEL.md

## Core Entities

### Customer

* id
* name
* phone
* region
* machine_list

### Machine

* model
* serial
* year
* telemetry_status

### Product

* sku
* name
* category
* stock_by_location

### Deal

* id
* customer_id
* products
* quantity
* status
* omie_order_id
* omie_invoice_id

### Campaign

* id
* name
* segment
* start_date
* end_date

### Campaign Log

* customer_id
* campaign_id
* message_sent
* response
* outcome

---

# CAMPAIGN_PLAYBOOK.md

## Equipment Parts Campaign

Goal:
Sell spare parts based on installed machine base.

Strategy:

* Segment by machine model
* Offer maintenance kits

Message Example:
"We noticed your machine model {{model}} may benefit from a maintenance kit currently available in stock."

## Telemetry Activation Campaign

Goal:
Increase number of machines connected to telemetry.

Message Example:
"We noticed your machine is not yet connected to the Case telemetry system. We can help activate it quickly."

## Seasonal Promotion Campaign

Goal:
Promote inventory surplus items.

Agent logic:
1 Identify excess inventory
2 Match compatible machines
3 Offer discount

---

# HANDOFF_PROTOCOL.md

## When to Handoff

Conditions:

* Customer requests negotiation
* Customer complaint
* Complex product inquiry
* Financing discussion

## Handoff Steps

1 Create task for salesperson
2 Attach conversation transcript
3 Attach deal information
4 Suggest next action

## Required Context

The handoff must include:

* Customer name
* Machine model
* Requested product
* Conversation summary

## Post Handoff

Salesperson continues conversation inside Bitrix.

Agent stops sending automated messages until task resolved.

---

# CRONJOBS_SPEC.md

## Overview

Defines all automated scheduled tasks executed by the system.

### Stock Synchronization

Frequency: Every 15 minutes

Steps:
1 Pull product updates from Omie
2 Update stock_snapshot table
3 Refresh cache

### Daily Outreach Campaign

Frequency: Every weekday at 09:00

Steps:
1 Load campaign segment
2 Filter opt-out customers
3 Queue WhatsApp outreach messages
4 Log campaign activity

### Follow-up Messages

Frequency: Every weekday at 14:00

Steps:
1 Identify contacts without reply
2 Send follow-up message
3 Update campaign_log

### Telemetry Activation Campaign

Frequency: Daily at 10:00

Steps:
1 Load list of machines without telemetry
2 Contact customers
3 Request activation data
4 Update CRM records

### Daily Performance Report

Frequency: Every day at 18:00

Steps:
1 Aggregate outreach metrics
2 Aggregate sales metrics
3 Generate report
4 Post report in CRM

---

# AGENT_PROMPTS.md

## Sales Agent Prompt

You are a commercial sales assistant operating inside Bitrix CRM conversations.

Rules:

* Always verify stock availability before offering products
* Never invent stock information
* Prefer recommending compatible products based on machine model

Goals:

* Assist customer
* Identify purchase intent
* Create CRM deals

Escalation:
If the conversation involves negotiation, complaints, or financing questions, trigger human handoff.

## Campaign Agent Prompt

You are responsible for proactive outreach campaigns.

Rules:

* Respect opt-out lists
* Respect contact frequency limits
* Personalize messages using machine data

Goals:

* Generate replies
* Create qualified leads

## ERP Agent Prompt

You are responsible for ERP interaction with Omie.

Rules:

* Validate required fields before sending orders
* Ensure idempotency to avoid duplicate orders

Actions:

* Create order
* Trigger invoice
* Return ERP IDs to CRM

## Reporting Agent Prompt

You analyze daily operational metrics.

Goals:

* Produce transparent reports
* Highlight anomalies
* Provide campaign performance insights

---

# INTEGRATION_SPEC.md

## Bitrix Integration

Key Endpoints:

* Deal creation
* Deal update
* Activity creation
* Contact lookup

Webhook Triggers:

* Deal stage change
* New message received

## Omie Integration

Core Operations:

* Product catalog retrieval
* Stock query
* Order creation
* Invoice generation

## Payload Example – Deal Won

Input from Bitrix:

{
"deal_id": "123",
"customer_id": "456",
"products": [
{
"sku": "ABC123",
"qty": 2
}
]
}

Output to Omie:

{
"order": {
"customer": "456",
"items": [
{
"sku": "ABC123",
"quantity": 2
}
]
}
}

Response Handling

System must:

* Save Omie order ID
* Save Omie invoice ID
* Update Bitrix deal fields

## Idempotency

Each integration event must include:

* event_id
* timestamp

System must ignore duplicate events.

## Retry Strategy

Retries follow exponential backoff.

After 5 failures:
Event goes to dead-letter queue.
