# Softprobe Business Vision & Strategic Goals

**Company**: Softprobe
**Date**: 2025-12-31
**Status**: Strategic Vision

---

## Vision Statement

**Build an AI-ready enterprise data lake for any business.**

Softprobe's ambition is to create the richest, most comprehensive dataset that enables LLM-based AI agents to answer any business question. We are not just building an observability platform – we are building the foundational data infrastructure that makes businesses fully transparent and queryable by AI.

---

## Strategic Objectives

### 1. Start with Session Graph (HTTP JSON Bodies)

**Phase 1: Observability Foundation**

Our initial product captures the complete digital behavior of applications:

- **Full HTTP request/response bodies** stored in an efficient, open-source data lake
- **Session-based organization** enabling AI agents to understand complete user journeys
- **Business context extraction** from JSON payloads (user IDs, order IDs, transactions)
- **Real-time troubleshooting** by giving AI agents access to every HTTP interaction

**Why Start Here:**
- Applications generate the most real-time, high-resolution business data
- HTTP bodies contain rich business logic and transaction details
- Session graphs capture user behavior and business process flows
- Immediate value for engineering teams (debugging, troubleshooting)

### 2. Expand to All Business Data

**Phase 2+: Unified Enterprise Data Lake**

Our roadmap extends beyond observability to capture ALL data sources across the business:

#### **Data Source Integration via Fivetran/Airbyte**

| Category | Examples | Business Value |
|----------|----------|----------------|
| **Project Management** | JIRA, Asana, Linear | Product development context, sprint velocity, issue patterns |
| **Customer Data** | Salesforce, HubSpot, Zendesk | Sales pipeline, customer interactions, support tickets |
| **Financial Systems** | Stripe, QuickBooks, Payment Gateways | Revenue, transactions, financial health |
| **Order/Inventory** | Shopify, ERP systems, Warehouse DBs | Order flow, inventory levels, fulfillment |
| **People/HR** | BambooHR, Workday, PTO systems | Team structure, roles, capacity planning |
| **Marketing** | Google Analytics, Ad platforms, Email | Campaign performance, attribution, user acquisition |
| **Code/DevOps** | GitHub, GitLab, CI/CD pipelines | Code changes, deployments, release velocity |
| **Communication** | Slack, Email, Meeting transcripts | Team collaboration, decision-making context |

#### **Unified Schema: Business Knowledge Graph**

All data sources are integrated into a unified knowledge graph:

```
Session Graph (HTTP data) ←→ Order Database ←→ Payment Gateway
        ↓                           ↓                    ↓
    User Profile  ←→  Salesforce CRM  ←→  Support Tickets
        ↓                           ↓                    ↓
    JIRA Issues   ←→  GitHub Commits ←→  Deployment Events
```

**Example AI Agent Query:**

> "Why did order ORD-12345 fail, and what were the related JIRA issues, Slack conversations, and code deployments around that time?"

The AI agent can:
1. Find the session via order ID (from HTTP bodies)
2. Correlate with payment gateway failures (Stripe data)
3. Link to related JIRA bugs filed by customers (JIRA sync)
4. Surface Slack conversations where engineers discussed the issue
5. Identify code commits and deployments that may have caused it (GitHub/CI sync)

---

## Competitive Differentiation

### vs. Traditional Observability Platforms

| Platform | Data Scope | AI-Ready | Open Source | Analytics Use Case |
|----------|------------|----------|-------------|-------------------|
| **Datadog** | Observability only | ❌ No raw data access | ❌ Proprietary | ❌ Limited |
| **New Relic** | Observability only | ❌ No raw data access | ❌ Proprietary | ❌ Limited |
| **Observe Inc** | Observability + some integrations | ⚠️ Partial (Snowflake) | ⚠️ Uses Snowflake | ✅ Yes |
| **Softprobe** | **ALL business data** | ✅ Full raw data access | ✅ Iceberg (OSS) | ✅ Primary use case |

### vs. Data Warehouses (Snowflake, BigQuery)

| Capability | Snowflake/BigQuery | Softprobe |
|------------|-------------------|-----------|
| **Real-time session data** | ❌ Not designed for this | ✅ Core feature |
| **Full HTTP bodies** | ❌ Too expensive | ✅ Optimized storage |
| **Observability** | ❌ Requires separate tools | ✅ Built-in |
| **Business data integration** | ✅ Via ETL | ✅ Via Fivetran/Airbyte |
| **AI agent context** | ⚠️ Static tables only | ✅ Live session + historical |
| **Cost** | 💰💰💰 Expensive | 💰 OSS-based, lower cost |

### vs. Data Lakes (Databricks)

| Capability | Databricks | Softprobe |
|------------|-----------|-----------|
| **Storage format** | ✅ Iceberg/Delta | ✅ Iceberg |
| **Real-time ingestion** | ⚠️ Requires Spark streaming | ✅ Built-in OTLP collector |
| **Session-aware** | ❌ Must build yourself | ✅ Native |
| **Business attribute indexing** | ❌ Manual | ✅ Configurable |
| **Observability integration** | ❌ Separate tools | ✅ Unified |

---

## Why This Matters for AI Agents

### The Problem with Current AI Systems

LLM-based AI agents are only as good as the data they can access:

1. **Fragmented Data**: Business data is scattered across dozens of SaaS tools
2. **No Session Context**: AI can't see what users actually did (HTTP interactions)
3. **Static Snapshots**: Most data warehouses only have periodic snapshots, not real-time behavior
4. **Poor Troubleshooting**: Can't correlate application behavior with business outcomes

### Softprobe's Solution: The Complete Business Context

Our data lake provides AI agents with:

✅ **Real-time session data** - What users are doing RIGHT NOW
✅ **Historical business data** - Customer records, orders, support tickets
✅ **Code & deployment context** - What changed and when
✅ **Team collaboration data** - Slack, JIRA, meetings
✅ **Financial data** - Revenue, payments, refunds
✅ **All queryable via SQL** - Standard interface for AI agents

**Result**: AI agents can answer complex business questions that require correlating multiple data sources across time.

---

## Business Model Implications

### Target Customers

1. **Phase 1: Engineering Teams**
   - Immediate pain: Complex troubleshooting
   - Entry point: OTLP collector + HTTP body storage
   - Value: AI-powered debugging, session replay

2. **Phase 2: Product Teams**
   - Pain: Understanding user behavior + business impact
   - Expansion: Correlate sessions with product analytics, support tickets
   - Value: AI-driven insights on feature usage, conversion funnels

3. **Phase 3: Business Intelligence / Leadership**
   - Pain: Fragmented data, slow insights
   - Expansion: Full business data integration
   - Value: AI agent that answers "What's happening with customer X?" across all systems

### Revenue Streams

1. **Storage (Iceberg on S3/R2)** - Usage-based pricing
2. **Compute (Query/ETL)** - Per-query or compute-hour pricing
3. **Integrations** - Premium connectors (Fivetran/Airbyte licenses)
4. **AI Agent Platform** - Query API for LLM-based agents
5. **Managed Service** - Hosted version vs. self-hosted OSS

### Competitive Moats

1. **Session Graph Expertise** - Unique capability to capture full HTTP context
2. **Unified Schema** - Pre-built knowledge graph connecting all data sources
3. **AI-Native Design** - Built for LLM consumption from day one
4. **Open Source Foundation** - Iceberg, DuckDB, OSS-first approach reduces lock-in fear
5. **Cost Structure** - Object storage + columnar compression = 10x cheaper than Datadog

---

## Technical Architecture Implications

### Design Principles Driven by Business Goals

1. **Store EVERYTHING** (No sampling)
   - Business goal: Complete data for AI agents
   - Technical: Efficient compression (ZSTD), columnar storage (Parquet/Iceberg)

2. **Separated Storage** (Metadata vs. Bodies)
   - Business goal: Fast search without expensive I/O
   - Technical: Business attribute index for lookup, bodies for deep analysis

3. **Open Standards** (Iceberg, OTLP, SQL)
   - Business goal: No vendor lock-in, integrate with existing tools
   - Technical: Standard formats, DuckDB/Spark compatibility

4. **Schema-on-Read** (Flexible transformations)
   - Business goal: Support any business, any schema
   - Technical: Store raw, extract via ETL/SQL

5. **Session-Based Organization**
   - Business goal: AI agents need complete user journey context
   - Technical: Session-aware buffering, co-located storage

### Future Technical Roadmap (Aligned with Business Goals)

**Q1 2026: HTTP Body Storage + Business Attribute Indexing**
- ✅ OTLP collector with full body capture
- ✅ Separated storage (metadata vs. bodies)
- ✅ User-provided `sp.*` attributes for search
- ✅ DuckDB query interface

**Q2 2026: ETL Framework for Custom Extraction**
- Schema-on-demand transformations (Observe-style)
- Pre-built extraction templates (e-commerce, SaaS, travel)
- User-configurable extraction rules

**Q3 2026: First Business Data Integrations**
- Fivetran connector for Stripe (payments)
- Airbyte connector for JIRA (issues)
- Knowledge graph: Session ↔ Order ↔ Payment ↔ JIRA

**Q4 2026: AI Agent Platform**
- Query API for LLM-based agents
- Natural language to SQL translation
- Pre-built agent templates ("Debug order failure", "Analyze user churn")

**2027+: Full Enterprise Data Lake**
- 50+ data source connectors
- Real-time streaming for all sources (not just HTTP)
- Multi-tenant query isolation and access control
- Advanced AI capabilities (predictive analytics, anomaly detection)

---

## Success Metrics

### Phase 1 (Observability)
- **Adoption**: 100+ engineering teams using HTTP body storage
- **Data Volume**: 10TB+ of session data ingested
- **Query Performance**: <500ms for business attribute lookups
- **Customer Feedback**: "Debugging is 10x faster"

### Phase 2 (Unified Data Lake)
- **Integrations**: 10+ data sources connected per customer
- **Cross-Source Queries**: AI agents answering multi-system questions
- **Expansion Revenue**: 3x growth from data integration upsells
- **Customer Feedback**: "Our data is finally in one place"

### Phase 3 (AI-Native Business Intelligence)
- **AI Agent Usage**: Majority of queries from AI agents (not humans)
- **Business Impact**: Customers attribute revenue/cost improvements to AI insights
- **Market Position**: Recognized as "AI-ready data lake" category leader

---

## Why We Will Win

1. **First-Mover Advantage**: No one else combines observability + business data for AI
2. **Technical Excellence**: Iceberg + Parquet + DuckDB = best-in-class performance/cost
3. **Open Source Strategy**: Lower lock-in risk = faster enterprise adoption
4. **AI Timing**: LLM capabilities are mature enough NOW to consume structured data
5. **Product Focus**: Start narrow (HTTP bodies) and expand methodically

---

## Call to Action

Every technical decision should be evaluated against this question:

> **"Does this bring us closer to building the richest dataset for AI agents to understand any business?"**

- If YES → Prioritize
- If NO → Defer or eliminate

Our competitive advantage comes from:
1. **Completeness** - Store everything, not just metrics
2. **Accessibility** - SQL-queryable, open standards
3. **Context** - Session graphs + business data integration
4. **Cost** - OSS-based, 10x cheaper than SaaS alternatives

This is not just an observability platform. This is the **data foundation for AI-powered business intelligence**.

---

## References

- [Design Document](design.md) - Technical architecture and implementation
- [Storage Design Brainstorm](storage_design.md) - HTTP body storage and indexing options
- [OpenSpec Proposal](../openspec/changes/add-iceberg-otlp-migration/proposal.md) - Iceberg migration plan
