# Digital-to-Call Leakage Detection

> **Transform raw data into live insights → agentic actions → cost savings**

Using synthetic data (generated member journeys via Python Faker + Databricks notebooks), this project demonstrates how Databricks turns raw data into actionable intelligence for call center optimization.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Data Foundation](#data-foundation)
- [Live Monitoring Pipeline](#live-monitoring-pipeline)
- [Command Center Dashboard](#command-center-dashboard)
- [Demo Flow](#demo-flow)
- [Getting Started](#getting-started)

---

## Overview

This solution detects and quantifies "digital-to-call leakage" — situations where members abandon digital self-service and call the contact center instead, driving up operational costs.

**Key Metric:** Avoidable calls = digital abandons within 30 minutes → call (cost: $8/call)

---

## Key Features

- **Real-time Leakage Detection** - Identify digital failures that result in calls
- **Segment Analysis** - Understand which member groups are most affected
- **Financial Quantification** - Model cost savings from fixes
- **AI-Powered Recommendations** - Next-best-action suggestions with ROI prioritization
- **Natural Language Queries** - Executive self-service via Genie

---

## Architecture

### Data Layers

| Layer | Description | Tables |
|-------|-------------|--------|
| **Bronze** | Raw ingested data | `digital_journeys_raw`, `call_logs_raw`, `member_profiles_raw` |
| **Silver** | Cleaned & enriched data | `digital_journeys_cleaned`, `call_logs_cleaned`, `digital_call_leakage` |
| **Gold** | Dashboard-ready aggregations | `daily_leakage_summary`, `segment_drilldown`, `savings_model` |

---

## Data Foundation

### Ingest Synthetic Data into Databricks Lakehouse

| Data Type | Description |
|-----------|-------------|
| **Digital Logs** | App logins, bill-pay abandons, timestamps |
| **Call Center** | ACD/IVR data: 1.2M post-digital calls, handle time, sentiment |
| **Member Attributes** | Plan type, tenure, segment (MAPD/new vs. tenured) |

**Unity Catalog Tables:** `digital_journeys`, `call_logs`, `member_profiles`

---

## Live Monitoring Pipeline

- **Unity Catalog + Streaming Tables:** Delta Live Tables (DLT) for near-real-time ingestion
- **Genie Query:** Natural language dashboard
  - *Example:* "Show top digital billing failures leaking to calls today"
- **Output:** Live visualization of 14% spike in MAPD segment, ~$50K daily call cost

### Insight Generation

**Insight Scout Agent** (via Databricks Agent Framework/Genie):

- Daily job scans for spikes: *"MAPD billing UI fails up 14%, driving $Y incremental calls."*
- Alert dashboard: Top-3 issues (billing > ID card > pharmacy), trend charts, segment breakdown
- Financial Quant: Model 10% fix = $2.5M annualized savings

---

## Command Center Dashboard

### Executive Summary View

| Issue | Daily Calls | Cost | Segment | Trend |
|-------|-------------|------|---------|-------|
| Billing Abandon → Call | 5K | $40K | MAPD New | +14% |
| ID Card Repeats | 3K | $24K | Spanish | +8% |
| **Total Leakage** | **12K** | **$112K** | - | - |

### Features

- **Journey Playback:** Click `member_id` → timeline (digital fail → call transcript → sentiment drop)
- **Role-based Views:** Tailored dashboards for different stakeholders
- **Exec Q&A via Genie:** "If we fix billing this week, what's upside?" → Instant projection

---

## Actionable AI

### Next-Best-Action Agent

Proposes prioritized fixes with ROI scoring:

| Action | Est. Savings | Effort |
|--------|-------------|--------|
| Update IVR prompt + digital FAQ for MAPD billing | $1.2M/yr | 1 sprint |
| Digital UX Fix | $1.0M/yr | 1 sprint |
| IVR Messaging Update | $300K/yr | 1 week |
| Agent Script Update | $160K/yr | 3 days |

---

## Demo Flow

### 1. "What's breaking right now?"

**Executive asks:** *"Show me where digital journeys are leaking into the call center today."*

**Command Center Response:**

| Journey | Daily Calls | Daily Cost | Segment | Trend |
|---------|-------------|------------|---------|-------|
| Billing: Digital Abandon → Call | 5,000 | $40,000 | MAPD New | +14% |
| ID Card: Download Fail → Call | 3,000 | $24,000 | Spanish | +8% |
| Pharmacy: Refill → Call Escalate | 4,000 | $32,000 | Mixed | +3% |

> *"Your single biggest leak today is billing: 5,000 calls, $40K in cost, and it's spiking."*

---

### 2. "Who is this hurting?"

**Executive asks:** *"Which members are most impacted by the billing problem?"*

**Segment Drill-Down:**

| Segment | Daily Calls | Cost | Repeat Call % | Sentiment (Avg) |
|---------|-------------|------|---------------|-----------------|
| MAPD New | 3,000 | $24,000 | 28% | -0.65 |
| MAPD Tenured | 1,200 | $9,600 | 18% | -0.40 |
| Commercial | 800 | $6,400 | 15% | -0.30 |

> *"MAPD new members are the epicenter: $24K/day, higher repeat calls, and the worst emotion."*

---

### 3. "Show me what this feels like."

**Journey Playback Timeline** for member `M-104382`:

| Time | Event |
|------|-------|
| 09:01 | Mobile app login |
| 09:03 | Billing page open |
| 09:04 | ❌ Error: "Your plan is not eligible for this option" → user abandons |
| 09:18 | Member calls; IVR routes to billing |
| 09:21–09:39 | 18-min agent call; sentiment drops from neutral to strongly negative |
| 09:45 | Post-call survey: "Confusing bill and plan rules" |

> *"This is happening thousands of times a day, for the same pattern."*

---

### 4. "Is this worth fixing now?"

**Financial Impact Model:**

| Metric | Value |
|--------|-------|
| Avoidable billing calls/day | 5,000 |
| Cost per call | $8 |
| Annualized cost | ≈$14.6M |

**Savings at 10% Fix Rate:**
- Daily savings: 500 calls → $4,000/day
- Annual savings: ≈$1.46M
- Repeat calls avoided: 140/day
- NPS lift: +8–10 points (estimated)

> *"Even a modest 10% fix on just this one journey is worth around $1.5M a year."*

---

### 5. "Why is this happening?"

**Root Cause Analysis:**

- 78% of leaks tied to "MAPD eligibility message"
- 82% of affected journeys are mobile
- Spike started 2 weeks ago, aligned with AEP benefit changes

> *"The problem isn't billing broadly. It's a specific eligibility message on mobile for MAPD members."*

---

### 6. "What should my teams do next?"

**Recommended Actions:**

| Priority | Action | Owner | Est. Savings | Effort |
|----------|--------|-------|--------------|--------|
| 1 | Digital UX Fix - Update MAPD billing eligibility message | Digital | $1.0M/yr | 1 sprint |
| 2 | IVR Messaging Update - Proactive MAPD billing message | Comms | $300K/yr | 1 week |
| 3 | Agent Script Update - Simplified explanation macro | Contact Center | $160K/yr | 3 days |

> *"No more guessing which fire to fight first."*

---

### 7. "How do we know it worked?"

**Before/After Impact Tracker:**

| Metric | Before | After |
|--------|--------|-------|
| Calls/day | 5,000 | 4,250 |
| Cost/day | $40K | $34K |
| Repeat call rate | 23% | 18% |

> *"The same command center that found the problem tracks the outcome."*

---

## Getting Started

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Python 3.8+ with `faker` library
- Delta Lake support

### Quick Start

```python
# Run the complete pipeline
%run ./notebooks/04_run_pipeline
```

See the [notebooks/README.md](notebooks/README.md) for detailed documentation on each notebook.

---

## Call to Action

> **"Same flow. Your data. Live in 30 days. This becomes your operational nervous system, built on what you already have."**

---

## License

This project is for demonstration purposes.

## Support

For questions or issues, please open an issue in this repository.