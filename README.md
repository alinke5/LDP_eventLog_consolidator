# 📊 Lakeflow Declarative Pipeline – Event Log Consolidator

Unify **LAkeflow Declarative Pipelines (LDP)** event logs from multiple pipelines into a single analytics-ready layer for monitoring, troubleshooting, and performance insights.

For reference, Lakeflow Declarative Pipelines was formerly known as Delta Live Tables (DLT). You will see LDP and DLT interchangeably throughout the codebase as "DLT" is still used in the Databricks Event Log table names. 

Author: Alex Linke | Sr. Solutions Architect @ Databricks
---

## 🚀 Overview

By default, every LDP pipeline writes its own event log table.  
That’s fine for debugging a single pipeline but makes it **hard to see the big picture** across dozens of pipelines.

This solution:
- 🔎 **Discovers** event log tables across catalogs & schemas  
- 🗂 **Consolidates** them into one Delta table  
- 📈 **Publishes analytics** tables for success/failure rates, run durations, error analysis, and cluster usage  

---

## 📊 Analytics Tables Produced

- **`dlt_consolidated_events`** — Raw union of all LDP events (with source info)  
- **`dlt_pipeline_runs`** — One row per run: start, end, duration, status  
- **`dlt_performance_metrics`** — Success rates, avg/min/max/P95 durations  
- **`dlt_error_analysis`** — Error counts, sample messages, affected runs  
- **`dlt_duration_trends`** — Daily medians & P95 runtime trends  
- **`dlt_recent_activity`** — Rolling N-day window of runs  
- **`dlt_cluster_usage`** — Distinct clusters used per pipeline  

---

## 🏗️ Architecture
- Multiple Pipelines → Event Logs → Consolidator → Analytics Tables


1. **Discovery** → Find all event log tables  
2. **Consolidation** → Union into a uniform schema  
3. **Enrichment** → Extract metadata (IDs, names, clusters, updates)  
4. **Analytics** → Build performance, error, and trend tables  
5. **Publish** → Write consolidated Delta tables & temp views  

---

## ⚡ Quickstart

1. **Import** the `.py` notebook/script into Databricks  
2. **Configure** the parameters at the top:

```python
CONFIG = {
    "target_catalogs": ["prod", "dev"],   # or None for all
    "schema_filter": None,                # regex (optional)
    "days_back": 90,                      # how far back to pull logs
    "output_catalog": "analytics",
    "output_schema": "dlt_monitoring",
    "save_results": True
}
```
3. Run once interactively to validate
4. **Schedule** as:

🕒 A Databricks Job, or

🔄 A Lakeflow Declarative Pipeline task

## 🔍 Example Queries

**Pipelines with the longest average runtime**
```sql
SELECT pipeline_name, avg_duration_minutes
FROM analytics.dlt_monitoring.dlt_performance_metrics
ORDER BY avg_duration_minutes DESC;
Recent failures


SELECT pipeline_name, run_start, error_count, duration_minutes
FROM analytics.dlt_monitoring.dlt_pipeline_runs
WHERE status = 'FAILED'
ORDER BY run_start DESC;
Common error signatures


SELECT pipeline_name, event_type, error_count, sample_message
FROM analytics.dlt_monitoring.dlt_error_analysis
ORDER BY error_count DESC
LIMIT 25;
```
--- 

## 🛠 Requirements
Databricks Workspace with Unity Catalog

Permissions to SHOW CATALOGS/SCHEMAS/TABLES + read event log tables

Write access to your target output_catalog.output_schema

Works best with Serverless Notebooks!!

## 📅 Ops Tips
Start with days_back = 30–90 for performance

Overwrites analytics tables by default → switch to merge if incremental is preferred

Schedule refreshes daily or hourly depending on SLAs

## 🤝 Contributing
Pull requests welcome! Ideas for dashboards, alerting, or extensions are encouraged.
