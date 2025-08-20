Lakeflow Declarative Pipeline – DLT Event Log Consolidator & Analytics

Author: Alex Linke (Sr. Solutions Architect @ Databricks)
Purpose: Unify Delta Live Tables (DLT) event logs created per-pipeline into a single analytics layer so you can monitor performance, failures, and trends across all pipelines.

Why this exists

DLT writes event logs one table per pipeline, which is great for local debugging but painful for org-wide analytics. This project:

Discovers DLT event log tables across selected catalogs/schemas

Consolidates them into one Delta table

Extracts real pipeline metadata (name, run/update IDs, cluster, flow, etc.)

Publishes a suite of analytics tables (runs, success/failure rates, duration trends, error analysis, cluster usage)

What you get (Delta tables)

After a run, you’ll have these tables in your target catalog.schema:

dlt_consolidated_events — Raw, unioned DLT events + source table info

dlt_pipeline_runs — One row per actual run with start/end/duration/status

dlt_performance_metrics — Success rate, avg/min/max/P95 duration, error totals

dlt_error_analysis — Error counts, affected runs, first/last error, sample message

dlt_duration_trends — Daily medians/P95 and outlier detection by pipeline

dlt_recent_activity — Last N days’ runs for heads-up monitoring

dlt_cluster_usage — Distinct clusters used per pipeline

The script also creates temporary views in the notebook session with the same names (e.g., dlt_pipeline_runs) for ad-hoc SQL.

Architecture (high level)

Discovery: SHOW CATALOGS/SCHEMAS/TABLES (optionally filtered) → find DLT event log tables.

Consolidation: Read all discovered event logs → uniform schema → union.

Enrichment: Pull pipeline metadata from the origin struct (pipeline_id, pipeline_name, update_id, flow info, cluster_id, etc.).

Analytics: Build runs, performance, error, trend, cluster views/tables.

Publish: Write Delta tables to your configured catalog.schema (overwrite) and create temp views.

Prerequisites

Unity Catalog enabled workspaces (for multi-catalog discovery)

Permissions to SHOW CATALOGS/SCHEMAS/TABLES and READ event-log tables

Permissions to CREATE SCHEMA and WRITE to the target catalog.schema

Databricks cluster / SQL Warehouse with a recent DBR (e.g., 14.x/15.x)

PySpark (pyspark.sql used heavily)

Quickstart

Import the notebook/script into Databricks (this repo’s .py file).

Edit configuration near the top of the script:

CONFIG = {
    # Discovery
    'include_system_catalogs': False,
    'target_catalogs': ['<your_catalogs>'],   # e.g., ['prod', 'dev'] or None to scan all
    'catalog_filter': None,                   # regex, e.g., r'^prod_.*$'
    'schema_filter': None,                    # regex, e.g., r'^dlt_.*$'

    # Data processing
    'days_back': 90,                          # limit processing window (None = all)
    'add_source_info': True,                  # include source catalog/schema/table cols

    # Output
    'save_results': True,
    'output_catalog': '<your_output_catalog>',
    'output_schema':  '<your_output_schema>',
    'show_sample_data': True,
    'max_rows_to_show': 20
}


Important: Change output_catalog and output_schema to where you want the tables written.

Run the notebook once interactively to validate discovery + writes.

Schedule it:

As a Databricks Job, or

As a task inside a Lakeflow declarative pipeline (recommended) on your cadence.

Example queries

Top pipelines by average duration

SELECT pipeline_name, avg_duration_minutes, max_duration_minutes
FROM <catalog>.<schema>.dlt_performance_metrics
ORDER BY avg_duration_minutes DESC;


Recent failures with durations

SELECT pipeline_name, run_start, duration_minutes, error_count
FROM <catalog>.<schema>.dlt_pipeline_runs
WHERE status = 'FAILED'
ORDER BY run_start DESC;


Success/failure rates

SELECT pipeline_name, total_runs, success_rate, failure_rate
FROM <catalog>.<schema>.dlt_performance_metrics
ORDER BY total_runs DESC;


Daily P95 trend

SELECT pipeline_name, run_date, p95_duration_minutes
FROM <catalog>.<schema>.dlt_duration_trends
ORDER BY run_date DESC;


Common error signatures

SELECT pipeline_name, event_type, error_count, sample_message
FROM <catalog>.<schema>.dlt_error_analysis
ORDER BY error_count DESC
LIMIT 50;

Scheduling & ops tips

Start with days_back = 30–90 to keep reads efficient.

Use a Serverless SQL Warehouse or Jobs cluster sized for your environment’s number of pipelines.

The script overwrites analytics tables for simplicity; change to merge if you want incremental.

Troubleshooting

No tables discovered: Check target_catalogs/filters and your permissions to SHOW/READ.

Writes fail: Ensure the output_catalog.schema exists (the script attempts to create the schema) and your principal can write there.

Performance: Narrow filters and/or reduce days_back if you have thousands of pipelines.

License

MIT (or your preferred license)

LinkedIn Blog (copy/paste)

Title: One Place to See Them All: Consolidating DLT Event Logs for Org-Wide Insights

If you’ve ever tried to answer “Which data pipelines are failing the most?” or “Why did our ETL take 3 hours yesterday?” across multiple DLT pipelines, you’ve probably run into the same problem I did: event logs are per-pipeline. Great for local debugging—tricky for cross-pipeline analytics.

So I built a small solution on Databricks that:

Discovers DLT event log tables across your catalogs/schemas

Consolidates them into one Delta table

Extracts real pipeline metadata (pipeline name, run/update IDs, cluster info, etc.)

Publishes analytics tables for success/failure rates, duration trends (median/P95), error analysis, and cluster usage

The result is a lightweight monitoring layer that turns scattered logs into clean, queryable tables you can put behind dashboards, alerts, and SLOs.

What’s included

dlt_consolidated_events (union of all logs + source info)

dlt_pipeline_runs (one row per actual run with start/end/duration/status)

dlt_performance_metrics (success rate, avg/min/max/P95 durations)

dlt_error_analysis (common error signatures, affected runs)

dlt_duration_trends, dlt_recent_activity, dlt_cluster_usage

Example questions it answers quickly

Which pipelines have the lowest success rate this month?

What’s the P95 duration trend for our critical jobs?

Which failures share the same error signature?

Are we using multiple clusters for the same pipeline across runs?

How it runs
It’s a PySpark notebook you can schedule as a Databricks Job or as a task inside a Lakeflow declarative pipeline. You point it at the catalogs/schemas you care about, choose an output location, and it does the rest. Most teams start with a 30–90 day window and refresh daily.
