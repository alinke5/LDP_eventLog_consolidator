# Databricks notebook source
# MAGIC %md
# MAGIC **Subject:** Comprehensive Lakeflow Declaritive Pipelines (formerly "DLT") Monitoring Solution - Unified Analytics Across Your Databricks Environment
# MAGIC </br>
# MAGIC **Crafted By:** Alex Linke, Sr. Solutions Architect @ Databricks
# MAGIC
# MAGIC
# MAGIC ## 🎯 **The Challenge You're Facing**
# MAGIC
# MAGIC Currently, your DLT pipelines create scattered `dlt_event_log_*` tables across different catalogs and schemas throughout your Databricks environment. This makes it nearly impossible to:
# MAGIC
# MAGIC - Get a **holistic view** of pipeline performance across your entire organization
# MAGIC - Track **individual pipeline runs** with start times, durations, and success rates
# MAGIC - Identify **performance trends** and bottlenecks before they impact business operations
# MAGIC - Create **centralized monitoring** and alerting for your data engineering teams
# MAGIC - Answer simple questions like *"Which pipelines are failing most often?"* or *"Why did our ETL take 3 hours yesterday?"*
# MAGIC
# MAGIC ## ✨ **Our Solution: DLT Pipeline Analytics Consolidator**
# MAGIC
# MAGIC We've built a comprehensive **PySpark-based solution** that automatically:
# MAGIC
# MAGIC 🔍 **Discovers** all DLT event log tables across your entire Databricks environment  
# MAGIC 🔄 **Consolidates** scattered logs into a unified, queryable dataset  
# MAGIC 📈 **Extracts** real pipeline metadata (names, run IDs, clusters, durations)  
# MAGIC 📊 **Generates** production-ready analytics tables with rich insights  
# MAGIC 💾 **Saves** everything as Delta tables for ongoing monitoring and dashboards  
# MAGIC
# MAGIC ## 🚀 **Immediate Business Value**
# MAGIC
# MAGIC ### **Operational Visibility**
# MAGIC - **Real-time monitoring** of all pipeline runs across your environment
# MAGIC - **Success/failure rates** by pipeline name with drill-down capabilities
# MAGIC - **Performance trends** to identify degradation before it impacts SLAs
# MAGIC
# MAGIC ### **Cost Optimization**
# MAGIC - **Duration analysis** to identify long-running pipelines consuming excessive compute
# MAGIC - **Cluster utilization** insights to optimize resource allocation
# MAGIC - **Error pattern analysis** to reduce troubleshooting time
# MAGIC
# MAGIC ### **Data Quality Assurance**
# MAGIC - **Failed run tracking** with detailed error messages and affected datasets
# MAGIC - **Trend analysis** to predict and prevent future pipeline issues
# MAGIC - **Compliance reporting** with full audit trails of all pipeline executions
# MAGIC
# MAGIC ## 📊 **What You'll Get - Concrete Deliverables**
# MAGIC
# MAGIC ### **1. Executive Dashboard Data**
# MAGIC ```sql
# MAGIC -- Pipeline Health Overview
# MAGIC SELECT pipeline_name, total_runs, success_rate, avg_duration_minutes
# MAGIC FROM your_catalog.analytics.dlt_performance_metrics
# MAGIC WHERE success_rate < 0.95 OR avg_duration_minutes > 60
# MAGIC ```
# MAGIC
# MAGIC ### **2. Operational Monitoring Tables**
# MAGIC - **`dlt_pipeline_runs`** - Every pipeline execution with start/end times, status, duration
# MAGIC - **`dlt_performance_metrics`** - Success rates, average durations, error patterns by pipeline
# MAGIC - **`dlt_error_analysis`** - Detailed failure analysis with error messages and affected runs
# MAGIC - **`dlt_duration_trends`** - Performance trends with median, P95, and outlier detection
# MAGIC
# MAGIC ### **3. Real-time Analytics Examples**
# MAGIC - *"Show me all failed pipeline runs in the last 7 days"*
# MAGIC - *"Which pipelines have the highest average duration?"*
# MAGIC - *"What's our overall pipeline success rate by day?"*
# MAGIC - *"Which clusters are most/least reliable for specific pipelines?"*
# MAGIC
# MAGIC ## ⚡ **Implementation & Time to Value**
# MAGIC
# MAGIC ### **Quick Deployment**
# MAGIC - **Single PySpark script** - runs directly in your Databricks environment
# MAGIC - **No external dependencies** - uses your existing Databricks infrastructure
# MAGIC - **Configurable scope** - can start with specific catalogs/schemas and expand
# MAGIC - **Immediate results** - see analytics within minutes of first run
# MAGIC
# MAGIC ### **Minimal Maintenance**
# MAGIC - **Automated discovery** - finds new DLT tables as you create them
# MAGIC - **Scheduled execution** - can run daily/weekly via Databricks Jobs
# MAGIC - **Self-maintaining** - handles schema changes and new pipeline deployments
# MAGIC - **Scalable** - performs well across environments with hundreds of pipelines
# MAGIC
# MAGIC ## 💼 **Business Impact - What This Means for You**
# MAGIC
# MAGIC ### **For Data Engineering Teams**
# MAGIC - **Reduce troubleshooting time** from hours to minutes with centralized error analysis
# MAGIC - **Proactive monitoring** prevents pipeline failures from impacting downstream processes
# MAGIC - **Performance optimization** identifies bottlenecks before they affect SLAs
# MAGIC
# MAGIC ### **For Data Platform Teams**
# MAGIC - **Resource optimization** through cluster usage and duration analysis
# MAGIC - **Capacity planning** with trend analysis and growth projections
# MAGIC - **Compliance reporting** with complete audit trails
# MAGIC
# MAGIC ### **For Business Leadership**
# MAGIC - **Data reliability metrics** - know your data pipeline health at a glance
# MAGIC - **Cost visibility** - understand compute spending across all data pipelines
# MAGIC - **Risk mitigation** - early warning system for data pipeline issues
# MAGIC

# COMMAND ----------

"""
Simple DLT Event Log Consolidator for Databricks Notebooks

A focused  script to discover, consolidate, and analyze DLT event logs
across your Databricks environment. Run this directly in a notebook cell.

"""

from pyspark.sql.functions import *
from pyspark.sql.functions import percentile_approx, element_at
from pyspark.sql.types import *
import re
import time
from datetime import datetime, timedelta

# ================================================================================
# CONFIGURATION - Modify these settings as needed
# ================================================================================

CONFIG = {
    # Discovery settings
    'include_system_catalogs': False,
    'target_catalogs': ['alex_linke', 'jordan_reiser', 'abhijeet_kumar', 'dbdemos_abhijeet_kumar', 'mfg_sandbox'],  # Only scan these catalogs
    'catalog_filter': None,  # e.g., 'prod.*' to only scan prod catalogs (regex)
    'schema_filter': None,   # e.g., 'dlt.*' to only scan DLT schemas (regex)
    
    # Data processing
    'days_back': 90,  # Only process last N days (None for all data)
    'add_source_info': True,  # Add columns showing source table info
    
    # Output settings
    'save_results': True,
    'output_catalog': 'alex_linke', #This will need to be modified
    'output_schema': 'wesco', # This will need to be modified
    'show_sample_data': True,
    'max_rows_to_show': 20 #You can change this if you want, its just for the preview when the script is runnign
}

print("🚀 DLT Event Log Consolidator - Starting...")
print(f"Configuration: {CONFIG}")

# ================================================================================
# STEP 1: DISCOVER DLT EVENT LOG TABLES
# ================================================================================

def discover_dlt_tables(spark, config):
    """Discover all DLT event log tables across the environment"""
    
    print("\n🔍 STEP 1: Discovering DLT event log tables...")
    
    discovered_tables = []
    
    # Get catalogs to scan
    try:
        # Use specific target catalogs if provided, otherwise discover all
        if config.get('target_catalogs'):
            catalogs = config['target_catalogs']
            print(f"📂 Using target catalogs: {catalogs}")
        else:
            catalogs_df = spark.sql("SHOW CATALOGS")
            catalogs = [row.catalog for row in catalogs_df.collect()]
            
            # Filter system catalogs
            if not config['include_system_catalogs']:
                system_catalogs = ['system', 'information_schema', 'samples']
                catalogs = [cat for cat in catalogs if cat not in system_catalogs]
            
            # Apply catalog filter if specified
            if config['catalog_filter']:
                catalogs = [cat for cat in catalogs if re.match(config['catalog_filter'], cat)]
            
            print(f"📂 Discovered {len(catalogs)} catalogs: {catalogs}")
        
        print(f"🎯 Scanning {len(catalogs)} catalogs total")
        
        for i, catalog in enumerate(catalogs, 1):
            catalog_start_time = time.time()
            print(f"\n📂 [{i}/{len(catalogs)}] Processing catalog: {catalog}")
            try:
                # Get schemas in this catalog - handle different Databricks versions
                try:
                    schemas_df = spark.sql(f"SHOW SCHEMAS IN {catalog}")
                    
                    # Check what columns are available and use the right one
                    columns = schemas_df.columns
                    print(f"    📁 Schema columns available: {columns}")
                    
                    if 'namespace' in columns:
                        schemas = [row.namespace for row in schemas_df.collect()]
                    elif 'schemaName' in columns:
                        schemas = [row.schemaName for row in schemas_df.collect()]
                    elif 'databaseName' in columns:
                        schemas = [row.databaseName for row in schemas_df.collect()]
                    else:
                        # Use first column as fallback
                        col_name = columns[0]
                        schemas = [getattr(row, col_name) for row in schemas_df.collect()]
                        
                except Exception as schema_error:
                    print(f"    ⚠️ Error getting schemas for {catalog}: {schema_error}")
                    # Fallback: try using information_schema
                    try:
                        schemas_df = spark.sql(f"""
                            SELECT DISTINCT schema_name 
                            FROM {catalog}.information_schema.schemata
                        """)
                        schemas = [row.schema_name for row in schemas_df.collect()]
                    except:
                        schemas = []
                
                # Apply schema filter if specified
                if config['schema_filter']:
                    schemas = [schema for schema in schemas if re.match(config['schema_filter'], schema)]
                
                for schema in schemas:
                    try:
                        # Get tables in this schema - handle different column names
                        tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
                        
                        # Check what columns are available
                        columns = tables_df.columns
                        
                        if 'tableName' in columns:
                            tables = [row.tableName for row in tables_df.collect()]
                        elif 'name' in columns:
                            tables = [row.name for row in tables_df.collect()]
                        elif 'table' in columns:
                            tables = [row.table for row in tables_df.collect()]
                        else:
                            # Use first column as fallback
                            col_name = columns[0]
                            tables = [getattr(row, col_name) for row in tables_df.collect()]
                        
                        # Find event log tables
                        event_log_tables = [
                            table for table in tables 
                            if any(pattern in table.lower() for pattern in ['event_log', 'dlt_event_log'])
                        ]
                        
                        for table in event_log_tables:
                            full_name = f"{catalog}.{schema}.{table}"
                            try:
                                # Get basic info
                                count_result = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_name}").collect()
                                row_count = count_result[0].cnt if count_result else 0
                                
                                discovered_tables.append({
                                    'catalog': catalog,
                                    'schema': schema,
                                    'table': table,
                                    'full_name': full_name,
                                    'row_count': row_count
                                })
                                
                                print(f"  ✅ Found: {full_name} ({row_count:,} rows)")
                                
                            except Exception as e:
                                print(f"  ⚠️ Could not access {full_name}: {str(e)[:100]}")
                                
                    except Exception as e:
                        print(f"  ⚠️ Could not scan schema {catalog}.{schema}: {str(e)[:100]}")
                        
            except Exception as e:
                print(f"  ⚠️ Could not scan catalog {catalog}: {str(e)[:100]}")
            
            finally:
                catalog_time = time.time() - catalog_start_time
                print(f"  ⏱️ Catalog {catalog} completed in {catalog_time:.1f} seconds")
                
    except Exception as e:
        print(f"❌ Error during discovery: {e}")
        return []
    
    print(f"\n🎉 Discovery complete! Found {len(discovered_tables)} DLT event log tables")
    return discovered_tables

# Run discovery
discovered_tables = discover_dlt_tables(spark, CONFIG)

if not discovered_tables:
    print("❌ No DLT event log tables found. Check your permissions and DLT pipeline setup.")
else:
    # Show discovery summary
    discovery_df = spark.createDataFrame([
        (t['catalog'], t['schema'], t['table'], t['full_name'], t['row_count']) 
        for t in discovered_tables
    ], ['catalog', 'schema', 'table', 'full_name', 'row_count'])
    
    print("\n📊 Discovered Tables Summary:")
    discovery_df.orderBy('catalog', 'schema', 'table').show(truncate=False)
    
    print("\n📈 Summary by Catalog:")
    discovery_df.groupBy('catalog') \
                .agg(count('*').alias('table_count'), 
                     sum('row_count').alias('total_rows')) \
                .orderBy('catalog').show()

# ================================================================================
# STEP 2: CONSOLIDATE EVENT LOGS
# ================================================================================

def consolidate_event_logs(spark, discovered_tables, config):
    """Consolidate all discovered event logs into a single DataFrame"""
    
    print("\n🔄 STEP 2: Consolidating event log data...")
    
    if not discovered_tables:
        print("❌ No tables to consolidate")
        return None
    
    consolidated_dfs = []
    
    for table_info in discovered_tables:
        table_name = table_info['full_name']
        
        try:
            print(f"  📥 Processing {table_name}...")
            df = spark.table(table_name)
            
            # Add source information if requested
            if config['add_source_info']:
                df = df.withColumn("source_catalog", lit(table_info['catalog'])) \
                       .withColumn("source_schema", lit(table_info['schema'])) \
                       .withColumn("source_table", lit(table_info['table'])) \
                       .withColumn("source_full_name", lit(table_name))
            
            # Apply time filtering if specified
            if config['days_back']:
                cutoff_date = datetime.now() - timedelta(days=config['days_back'])
                df = df.filter(col("timestamp") >= lit(cutoff_date))
                print(f"    📅 Filtered to last {config['days_back']} days")
            
            # Add processing metadata
            df = df.withColumn("consolidated_at", current_timestamp())
            
            consolidated_dfs.append(df)
            print(f"    ✅ Added {df.count():,} rows")
            
        except Exception as e:
            print(f"    ⚠️ Error processing {table_name}: {str(e)[:100]}")
            continue
    
    if not consolidated_dfs:
        print("❌ No tables could be successfully processed")
        return None
    
    # Union all DataFrames
    print(f"\n🔗 Combining {len(consolidated_dfs)} datasets...")
    consolidated_df = consolidated_dfs[0]
    for df in consolidated_dfs[1:]:
        consolidated_df = consolidated_df.unionByName(df, allowMissingColumns=True)
    
    # Get row count (cache not supported on serverless)
    total_rows = consolidated_df.count()
    
    print(f"🎉 Consolidation complete! Total rows: {total_rows:,}")
    return consolidated_df

# Run consolidation
consolidated_df = consolidate_event_logs(spark, discovered_tables, CONFIG)

if consolidated_df:
    print(f"\n📋 Consolidated Data Schema:")
    consolidated_df.printSchema()
    
    if CONFIG['show_sample_data']:
        print(f"\n📝 Sample Data (showing {CONFIG['max_rows_to_show']} rows):")
        consolidated_df.select(
            "timestamp", "level", "event_type", "source_full_name", "message"
        ).orderBy(desc("timestamp")).show(CONFIG['max_rows_to_show'], truncate=False)

# ================================================================================
# STEP 3: BASIC ANALYTICS
# ================================================================================

def generate_analytics(df):
    """Generate comprehensive analytics by extracting pipeline metadata from event logs"""
    
    print("\n📈 STEP 3: Generating Advanced Pipeline Analytics...")
    
    # Extract pipeline metadata from the origin struct
    enriched_df = df.withColumn("pipeline_id", col("origin.pipeline_id")) \
                    .withColumn("pipeline_name", col("origin.pipeline_name")) \
                    .withColumn("pipeline_type", col("origin.pipeline_type")) \
                    .withColumn("update_id", col("origin.update_id")) \
                    .withColumn("flow_id", col("origin.flow_id")) \
                    .withColumn("flow_name", col("origin.flow_name")) \
                    .withColumn("cluster_id", col("origin.cluster_id")) \
                    .withColumn("batch_id", col("origin.batch_id")) \
                    .withColumn("request_id", col("origin.request_id")) \
                    .filter(col("pipeline_id").isNotNull())  # Only events with pipeline metadata
    
    print(f"📊 Extracted metadata from {enriched_df.count():,} events with pipeline information")
    
    # 1. Detailed Pipeline Runs Analysis (grouped by update_id for actual runs)
    print("\n🏃 Detailed Pipeline Runs Analysis:")
    pipeline_runs = enriched_df.groupBy(
        "pipeline_id", "pipeline_name", "update_id", "source_full_name"
    ).agg(
        min("timestamp").alias("run_start"),
        max("timestamp").alias("run_end"), 
        to_date(min("timestamp")).alias("run_date"),
        count("*").alias("total_events"),
        countDistinct("event_type").alias("unique_event_types"),
        sum(when(col("level") == "ERROR", 1).otherwise(0)).alias("error_count"),
        sum(when(col("level") == "WARN", 1).otherwise(0)).alias("warning_count"),
        sum(when(col("level") == "INFO", 1).otherwise(0)).alias("info_count"),
        collect_set(when(col("level") == "ERROR", col("message"))).alias("error_messages"),
        first("cluster_id").alias("cluster_id"),
        first("pipeline_type").alias("pipeline_type")
    ).withColumn(
        "duration_minutes",
        (unix_timestamp("run_end") - unix_timestamp("run_start")) / 60
    ).withColumn(
        "has_errors", 
        col("error_count") > 0
    ).withColumn(
        "status",
        when(col("error_count") > 0, "FAILED")
        .when(col("total_events") < 5, "INCOMPLETE") 
        .otherwise("SUCCESS")
    ).filter(col("duration_minutes") > 0.1)  # Filter out very short spurious runs
    
    print(f"📋 Found {pipeline_runs.count()} distinct pipeline runs")
    pipeline_runs.select(
        "pipeline_name", "run_date", "run_start", "duration_minutes", 
        "status", "error_count", "total_events", "cluster_id"
    ).orderBy(desc("run_start")).show(20, truncate=False)
    
    # 2. Performance Metrics by Actual Pipeline Name
    print("\n⚡ Performance Metrics by Pipeline:")
    performance_metrics = pipeline_runs.groupBy("pipeline_name", "pipeline_id") \
        .agg(
            count("*").alias("total_runs"),
            avg("duration_minutes").alias("avg_duration_minutes"),
            min("duration_minutes").alias("min_duration_minutes"),
            max("duration_minutes").alias("max_duration_minutes"),
            stddev("duration_minutes").alias("duration_stddev"),
            sum("error_count").alias("total_errors"),
            sum("warning_count").alias("total_warnings"),
            sum(when(col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_runs"),
            sum(when(col("status") == "FAILED", 1).otherwise(0)).alias("failed_runs"),
            countDistinct("cluster_id").alias("unique_clusters"),
            max("run_date").alias("last_run_date"),
            avg("total_events").alias("avg_events_per_run")
        ).withColumn(
            "success_rate",
            col("successful_runs") / col("total_runs")
        ).withColumn(
            "failure_rate", 
            col("failed_runs") / col("total_runs")
        ).withColumn(
            "avg_duration_hours",
            col("avg_duration_minutes") / 60
        )
    
    performance_metrics.select(
        "pipeline_name", "total_runs", "success_rate", "avg_duration_minutes", 
        "max_duration_minutes", "last_run_date", "unique_clusters"
    ).orderBy(desc("total_runs")).show(truncate=False)
    
    # 3. Detailed Error Analysis by Pipeline
    print("\n🔍 Error Analysis by Pipeline:")
    error_analysis = enriched_df.filter(col("level") == "ERROR") \
        .groupBy("pipeline_name", "pipeline_id", "event_type") \
        .agg(
            count("*").alias("error_count"),
            countDistinct("update_id").alias("affected_runs"),
            min("timestamp").alias("first_error"),
            max("timestamp").alias("last_error"),
            collect_list("message").alias("sample_error_messages")
        ).withColumn(
            "sample_message",
            element_at(col("sample_error_messages"), 1)
        ).orderBy(desc("error_count"))
    
    error_analysis.select(
        "pipeline_name", "event_type", "error_count", "affected_runs", 
        "first_error", "last_error", "sample_message"
    ).show(15, truncate=False)
    
    # 4. Daily Pipeline Activity (last 7 days)
    print("\n📅 Daily Pipeline Activity (Last 7 Days):")
    recent_activity = pipeline_runs.filter(col("run_date") >= date_sub(current_date(), 7)) \
        .groupBy("run_date", "pipeline_name") \
        .agg(
            count("*").alias("total_runs"),
            sum(when(col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_runs"),
            sum(when(col("status") == "FAILED", 1).otherwise(0)).alias("failed_runs"), 
            avg("duration_minutes").alias("avg_duration"),
            sum("total_events").alias("total_events")
        ).withColumn(
            "success_rate_daily",
            col("successful_runs") / col("total_runs")
        ).orderBy(desc("run_date"), "pipeline_name")
    
    recent_activity.show(truncate=False)
    
    # 5. Pipeline Run Duration Trends
    print("\n⏱️ Pipeline Duration Trends (Longest Running Pipelines):")
    duration_trends = pipeline_runs.filter(col("duration_minutes") > 1) \
        .groupBy("pipeline_name") \
        .agg(
            count("*").alias("runs"),
            avg("duration_minutes").alias("avg_minutes"),
            max("duration_minutes").alias("max_minutes"),
            min("duration_minutes").alias("min_minutes"),
            percentile_approx("duration_minutes", 0.5).alias("median_minutes"),
            percentile_approx("duration_minutes", 0.95).alias("p95_minutes")
        ).orderBy(desc("avg_minutes"))
    
    duration_trends.show(truncate=False)
    
    # 6. Cluster Usage Analysis
    print("\n🖥️ Cluster Usage by Pipeline:")
    cluster_usage = pipeline_runs.groupBy("pipeline_name", "cluster_id") \
        .agg(
            count("*").alias("runs_on_cluster"),
            avg("duration_minutes").alias("avg_duration_on_cluster"),
            sum(when(col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_runs")
        ).withColumn(
            "success_rate_cluster",
            col("successful_runs") / col("runs_on_cluster")
        ).orderBy("pipeline_name", desc("runs_on_cluster"))
    
    cluster_usage.show(truncate=False)
    
    return {
        'pipeline_runs': pipeline_runs,
        'performance_metrics': performance_metrics,
        'error_analysis': error_analysis,
        'recent_activity': recent_activity,
        'duration_trends': duration_trends,
        'cluster_usage': cluster_usage
    }

# Generate analytics if we have data
if consolidated_df:
    analytics_results = generate_analytics(consolidated_df)

# ================================================================================
# STEP 4: SAVE RESULTS (OPTIONAL)
# ================================================================================

if consolidated_df and CONFIG['save_results']:
    print(f"\n💾 STEP 4: Saving Results...")
    
    catalog = CONFIG['output_catalog']
    schema = CONFIG['output_schema']
    
    try:
        # Ensure schema exists
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
            print(f"✅ Schema {catalog}.{schema} ready")
        except Exception as schema_error:
            print(f"⚠️ Could not create schema (may already exist): {schema_error}")
        
        # Save consolidated data
        consolidated_table = f"{catalog}.{schema}.dlt_consolidated_events"
        print(f"📊 Saving consolidated data to table: {consolidated_table}")
        consolidated_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(consolidated_table)
        
        # Save analytics if available
        if 'analytics_results' in locals():
            for name, df_result in analytics_results.items():
                table_name = f"{catalog}.{schema}.dlt_{name}"
                print(f"📈 Saving {name} to table: {table_name}")
                df_result.write \
                        .format("delta") \
                        .mode("overwrite") \
                        .option("overwriteSchema", "true") \
                        .saveAsTable(table_name)
        
        print("✅ All results saved successfully!")
        print(f"\n🎯 Access your data:")
        print(f"   Consolidated Events: SELECT * FROM {consolidated_table}")
        print(f"   Pipeline Runs: SELECT * FROM {catalog}.{schema}.dlt_pipeline_runs")
        print(f"   Performance Metrics: SELECT * FROM {catalog}.{schema}.dlt_performance_metrics")
        print(f"   Error Analysis: SELECT * FROM {catalog}.{schema}.dlt_error_analysis")
        print(f"   Recent Activity: SELECT * FROM {catalog}.{schema}.dlt_recent_activity")
        print(f"   Duration Trends: SELECT * FROM {catalog}.{schema}.dlt_duration_trends")
        print(f"   Cluster Usage: SELECT * FROM {catalog}.{schema}.dlt_cluster_usage")
        
    except Exception as e:
        print(f"❌ Error saving results: {e}")

# ================================================================================
# STEP 5: CREATE TEMPORARY VIEWS FOR SQL ACCESS
# ================================================================================

if consolidated_df:
    print(f"\n🎯 STEP 5: Creating Temporary Views for SQL Access...")
    
    try:
        # Create main consolidated view
        consolidated_df.createOrReplaceTempView("dlt_consolidated_events")
        print("✅ Created view: dlt_consolidated_events")
        
        # Create analytics views if available
        if 'analytics_results' in locals():
            for name, df_result in analytics_results.items():
                view_name = f"dlt_{name}"
                df_result.createOrReplaceTempView(view_name)
                print(f"✅ Created view: {view_name}")
        
        print(f"\n📝 You can now use SQL queries like:")
        print(f"   %sql SELECT * FROM dlt_consolidated_events WHERE level = 'ERROR' ORDER BY timestamp DESC LIMIT 10")
        print(f"   %sql SELECT pipeline_name, total_runs, success_rate, avg_duration_minutes FROM dlt_performance_metrics ORDER BY avg_duration_minutes DESC")
        print(f"   %sql SELECT pipeline_name, status, duration_minutes FROM dlt_pipeline_runs ORDER BY run_start DESC LIMIT 20")
        
    except Exception as e:
        print(f"❌ Error creating views: {e}")

# ================================================================================
# SUMMARY
# ================================================================================

print(f"\n" + "="*80)
print(f"🎉 DLT EVENT LOG CONSOLIDATION COMPLETE!")
print(f"="*80)

if discovered_tables:
    print(f"📊 Discovered: {len(discovered_tables)} DLT event log tables")
    
if consolidated_df:
    print(f"🔗 Consolidated: {consolidated_df.count():,} total events")
    print(f"📅 Time Range: {CONFIG['days_back']} days back" if CONFIG['days_back'] else "📅 Time Range: All available data")
    
    if CONFIG['save_results']:
        print(f"💾 Saved to: {CONFIG['output_catalog']}.{CONFIG['output_schema']} schema")
    
    print(f"🎯 Views Created: Use SQL queries on dlt_consolidated_events and dlt_* views")
    
print(f"\n💡 Next Steps:")
print(f"   1. Query pipeline runs by name: SELECT * FROM alex_linke.wesco.dlt_pipeline_runs WHERE pipeline_name = 'YourPipelineName'")
print(f"   2. Monitor performance: SELECT pipeline_name, success_rate, avg_duration_minutes FROM alex_linke.wesco.dlt_performance_metrics")
print(f"   3. Build dashboards using the saved Delta tables with actual pipeline names and run metadata")
print(f"   4. Set up alerts on failure_rate, avg_duration spikes, or error patterns")
print(f"   5. Schedule this script to run regularly for ongoing pipeline monitoring")

print(f"\n🔚 Script completed successfully!") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pipeline_name, avg_duration_minutes, max_duration_minutes 
# MAGIC FROM alex_linke.wesco.dlt_performance_metrics 
# MAGIC ORDER BY avg_duration_minutes DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pipeline_name, run_start, duration_minutes, error_count
# MAGIC FROM alex_linke.wesco.dlt_pipeline_runs 
# MAGIC WHERE status = 'FAILED' 
# MAGIC ORDER BY run_start DESC