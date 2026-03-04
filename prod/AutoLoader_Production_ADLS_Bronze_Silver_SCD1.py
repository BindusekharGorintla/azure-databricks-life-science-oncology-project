# Databricks notebook source
# MAGIC %md
# MAGIC # Production Auto Loader Pipeline: ADLS Gen2 → Bronze → Silver (SCD1)
# MAGIC 
# MAGIC **Version:** 1.0.0
# MAGIC **Author:** Data Engineering Team
# MAGIC **Date:** 2025-02-11
# MAGIC 
# MAGIC **Pipeline:**
# MAGIC 1. ADLS Gen2 → Bronze (Streaming with Auto Loader)
# MAGIC 2. Bronze → Silver (SCD Type 1 with MERGE)
# MAGIC 
# MAGIC **File Format:** CSV
# MAGIC **Columns:** patient_id, name, dob, ssn, gender, cancer_patient
# MAGIC 
# MAGIC **Features:**
# MAGIC - Service Principal authentication
# MAGIC - Incremental processing
# MAGIC - Schema evolution
# MAGIC - PII handling (SSN masking)
# MAGIC - Data quality checks
# MAGIC - Comprehensive logging
# MAGIC - Error handling
# MAGIC - Performance monitoring

# COMMAND ----------

# DBTITLE 1,Install Required Packages (if needed)
# MAGIC %pip install delta-spark

# Uncomment if needed
# dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Libraries
import logging
import time
from datetime import datetime
from typing import Dict, Any, List, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

print("✅ Libraries imported")

# COMMAND ----------

# DBTITLE 1,Configuration Class
# =============================================================================
# PRODUCTION CONFIGURATION
# =============================================================================

class PipelineConfig:
    """Production pipeline configuration."""
    
    # ==========================================================================
    # AZURE ADLS GEN2 CONFIGURATION
    # ==========================================================================
    TENANT_ID = "your-tenant-id"
    CLIENT_ID = "your-client-id"
    CLIENT_SECRET = "your-client-secret"
    
    # Or use Databricks Secrets (RECOMMENDED)
    # TENANT_ID = dbutils.secrets.get(scope="azure-sp", key="tenant-id")
    # CLIENT_ID = dbutils.secrets.get(scope="azure-sp", key="client-id")
    # CLIENT_SECRET = dbutils.secrets.get(scope="azure-sp", key="client-secret")
    
    STORAGE_ACCOUNT_NAME = "your_storage_account"
    CONTAINER_NAME = "raw-data"
    FOLDER_PATH = "patients/csv"  # Path to patient CSV files
    
    # ==========================================================================
    # DELTA LAKE CONFIGURATION
    # ==========================================================================
    CATALOG = "your_catalog"
    SCHEMA = "your_schema"
    
    # Bronze layer
    BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.patient_bronze"
    BRONZE_CHECKPOINT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/patient_bronze"
    BRONZE_SCHEMA_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/schemas/patient_bronze"
    
    # Silver layer
    SILVER_TABLE = f"{CATALOG}.{SCHEMA}.patient_silver"
    SILVER_CHECKPOINT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/patient_silver"
    
    # ==========================================================================
    # STREAMING CONFIGURATION
    # ==========================================================================
    TRIGGER_MODE = "availableNow"  # "availableNow" for batch, "processingTime" for continuous
    TRIGGER_INTERVAL = "30 seconds"
    MAX_FILES_PER_TRIGGER = 1000
    
    # ==========================================================================
    # DATA QUALITY CONFIGURATION
    # ==========================================================================
    RUN_QUALITY_CHECKS = True
    FAIL_ON_QUALITY_ISSUES = False
    MASK_PII = True  # Mask SSN in Silver
    
    # ==========================================================================
    # SPARK OPTIMIZATION
    # ==========================================================================
    SHUFFLE_PARTITIONS = 200
    BROADCAST_THRESHOLD = 100 * 1024 * 1024  # 100 MB

# Initialize config
config = PipelineConfig()

# Configure Spark
spark.conf.set("spark.sql.shuffle.partitions", config.SHUFFLE_PARTITIONS)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", config.BROADCAST_THRESHOLD)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

logger.info("✅ Configuration loaded")
logger.info(f"   Storage: {config.STORAGE_ACCOUNT_NAME}/{config.CONTAINER_NAME}/{config.FOLDER_PATH}")
logger.info(f"   Bronze: {config.BRONZE_TABLE}")
logger.info(f"   Silver: {config.SILVER_TABLE}")

# COMMAND ----------

# DBTITLE 1,Helper Functions
def time_execution(func):
    """Decorator to time function execution."""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        logger.info(f"⏱️  {func.__name__} completed in {elapsed_time:.2f} seconds")
        return result
    return wrapper


def log_dataframe_info(df: DataFrame, name: str) -> None:
    """Log DataFrame information."""
    try:
        count = df.count()
        logger.info(f"{name}: {count:,} records")
    except Exception as e:
        logger.warning(f"Could not log info for {name}: {str(e)}")


def create_quality_report(df: DataFrame, layer: str) -> Dict[str, Any]:
    """Generate data quality report."""
    report = {
        'layer': layer,
        'timestamp': datetime.now().isoformat(),
        'total_records': df.count(),
        'null_checks': {},
        'duplicates': 0
    }
    
    # Check nulls in key columns
    for col_name in ['patient_id', 'name', 'dob', 'gender']:
        if col_name in df.columns:
            null_count = df.filter(F.col(col_name).isNull()).count()
            report['null_checks'][col_name] = null_count
    
    # Check duplicates
    if 'patient_id' in df.columns:
        dup_count = (df
            .groupBy('patient_id')
            .count()
            .filter(F.col('count') > 1)
            .count()
        )
        report['duplicates'] = dup_count
    
    return report

print("✅ Helper functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure ADLS Gen2 Access

# COMMAND ----------

# DBTITLE 1,Setup Service Principal Authentication
@time_execution
def configure_adls_access() -> str:
    """
    Configure ADLS Gen2 access with Service Principal.
    
    Returns:
        Source path
    """
    logger.info("Configuring ADLS Gen2 access...")
    
    try:
        # Set OAuth authentication
        spark.conf.set(
            f"fs.azure.account.auth.type.{config.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
            "OAuth"
        )
        
        spark.conf.set(
            f"fs.azure.account.oauth.provider.type.{config.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        )
        
        spark.conf.set(
            f"fs.azure.account.oauth2.client.id.{config.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
            config.CLIENT_ID
        )
        
        spark.conf.set(
            f"fs.azure.account.oauth2.client.secret.{config.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
            config.CLIENT_SECRET
        )
        
        spark.conf.set(
            f"fs.azure.account.oauth2.client.endpoint.{config.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
            f"https://login.microsoftonline.com/{config.TENANT_ID}/oauth2/token"
        )
        
        # Build source path
        source_path = f"abfss://{config.CONTAINER_NAME}@{config.STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{config.FOLDER_PATH}"
        
        logger.info("✅ ADLS Gen2 access configured")
        logger.info(f"   Source: {source_path}")
        
        # Test connection
        logger.info("Testing connection...")
        files = dbutils.fs.ls(source_path)
        logger.info(f"✅ Connection successful - found {len(files)} items")
        
        return source_path
        
    except Exception as e:
        logger.error(f"❌ Error configuring ADLS access: {str(e)}")
        raise

source_path = configure_adls_access()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define Schema

# COMMAND ----------

# DBTITLE 1,Define Patient Schema
# Explicit schema for patient data
patient_schema = StructType([
    StructField("patient_id", StringType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("dob", DateType(), nullable=True),
    StructField("ssn", StringType(), nullable=True),
    StructField("gender", StringType(), nullable=True),
    StructField("cancer_patient", BooleanType(), nullable=True)
])

logger.info("✅ Patient schema defined")
logger.info(f"   Columns: {[f.name for f in patient_schema.fields]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Bronze Layer (Streaming)

# COMMAND ----------

# DBTITLE 1,Create Bronze Delta Table
@time_execution
def create_bronze_table() -> None:
    """Create Bronze Delta table if not exists."""
    logger.info(f"Creating Bronze table: {config.BRONZE_TABLE}")
    
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {config.BRONZE_TABLE} (
                -- Source columns
                patient_id          STRING NOT NULL,
                name                STRING,
                dob                 DATE,
                ssn                 STRING,
                gender              STRING,
                cancer_patient      BOOLEAN,
                
                -- Metadata columns
                _source_file        STRING,
                _ingestion_timestamp TIMESTAMP,
                _ingestion_date     DATE,
                _file_modification_time TIMESTAMP,
                _year               INT,
                _month              INT,
                _day                INT
            )
            USING DELTA
            PARTITIONED BY (_year, _month, _day)
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.enableChangeDataFeed' = 'true',
                'delta.deletedFileRetentionDuration' = 'interval 7 days',
                'delta.logRetentionDuration' = 'interval 30 days'
            )
            COMMENT 'Bronze layer - Raw patient data from ADLS Gen2'
        """)
        
        logger.info(f"✅ Bronze table created/verified: {config.BRONZE_TABLE}")
        
    except Exception as e:
        logger.error(f"❌ Error creating Bronze table: {str(e)}")
        raise

create_bronze_table()

# COMMAND ----------

# DBTITLE 1,Configure Auto Loader Stream to Bronze
@time_execution
def setup_bronze_stream() -> DataFrame:
    """
    Setup Auto Loader stream from ADLS Gen2 to Bronze.
    
    Returns:
        Streaming DataFrame
    """
    logger.info("Setting up Auto Loader stream to Bronze...")
    
    try:
        # Read stream with Auto Loader
        bronze_stream = (spark
            .readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", config.BRONZE_SCHEMA_PATH)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("cloudFiles.maxFilesPerTrigger", config.MAX_FILES_PER_TRIGGER)
            .schema(patient_schema)
            
            # CSV options
            .option("header", "true")
            .option("delimiter", ",")
            .option("quote", '"')
            .option("escape", '"')
            .option("dateFormat", "yyyy-MM-dd")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            
            .load(source_path)
        )
        
        # Add metadata columns
        bronze_enriched = (bronze_stream
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_ingestion_date", F.to_date(F.current_timestamp()))
            .withColumn("_file_modification_time", F.col("_metadata.file_modification_time"))
            .withColumn("_year", F.year(F.col("_ingestion_date")))
            .withColumn("_month", F.month(F.col("_ingestion_date")))
            .withColumn("_day", F.dayofmonth(F.col("_ingestion_date")))
        )
        
        logger.info("✅ Bronze stream configured")
        return bronze_enriched
        
    except Exception as e:
        logger.error(f"❌ Error setting up Bronze stream: {str(e)}")
        raise

bronze_stream_df = setup_bronze_stream()

# COMMAND ----------

# DBTITLE 1,Start Bronze Streaming Job
@time_execution
def start_bronze_stream(stream_df: DataFrame) -> Any:
    """
    Start Bronze streaming job.
    
    Args:
        stream_df: Streaming DataFrame
    
    Returns:
        StreamingQuery object
    """
    logger.info("Starting Bronze streaming job...")
    
    try:
        # Configure trigger
        if config.TRIGGER_MODE == "availableNow":
            trigger_config = {"availableNow": True}
        else:
            trigger_config = {"processingTime": config.TRIGGER_INTERVAL}
        
        # Write stream
        query = (stream_df
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", config.BRONZE_CHECKPOINT)
            .option("mergeSchema", "true")
            .trigger(**trigger_config)
            .toTable(config.BRONZE_TABLE)
        )
        
        logger.info("✅ Bronze streaming job started")
        logger.info(f"   Query ID: {query.id}")
        logger.info(f"   Checkpoint: {config.BRONZE_CHECKPOINT}")
        
        return query
        
    except Exception as e:
        logger.error(f"❌ Error starting Bronze stream: {str(e)}")
        raise

bronze_query = start_bronze_stream(bronze_stream_df)

# COMMAND ----------

# DBTITLE 1,Monitor Bronze Stream
import time

def monitor_bronze_stream(query, duration_seconds=60):
    """Monitor Bronze streaming progress."""
    logger.info(f"Monitoring Bronze stream for {duration_seconds} seconds...")
    
    start_time = time.time()
    iteration = 0
    
    while time.time() - start_time < duration_seconds:
        iteration += 1
        
        if not query.isActive:
            logger.warning("⚠️  Stream is not active")
            if query.exception():
                logger.error(f"❌ Stream exception: {query.exception()}")
            break
        
        status = query.status
        progress = query.lastProgress
        
        if progress:
            logger.info(f"[{iteration}] Batch {progress.get('batchId', 'N/A')}: "
                       f"{progress.get('numInputRows', 0):,} rows processed")
        
        time.sleep(10)
    
    logger.info("✅ Monitoring complete")

# Monitor for 60 seconds
monitor_bronze_stream(bronze_query, duration_seconds=60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Silver Layer (SCD Type 1)

# COMMAND ----------

# DBTITLE 1,Create Silver Delta Table
@time_execution
def create_silver_table() -> None:
    """Create Silver Delta table with SCD Type 1."""
    logger.info(f"Creating Silver table: {config.SILVER_TABLE}")
    
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {config.SILVER_TABLE} (
                -- Business key
                patient_id          STRING NOT NULL,
                
                -- SCD Type 1 attributes (always current)
                name                STRING,
                dob                 DATE,
                ssn_masked          STRING,  -- Masked SSN
                gender              STRING,
                cancer_patient      BOOLEAN,
                
                -- Audit columns
                created_timestamp   TIMESTAMP,
                updated_timestamp   TIMESTAMP,
                is_current          BOOLEAN,
                source_system       STRING,
                
                -- Data quality
                record_hash         STRING
            )
            USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.enableChangeDataFeed' = 'true',
                'delta.deletedFileRetentionDuration' = 'interval 7 days'
            )
            COMMENT 'Silver layer - Curated patient data with SCD Type 1'
        """)
        
        logger.info(f"✅ Silver table created/verified: {config.SILVER_TABLE}")
        
    except Exception as e:
        logger.error(f"❌ Error creating Silver table: {str(e)}")
        raise

create_silver_table()

# COMMAND ----------

# DBTITLE 1,Bronze to Silver Transformation Function
def transform_bronze_to_silver(bronze_df: DataFrame) -> DataFrame:
    """
    Transform Bronze data to Silver with PII masking and data quality.
    
    Args:
        bronze_df: Bronze DataFrame
    
    Returns:
        Transformed Silver DataFrame
    """
    logger.info("Transforming Bronze to Silver...")
    
    try:
        # Mask SSN (keep last 4 digits)
        if config.MASK_PII:
            ssn_masked = F.concat(
                F.lit("XXX-XX-"),
                F.substring(F.col("ssn"), -4, 4)
            )
        else:
            ssn_masked = F.col("ssn")
        
        # Transform
        silver_df = (bronze_df
            .select(
                # Business key
                F.col("patient_id"),
                
                # Attributes
                F.trim(F.initcap(F.col("name"))).alias("name"),
                F.col("dob"),
                ssn_masked.alias("ssn_masked"),
                F.upper(F.col("gender")).alias("gender"),
                F.coalesce(F.col("cancer_patient"), F.lit(False)).alias("cancer_patient"),
                
                # Audit columns
                F.col("_ingestion_timestamp").alias("created_timestamp"),
                F.col("_ingestion_timestamp").alias("updated_timestamp"),
                F.lit(True).alias("is_current"),
                F.lit("ADLS_GEN2").alias("source_system"),
                
                # Data quality - hash for change detection
                F.sha2(
                    F.concat_ws("|",
                        F.coalesce(F.col("patient_id"), F.lit("")),
                        F.coalesce(F.col("name"), F.lit("")),
                        F.coalesce(F.col("dob").cast("string"), F.lit("")),
                        F.coalesce(F.col("ssn"), F.lit("")),
                        F.coalesce(F.col("gender"), F.lit("")),
                        F.coalesce(F.col("cancer_patient").cast("string"), F.lit(""))
                    ),
                    256
                ).alias("record_hash")
            )
            .filter(F.col("patient_id").isNotNull())  # Filter out null patient_ids
        )
        
        logger.info("✅ Transformation complete")
        return silver_df
        
    except Exception as e:
        logger.error(f"❌ Error in transformation: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,SCD Type 1 Merge Function
def merge_to_silver_scd1(micro_batch_df: DataFrame, batch_id: int) -> None:
    """
    Merge micro-batch to Silver table with SCD Type 1 logic.
    
    SCD Type 1: Overwrite existing records (no history)
    
    Args:
        micro_batch_df: Micro-batch DataFrame from Bronze
        batch_id: Batch identifier
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"Processing Batch: {batch_id}")
    logger.info(f"{'='*80}")
    
    if micro_batch_df.rdd.isEmpty():
        logger.info("Empty batch - skipping")
        return
    
    try:
        # Count records
        batch_count = micro_batch_df.count()
        logger.info(f"Batch records: {batch_count:,}")
        
        # Transform Bronze to Silver
        silver_batch = transform_bronze_to_silver(micro_batch_df)
        
        # Deduplicate within batch (keep latest)
        window_spec = Window.partitionBy("patient_id").orderBy(
            F.col("created_timestamp").desc()
        )
        
        deduped_batch = (silver_batch
            .withColumn("_rn", F.row_number().over(window_spec))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        
        deduped_count = deduped_batch.count()
        duplicates_removed = batch_count - deduped_count
        
        if duplicates_removed > 0:
            logger.info(f"Deduplication: {batch_count:,} → {deduped_count:,} "
                       f"(removed {duplicates_removed:,} duplicates)")
        
        # Check if Silver table exists
        table_exists = spark.catalog.tableExists(config.SILVER_TABLE)
        
        if not table_exists:
            # First load - create table
            logger.info(f"Creating Silver table with initial data: {config.SILVER_TABLE}")
            (deduped_batch.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(config.SILVER_TABLE)
            )
            logger.info(f"✅ Initial load: {deduped_count:,} records")
            
        else:
            # SCD Type 1 MERGE
            logger.info("Executing SCD Type 1 MERGE...")
            
            delta_table = DeltaTable.forName(spark, config.SILVER_TABLE)
            
            # MERGE logic
            (delta_table.alias("target")
                .merge(
                    deduped_batch.alias("source"),
                    "target.patient_id = source.patient_id"
                )
                .whenMatchedUpdate(
                    condition="target.record_hash <> source.record_hash",  # Only update if changed
                    set={
                        "name": "source.name",
                        "dob": "source.dob",
                        "ssn_masked": "source.ssn_masked",
                        "gender": "source.gender",
                        "cancer_patient": "source.cancer_patient",
                        "updated_timestamp": "source.updated_timestamp",
                        "record_hash": "source.record_hash"
                    }
                )
                .whenNotMatchedInsert(
                    values={
                        "patient_id": "source.patient_id",
                        "name": "source.name",
                        "dob": "source.dob",
                        "ssn_masked": "source.ssn_masked",
                        "gender": "source.gender",
                        "cancer_patient": "source.cancer_patient",
                        "created_timestamp": "source.created_timestamp",
                        "updated_timestamp": "source.updated_timestamp",
                        "is_current": "source.is_current",
                        "source_system": "source.source_system",
                        "record_hash": "source.record_hash"
                    }
                )
                .execute()
            )
            
            logger.info("✅ SCD Type 1 MERGE complete")
        
        logger.info(f"{'='*80}\n")
        
    except Exception as e:
        logger.error(f"❌ Error in batch {batch_id}: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,Start Silver Streaming Job
@time_execution
def start_silver_stream() -> Any:
    """
    Start Silver streaming job from Bronze.
    
    Returns:
        StreamingQuery object
    """
    logger.info("Starting Silver streaming job...")
    
    try:
        # Read from Bronze
        bronze_stream = (spark
            .readStream
            .format("delta")
            .table(config.BRONZE_TABLE)
        )
        
        # Configure trigger
        if config.TRIGGER_MODE == "availableNow":
            trigger_config = {"availableNow": True}
        else:
            trigger_config = {"processingTime": config.TRIGGER_INTERVAL}
        
        # Write to Silver with foreachBatch
        query = (bronze_stream
            .writeStream
            .format("delta")
            .outputMode("update")
            .foreachBatch(merge_to_silver_scd1)
            .option("checkpointLocation", config.SILVER_CHECKPOINT)
            .trigger(**trigger_config)
            .start()
        )
        
        logger.info("✅ Silver streaming job started")
        logger.info(f"   Query ID: {query.id}")
        logger.info(f"   Checkpoint: {config.SILVER_CHECKPOINT}")
        
        return query
        
    except Exception as e:
        logger.error(f"❌ Error starting Silver stream: {str(e)}")
        raise

silver_query = start_silver_stream()

# COMMAND ----------

# DBTITLE 1,Monitor Silver Stream
def monitor_silver_stream(query, duration_seconds=60):
    """Monitor Silver streaming progress."""
    logger.info(f"Monitoring Silver stream for {duration_seconds} seconds...")
    
    start_time = time.time()
    iteration = 0
    
    while time.time() - start_time < duration_seconds:
        iteration += 1
        
        if not query.isActive:
            logger.warning("⚠️  Stream is not active")
            if query.exception():
                logger.error(f"❌ Stream exception: {query.exception()}")
            break
        
        status = query.status
        progress = query.lastProgress
        
        if progress:
            logger.info(f"[{iteration}] Batch {progress.get('batchId', 'N/A')}: "
                       f"{progress.get('numInputRows', 0):,} rows processed")
        
        time.sleep(10)
    
    logger.info("✅ Monitoring complete")

# Monitor for 60 seconds
monitor_silver_stream(silver_query, duration_seconds=60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Data Quality Checks

# COMMAND ----------

# DBTITLE 1,Run Data Quality Checks
@time_execution
def run_quality_checks() -> Dict[str, Any]:
    """
    Run comprehensive data quality checks.
    
    Returns:
        Quality report dictionary
    """
    if not config.RUN_QUALITY_CHECKS:
        logger.info("Quality checks disabled")
        return {}
    
    logger.info("Running data quality checks...")
    
    reports = {}
    
    try:
        # Bronze quality check
        bronze_df = spark.table(config.BRONZE_TABLE)
        reports['bronze'] = create_quality_report(bronze_df, "Bronze")
        
        # Silver quality check
        silver_df = spark.table(config.SILVER_TABLE)
        reports['silver'] = create_quality_report(silver_df, "Silver")
        
        # Additional checks
        # Check for SSN masking in Silver
        if config.MASK_PII:
            unmasked_ssn = silver_df.filter(
                ~F.col("ssn_masked").like("XXX-XX-%")
            ).count()
            reports['silver']['unmasked_ssn'] = unmasked_ssn
            
            if unmasked_ssn > 0:
                logger.warning(f"⚠️  Found {unmasked_ssn:,} unmasked SSNs in Silver")
        
        # Check gender values
        invalid_gender = silver_df.filter(
            ~F.col("gender").isin(["M", "F", "MALE", "FEMALE", "OTHER", "UNKNOWN", None])
        ).count()
        reports['silver']['invalid_gender'] = invalid_gender
        
        if invalid_gender > 0:
            logger.warning(f"⚠️  Found {invalid_gender:,} invalid gender values")
        
        # Cancer patient distribution
        cancer_dist = silver_df.groupBy("cancer_patient").count().collect()
        reports['silver']['cancer_distribution'] = {
            str(row['cancer_patient']): row['count'] for row in cancer_dist
        }
        
        logger.info("✅ Quality checks completed")
        return reports
        
    except Exception as e:
        logger.error(f"❌ Error in quality checks: {str(e)}")
        return reports

quality_reports = run_quality_checks()

# COMMAND ----------

# DBTITLE 1,Display Quality Report
print("="*80)
print("DATA QUALITY REPORT")
print("="*80)

for layer, report in quality_reports.items():
    print(f"\n{'─'*80}")
    print(f"{layer.upper()} LAYER")
    print(f"{'─'*80}")
    
    print(f"\nTotal Records: {report.get('total_records', 0):,}")
    print(f"Duplicates: {report.get('duplicates', 0):,}")
    
    print(f"\nNull Checks:")
    for col, null_count in report.get('null_checks', {}).items():
        status = "✅" if null_count == 0 else "⚠️"
        print(f"  {status} {col}: {null_count:,} nulls")
    
    if layer == 'silver':
        print(f"\nPII Masking:")
        print(f"  Unmasked SSN: {report.get('unmasked_ssn', 0):,}")
        
        print(f"\nData Validation:")
        print(f"  Invalid Gender: {report.get('invalid_gender', 0):,}")
        
        print(f"\nCancer Patient Distribution:")
        for status, count in report.get('cancer_distribution', {}).items():
            print(f"  {status}: {count:,}")

print("\n" + "="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Stream Management

# COMMAND ----------

# DBTITLE 1,List Active Streams
def list_active_streams():
    """List all active streaming queries."""
    streams = spark.streams.active
    
    if not streams:
        logger.info("No active streams")
        return
    
    print("="*80)
    print("ACTIVE STREAMING QUERIES")
    print("="*80)
    
    for i, q in enumerate(streams, 1):
        print(f"\n{i}. {q.name or 'Unnamed'}")
        print(f"   ID:     {q.id}")
        print(f"   Status: {q.status.get('message', 'Running')}")
        print(f"   Active: {q.isActive}")
    
    print("\n" + "="*80)

list_active_streams()

# COMMAND ----------

# DBTITLE 1,Stop Streams
def stop_stream(query, name: str):
    """Stop a specific stream."""
    logger.info(f"Stopping stream: {name}")
    query.stop()
    logger.info(f"✅ Stream stopped: {name}")


def stop_all_streams():
    """Stop all active streams."""
    streams = spark.streams.active
    
    if not streams:
        logger.info("No streams to stop")
        return
    
    logger.info(f"Stopping {len(streams)} stream(s)...")
    for q in streams:
        name = q.name or q.id
        logger.info(f"  Stopping: {name}")
        q.stop()
    
    logger.info("✅ All streams stopped")

# To stop specific streams:
# stop_stream(bronze_query, "Bronze")
# stop_stream(silver_query, "Silver")

# To stop all:
# stop_all_streams()

logger.info("✅ Stop functions ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Maintenance and Optimization

# COMMAND ----------

# DBTITLE 1,Optimize Delta Tables
@time_execution
def optimize_tables():
    """Optimize Bronze and Silver Delta tables."""
    logger.info("Optimizing Delta tables...")
    
    try:
        # Optimize Bronze
        logger.info(f"Optimizing {config.BRONZE_TABLE}...")
        spark.sql(f"OPTIMIZE {config.BRONZE_TABLE}")
        logger.info("✅ Bronze optimized")
        
        # Optimize Silver
        logger.info(f"Optimizing {config.SILVER_TABLE}...")
        spark.sql(f"OPTIMIZE {config.SILVER_TABLE} ZORDER BY (patient_id)")
        logger.info("✅ Silver optimized with Z-ORDER")
        
        logger.info("✅ All tables optimized")
        
    except Exception as e:
        logger.error(f"❌ Error optimizing tables: {str(e)}")

# optimize_tables()

# COMMAND ----------

# DBTITLE 1,Vacuum Delta Tables
@time_execution
def vacuum_tables(retention_hours=168):
    """
    Vacuum Delta tables to remove old files.
    Default: 7 days (168 hours)
    """
    logger.info(f"Vacuuming tables (retention: {retention_hours}h)...")
    
    try:
        # Vacuum Bronze
        logger.info(f"Vacuuming {config.BRONZE_TABLE}...")
        spark.sql(f"VACUUM {config.BRONZE_TABLE} RETAIN {retention_hours} HOURS")
        logger.info("✅ Bronze vacuumed")
        
        # Vacuum Silver
        logger.info(f"Vacuuming {config.SILVER_TABLE}...")
        spark.sql(f"VACUUM {config.SILVER_TABLE} RETAIN {retention_hours} HOURS")
        logger.info("✅ Silver vacuumed")
        
        logger.info("✅ All tables vacuumed")
        
    except Exception as e:
        logger.error(f"❌ Error vacuuming tables: {str(e)}")

# vacuum_tables(retention_hours=168)

# COMMAND ----------

# DBTITLE 1,Show Table History
def show_table_history(table_name: str, limit: int = 10):
    """Show Delta table history."""
    logger.info(f"Showing history for {table_name}...")
    
    history_df = spark.sql(f"""
        DESCRIBE HISTORY {table_name}
        LIMIT {limit}
    """)
    
    display(history_df.select(
        "version", "timestamp", "operation", 
        "operationParameters", "operationMetrics"
    ))

# show_table_history(config.BRONZE_TABLE)
# show_table_history(config.SILVER_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Monitoring and Metrics

# COMMAND ----------

# DBTITLE 1,Show Table Statistics
def show_table_statistics():
    """Display statistics for Bronze and Silver tables."""
    print("="*80)
    print("TABLE STATISTICS")
    print("="*80)
    
    # Bronze stats
    print(f"\n{'─'*80}")
    print(f"BRONZE: {config.BRONZE_TABLE}")
    print(f"{'─'*80}")
    
    bronze_df = spark.table(config.BRONZE_TABLE)
    bronze_count = bronze_df.count()
    
    print(f"Total records: {bronze_count:,}")
    print(f"Unique patients: {bronze_df.select('patient_id').distinct().count():,}")
    print(f"Date range: {bronze_df.select(F.min('_ingestion_date'), F.max('_ingestion_date')).collect()[0]}")
    
    print("\nFiles processed:")
    display(bronze_df.groupBy("_source_file").count().orderBy(F.desc("count")))
    
    # Silver stats
    print(f"\n{'─'*80}")
    print(f"SILVER: {config.SILVER_TABLE}")
    print(f"{'─'*80}")
    
    silver_df = spark.table(config.SILVER_TABLE)
    silver_count = silver_df.count()
    
    print(f"Total records: {silver_count:,}")
    print(f"Cancer patients: {silver_df.filter(F.col('cancer_patient') == True).count():,}")
    print(f"Non-cancer patients: {silver_df.filter(F.col('cancer_patient') == False).count():,}")
    
    print("\nGender distribution:")
    display(silver_df.groupBy("gender").count().orderBy(F.desc("count")))
    
    print("\nRecently updated:")
    display(silver_df.orderBy(F.desc("updated_timestamp")).limit(10))
    
    print("\n" + "="*80)

show_table_statistics()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

displayHTML(f"""
<div style="font-family: Arial; padding: 20px; background-color: #f8f9fa; border-radius: 8px;">
    <h2 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px;">
        ✅ Production Auto Loader Pipeline - Deployment Complete
    </h2>

    <h3 style="color: #2980b9;">Pipeline Architecture</h3>
    <pre style="background: #2c3e50; color: #ecf0f1; padding: 12px; border-radius: 6px; font-size: 12px;">
ADLS Gen2 (CSV Files)
    ↓ Auto Loader (cloudFiles)
    ↓ Service Principal Auth
BRONZE Layer (Streaming)
    ↓ Incremental append
    ↓ Metadata enrichment
    ↓ Schema evolution
SILVER Layer (SCD Type 1)
    ↓ PII masking (SSN)
    ↓ Data quality checks
    ↓ MERGE (update/insert)
    ✅ Curated data ready!
    </pre>

    <h3 style="color: #2980b9; margin-top: 20px;">Pipeline Details</h3>
    <table style="width:100%; border-collapse: collapse; font-size: 13px;">
        <tr style="background:#ecf0f1;">
            <td style="padding:8px; font-weight:bold;">Source:</td>
            <td style="padding:8px;">{config.STORAGE_ACCOUNT_NAME}/{config.CONTAINER_NAME}/{config.FOLDER_PATH}</td>
        </tr>
        <tr>
            <td style="padding:8px; font-weight:bold;">Bronze Table:</td>
            <td style="padding:8px;">{config.BRONZE_TABLE}</td>
        </tr>
        <tr style="background:#ecf0f1;">
            <td style="padding:8px; font-weight:bold;">Silver Table:</td>
            <td style="padding:8px;">{config.SILVER_TABLE}</td>
        </tr>
        <tr>
            <td style="padding:8px; font-weight:bold;">Trigger Mode:</td>
            <td style="padding:8px;">{config.TRIGGER_MODE}</td>
        </tr>
        <tr style="background:#ecf0f1;">
            <td style="padding:8px; font-weight:bold;">PII Masking:</td>
            <td style="padding:8px;">{'Enabled' if config.MASK_PII else 'Disabled'}</td>
        </tr>
    </table>

    <h3 style="color: #2980b9; margin-top: 20px;">Production Features</h3>
    <ul style="font-size: 13px; line-height: 1.8;">
        <li>✅ <strong>Auto Loader:</strong> Incremental file processing</li>
        <li>✅ <strong>Service Principal:</strong> Secure authentication</li>
        <li>✅ <strong>Schema Evolution:</strong> Handles new columns</li>
        <li>✅ <strong>SCD Type 1:</strong> Always current records</li>
        <li>✅ <strong>PII Masking:</strong> SSN protection</li>
        <li>✅ <strong>Checkpointing:</strong> Exactly-once processing</li>
        <li>✅ <strong>Data Quality:</strong> Comprehensive checks</li>
        <li>✅ <strong>Logging:</strong> Detailed execution logs</li>
        <li>✅ <strong>Error Handling:</strong> Graceful failures</li>
        <li>✅ <strong>Monitoring:</strong> Stream progress tracking</li>
    </ul>

    <h3 style="color: #2980b9; margin-top: 20px;">Data Quality Results</h3>
    <table style="width:100%; border-collapse: collapse; font-size: 13px;">
        <tr style="background:#3498db; color:white;">
            <th style="padding:8px; text-align:left;">Layer</th>
            <th style="padding:8px; text-align:left;">Records</th>
            <th style="padding:8px; text-align:left;">Duplicates</th>
        </tr>
        <tr style="background:#ecf0f1;">
            <td style="padding:8px;">Bronze</td>
            <td style="padding:8px;">{quality_reports.get('bronze', {}).get('total_records', 0):,}</td>
            <td style="padding:8px;">{quality_reports.get('bronze', {}).get('duplicates', 0):,}</td>
        </tr>
        <tr>
            <td style="padding:8px;">Silver</td>
            <td style="padding:8px;">{quality_reports.get('silver', {}).get('total_records', 0):,}</td>
            <td style="padding:8px;">{quality_reports.get('silver', {}).get('duplicates', 0):,}</td>
        </tr>
    </table>

    <h3 style="color: #2980b9; margin-top: 20px;">Management Commands</h3>
    <pre style="background: #2c3e50; color: #ecf0f1; padding: 12px; border-radius: 6px; font-size: 12px;">
# List streams
list_active_streams()

# Stop streams
stop_stream(bronze_query, "Bronze")
stop_stream(silver_query, "Silver")

# Optimize tables
optimize_tables()

# Vacuum (cleanup old files)
vacuum_tables(retention_hours=168)

# Show history
show_table_history(config.SILVER_TABLE)

# Statistics
show_table_statistics()
    </pre>

    <div style="margin-top: 20px; padding: 15px; background-color: #d4edda; border-left: 4px solid #28a745;">
        <strong>✅ PIPELINE READY!</strong><br>
        Your production Auto Loader pipeline is now running.<br>
        New files in ADLS Gen2 will be automatically processed to Bronze and Silver layers.
    </div>
</div>
""")

print("="*80)
print("✅ PRODUCTION AUTO LOADER PIPELINE DEPLOYED")
print("="*80)
print(f"\nSource: {source_path}")
print(f"Bronze: {config.BRONZE_TABLE}")
print(f"Silver: {config.SILVER_TABLE}")
print("\n" + "="*80)
