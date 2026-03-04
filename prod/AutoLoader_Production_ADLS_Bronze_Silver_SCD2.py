# ================================================================
# pipeline.py — Production Auto Loader Pipeline
# ADLS Gen2 → Bronze → Silver (SCD2)
# Columns: patient_id, name, dob, ssn, gender, cancer_patient
# ================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    BooleanType, TimestampType, LongType
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("pipeline")

# ================================================================
# CONFIGURATION
# ================================================================

STORAGE_ACCOUNT   = "yourstorageaccount"
CONTAINER         = "raw"
ADLS_BASE_PATH    = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

SOURCE_PATH       = f"{ADLS_BASE_PATH}/patients/"
SCHEMA_HINT_PATH  = f"{ADLS_BASE_PATH}/schema_hints/patients/"

BRONZE_TABLE      = "catalog.bronze.patients"
BRONZE_PATH       = f"{ADLS_BASE_PATH}/bronze/patients/"
BRONZE_CHECKPOINT = f"{ADLS_BASE_PATH}/checkpoints/bronze/patients/"

SILVER_TABLE      = "catalog.silver.patients_scd2"
SILVER_PATH       = f"{ADLS_BASE_PATH}/silver/patients_scd2/"
SILVER_CHECKPOINT = f"{ADLS_BASE_PATH}/checkpoints/silver/patients_scd2/"

BUSINESS_KEY      = "patient_id"
TRACKED_COLUMNS   = ["name", "dob", "ssn", "gender", "cancer_patient"]

CSV_OPTIONS = {
    "header":                    "true",
    "inferSchema":               "false",
    "delimiter":                 ",",
    "quote":                     '"',
    "escape":                    '"',
    "multiLine":                 "true",
    "ignoreLeadingWhiteSpace":   "true",
    "ignoreTrailingWhiteSpace":  "true",
}

# ================================================================
# SPARK SESSION
# ================================================================

spark = SparkSession.builder \
    .appName("ADLS_Bronze_Silver_SCD2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ================================================================
# BRONZE SCHEMA
# ================================================================

BRONZE_SCHEMA = StructType([
    StructField("patient_id",     StringType(), nullable=False),
    StructField("name",           StringType(), nullable=True),
    StructField("dob",            StringType(), nullable=True),
    StructField("ssn",            StringType(), nullable=True),
    StructField("gender",         StringType(), nullable=True),
    StructField("cancer_patient", StringType(), nullable=True),
])

# ================================================================
# SETUP: Create Bronze & Silver tables if not exist
# ================================================================

def setup_tables():
    log.info("Setting up Bronze and Silver tables...")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            patient_id              STRING       NOT NULL,
            name                    STRING,
            dob                     STRING,
            ssn                     STRING,
            gender                  STRING,
            cancer_patient          STRING,
            _source_file            STRING,
            _source_file_mtime      TIMESTAMP,
            _load_datetime          TIMESTAMP,
            _batch_id               STRING
        )
        USING DELTA
        LOCATION '{BRONZE_PATH}'
        TBLPROPERTIES (
            'delta.enableChangeDataFeed'       = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
    """)
    log.info(f"Bronze table ready: {BRONZE_TABLE}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            patient_id              STRING       NOT NULL,
            name                    STRING,
            dob                     DATE,
            ssn                     STRING,
            gender                  STRING,
            cancer_patient          BOOLEAN,
            scd_start_date          TIMESTAMP    NOT NULL,
            scd_end_date            TIMESTAMP,
            is_current              BOOLEAN      NOT NULL,
            scd_version             BIGINT       NOT NULL,
            _bronze_load_datetime   TIMESTAMP,
            _silver_load_datetime   TIMESTAMP
        )
        USING DELTA
        LOCATION '{SILVER_PATH}'
        TBLPROPERTIES (
            'delta.enableChangeDataFeed'       = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
    """)
    log.info(f"Silver table ready: {SILVER_TABLE}")

# ================================================================
# BRONZE INGESTION — Auto Loader
# ================================================================

def ingest_bronze():
    log.info("Starting Bronze ingestion via Auto Loader...")

    stream = (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format",               "csv")
             .option("cloudFiles.schemaLocation",       SCHEMA_HINT_PATH)
             .option("cloudFiles.inferColumnTypes",     "false")
             .option("cloudFiles.schemaEvolutionMode",  "rescue")
             .option("cloudFiles.includeExistingFiles", "true")
             .options(**CSV_OPTIONS)
             .schema(BRONZE_SCHEMA)
             .load(SOURCE_PATH)
             .filter(F.col("patient_id").isNotNull())            # quarantine: drop rows missing PK
             .withColumn("_source_file",       F.input_file_name())
             .withColumn("_source_file_mtime", F.col("_metadata.file_modification_time"))
             .withColumn("_load_datetime",     F.current_timestamp())
             .withColumn("_batch_id",          F.expr("uuid()"))
    )

    query = (
        stream.writeStream
              .format("delta")
              .outputMode("append")
              .option("checkpointLocation", BRONZE_CHECKPOINT)
              .option("mergeSchema",        "true")
              .trigger(availableNow=True)
              .toTable(BRONZE_TABLE)
    )

    query.awaitTermination()
    log.info(f"Bronze ingestion complete → {BRONZE_TABLE}")

# ================================================================
# SILVER SCD2 — foreachBatch merge
# ================================================================

def cast_columns(df):
    """Cast raw Bronze strings to typed Silver columns."""
    return (
        df
        .withColumn("dob",            F.to_date(F.col("dob"), "yyyy-MM-dd"))
        .withColumn("cancer_patient", F.col("cancer_patient").cast(BooleanType()))
        .withColumn("name",           F.trim(F.col("name")))
        .withColumn("gender",         F.upper(F.trim(F.col("gender"))))
        .withColumn("ssn",            F.trim(F.col("ssn")))
    )


def apply_scd2(micro_batch_df, batch_id):
    """
    SCD Type 2 logic per micro-batch:
      - New patient       → INSERT as current row (version=1)
      - Changed patient   → EXPIRE old row, INSERT new row (version+1)
      - Deleted patient   → EXPIRE current row (soft delete)
    """
    if micro_batch_df.isEmpty():
        log.info(f"[Batch {batch_id}] Empty batch, skipping.")
        return

    silver = DeltaTable.forName(spark, SILVER_TABLE)
    now    = F.current_timestamp()

    # Deduplicate within batch: keep latest record per patient_id
    deduped = (
        micro_batch_df
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy(BUSINESS_KEY)
                      .orderBy(F.col("_commit_timestamp").desc())
            )
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    incoming = cast_columns(deduped)

    # ── Step 1: Expire rows that have changed or been deleted ──
    change_expr = " OR ".join([
        f"(target.{c} <> source.{c} OR "
        f"(target.{c} IS NULL AND source.{c} IS NOT NULL) OR "
        f"(target.{c} IS NOT NULL AND source.{c} IS NULL))"
        for c in TRACKED_COLUMNS
    ])

    (
        silver.alias("target")
        .merge(
            incoming.alias("source"),
            f"target.{BUSINESS_KEY} = source.{BUSINESS_KEY} AND target.is_current = true"
        )
        .whenMatchedUpdate(
            condition=change_expr,
            set={
                "is_current":   "false",
                "scd_end_date": "current_timestamp()"
            }
        )
        .whenNotMatchedBySourceUpdate(          # patient vanished from source → soft delete
            condition="target.is_current = true",
            set={
                "is_current":   "false",
                "scd_end_date": "current_timestamp()"
            }
        )
        .execute()
    )

    # ── Step 2: Insert new current rows ───────────────────────
    # Left anti-join against current Silver rows to find new/changed patients
    current_silver = (
        spark.table(SILVER_TABLE)
             .filter(F.col("is_current") == True)
             .select(BUSINESS_KEY, "scd_version")
    )

    new_rows = (
        incoming
        .join(current_silver, on=BUSINESS_KEY, how="left")
        .withColumn("scd_version",             F.coalesce(F.col("scd_version") + 1, F.lit(1).cast(LongType())))
        .withColumn("scd_start_date",          now)
        .withColumn("scd_end_date",            F.lit(None).cast(TimestampType()))
        .withColumn("is_current",              F.lit(True))
        .withColumn("_silver_load_datetime",   now)
        .withColumn("_bronze_load_datetime",   F.col("_load_datetime"))
        .select(
            "patient_id", "name", "dob", "ssn", "gender", "cancer_patient",
            "scd_start_date", "scd_end_date", "is_current", "scd_version",
            "_bronze_load_datetime", "_silver_load_datetime"
        )
    )

    new_rows.write.format("delta").mode("append").saveAsTable(SILVER_TABLE)
    log.info(f"[Batch {batch_id}] SCD2 merge complete → {SILVER_TABLE}")


def ingest_silver():
    log.info("Starting Silver SCD2 ingestion from Bronze CDF...")

    cdf_stream = (
        spark.readStream
             .format("delta")
             .option("readChangeFeed",  "true")
             .option("startingVersion", "latest")
             .table(BRONZE_TABLE)
             .filter(F.col("_change_type").isin("insert", "update_postimage"))
    )

    query = (
        cdf_stream
        .writeStream
        .format("delta")
        .foreachBatch(apply_scd2)
        .option("checkpointLocation", SILVER_CHECKPOINT)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    log.info(f"Silver SCD2 pipeline complete → {SILVER_TABLE}")

