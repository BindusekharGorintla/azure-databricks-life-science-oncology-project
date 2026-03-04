# ================================================================
# pipeline_scd1.py — Production Auto Loader Pipeline
# ADLS Gen2 → Bronze → Silver (SCD1)
# Columns: patient_id, name, dob, ssn, gender, cancer_patient
# ================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    BooleanType
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("pipeline_scd1")

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

SILVER_TABLE      = "catalog.silver.patients_scd1"
SILVER_PATH       = f"{ADLS_BASE_PATH}/silver/patients_scd1/"
SILVER_CHECKPOINT = f"{ADLS_BASE_PATH}/checkpoints/silver/patients_scd1/"

BUSINESS_KEY      = "patient_id"
TRACKED_COLUMNS   = ["name", "dob", "ssn", "gender", "cancer_patient"]

CSV_OPTIONS = {
    "header":                   "true",
    "inferSchema":              "false",
    "delimiter":                ",",
    "quote":                    '"',
    "escape":                   '"',
    "multiLine":                "true",
    "ignoreLeadingWhiteSpace":  "true",
    "ignoreTrailingWhiteSpace": "true",
}

# ================================================================
# SPARK SESSION
# ================================================================

spark = SparkSession.builder \
    .appName("ADLS_Bronze_Silver_SCD1") \
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
    log.info("Setting up Bronze and Silver (SCD1) tables...")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            patient_id              STRING      NOT NULL,
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
            patient_sk              STRING      NOT NULL,   -- surrogate key
            patient_id              STRING      NOT NULL,   -- business key
            name                    STRING,
            dob                     DATE,
            ssn                     STRING,
            gender                  STRING,
            cancer_patient          BOOLEAN,
            _created_datetime       TIMESTAMP,             -- first time record was inserted
            _updated_datetime       TIMESTAMP,             -- last time record was updated
            _bronze_load_datetime   TIMESTAMP,
            _silver_load_datetime   TIMESTAMP,
            _is_deleted             BOOLEAN                -- soft delete flag
        )
        USING DELTA
        LOCATION '{SILVER_PATH}'
        TBLPROPERTIES (
            'delta.enableChangeDataFeed'       = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
    """)
    log.info(f"Silver SCD1 table ready: {SILVER_TABLE}")

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
             .filter(F.col("patient_id").isNotNull())
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
# SILVER SCD1 — foreachBatch MERGE (upsert, no history)
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


def apply_scd1(micro_batch_df, batch_id):
    """
    SCD Type 1 logic per micro-batch:
      - New patient     → INSERT with _created_datetime + _updated_datetime
      - Changed patient → UPDATE in place (overwrite), only _updated_datetime changes
      - Deleted patient → Soft delete via _is_deleted = true
    No history is maintained — latest values always win.
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

    incoming = (
        cast_columns(deduped)
        # Surrogate key: deterministic hash of business key only (single row per patient in SCD1)
        .withColumn(
            "patient_sk",
            F.sha2(F.col("patient_id"), 256)
        )
        .withColumn("_silver_load_datetime", now)
        .withColumn("_bronze_load_datetime", F.col("_load_datetime"))
    )

    # ── MERGE: Upsert + Soft Delete ───────────────────────────
    (
        silver.alias("target")
        .merge(
            incoming.alias("source"),
            f"target.{BUSINESS_KEY} = source.{BUSINESS_KEY}"
        )

        # MATCHED + data changed → overwrite all tracked columns, bump _updated_datetime
        .whenMatchedUpdate(
            condition=" OR ".join([
                f"target.{c} <> source.{c} OR "
                f"(target.{c} IS NULL AND source.{c} IS NOT NULL) OR "
                f"(target.{c} IS NOT NULL AND source.{c} IS NULL)"
                for c in TRACKED_COLUMNS
            ]),
            set={
                "patient_sk":             "source.patient_sk",
                "name":                   "source.name",
                "dob":                    "source.dob",
                "ssn":                    "source.ssn",
                "gender":                 "source.gender",
                "cancer_patient":         "source.cancer_patient",
                "_updated_datetime":      "current_timestamp()",
                "_bronze_load_datetime":  "source._bronze_load_datetime",
                "_silver_load_datetime":  "source._silver_load_datetime",
                "_is_deleted":            "false",          # reactivate if previously deleted
            }
        )

        # NOT MATCHED (new patient) → INSERT with created + updated timestamps
        .whenNotMatched(
            condition="source.patient_id IS NOT NULL"
        ).insert({
            "patient_sk":             "source.patient_sk",
            "patient_id":             "source.patient_id",
            "name":                   "source.name",
            "dob":                    "source.dob",
            "ssn":                    "source.ssn",
            "gender":                 "source.gender",
            "cancer_patient":         "source.cancer_patient",
            "_created_datetime":      "current_timestamp()",
            "_updated_datetime":      "current_timestamp()",
            "_bronze_load_datetime":  "source._bronze_load_datetime",
            "_silver_load_datetime":  "source._silver_load_datetime",
            "_is_deleted":            "false",
        })

        # NOT MATCHED BY SOURCE (patient disappeared) → soft delete
        .whenNotMatchedBySourceUpdate(
            condition="target._is_deleted = false",
            set={
                "_is_deleted":       "true",
                "_updated_datetime": "current_timestamp()",
            }
        )
        .execute()
    )

    log.info(f"[Batch {batch_id}] SCD1 merge complete → {SILVER_TABLE}")


def ingest_silver():
    log.info("Starting Silver SCD1 ingestion from Bronze CDF...")

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
        .foreachBatch(apply_scd1)
        .option("checkpointLocation", SILVER_CHECKPOINT)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    log.info(f"Silver SCD1 pipeline complete → {SILVER_TABLE}")

# ================================================================
# MAIN ORCHESTRATOR
# ================================================================

if __name__ == "__main__":
    log.info("=" * 60)
    log.info("  ADLS Gen2 → Bronze → Silver (SCD1) Pipeline")
    log.info("=" * 60)

    setup_tables()      # Step 0: DDL

    ingest_bronze()     # Step 1: Auto Loader CSV → Bronze Delta

    ingest_silver()     # Step 2: Bronze CDF → Silver SCD1

    log.info("=" * 60)
    log.info("  Pipeline finished successfully.")
    log.info("=" * 60)
