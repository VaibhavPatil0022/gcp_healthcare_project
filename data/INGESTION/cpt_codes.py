from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CPT Codes Ingestion") \
    .getOrCreate()

# Configure variables
BUCKET_NAME = "healthcare-bucket-26"
CPT_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/cptcodes/*.csv"
BQ_TABLE = "cloud-vaib22.bronze_dataset.cpt_codes"
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"

# Read from CPT codes landing zone
cptcodes_df = spark.read.csv(CPT_BUCKET_PATH, header=True)

# Replace spaces with underscores and lowercase column names
for col in cptcodes_df.columns:
    new_col = col.replace(" ", "_").lower()
    cptcodes_df = cptcodes_df.withColumnRenamed(col, new_col)

# Write to BigQuery bronze dataset
(cptcodes_df.write
    .format("bigquery")
    .option("table", BQ_TABLE)
    .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
    .mode("overwrite")
    .save())