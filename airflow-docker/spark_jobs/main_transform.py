#!/usr/bin/env python
import argparse
from pyspark.sql import SparkSession

# Import refactored transformation modules
import mental_health_transform
import gdp_transform
import unemployment_transform

# Parse command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--project_id", required=True, help="GCP Project ID")
parser.add_argument("--bucket_name", required=True, help="GCS Bucket Name")
parser.add_argument("--bq_dataset", required=True, help="BigQuery dataset name")
args = parser.parse_args()

PROJECT_ID = args.project_id
BUCKET_NAME = args.bucket_name
BQ_DATASET = args.bq_dataset

if not PROJECT_ID or not BUCKET_NAME or not BQ_DATASET:
    raise ValueError("❌ Missing PROJECT_ID, BUCKET_NAME, or BQ_DATASET arguments.")

def create_spark_session(app_name: str = "DataPipeline"):
    """Create a Spark session."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def write_to_bq(df, table: str, bucket_name: str):
    """Write Spark DataFrame directly to BigQuery."""
    if df is None or df.rdd.isEmpty():
        raise ValueError(f"❌ DataFrame is empty, cannot write to BigQuery: {table}")

    df.write.format("bigquery") \
        .option("table", table) \
        .option("temporaryGcsBucket", bucket_name) \
        .mode("overwrite") \
        .save()
    print(f"✅ Written FINAL table to BigQuery: {table}")

def main():
    global spark
    spark = create_spark_session()
    bucket = f"gs://{BUCKET_NAME}"

    paths = {
        "mental_health_input": f"{bucket}/raw/mental_health_data.csv",
        "mental_health_output": f"{bucket}/processed/mental_health_data",
        "gdp_input": f"{bucket}/raw/gdp_data.csv",
        "gdp_output": f"{bucket}/processed/gdp_data",
        "unemployment_input": f"{bucket}/raw/unemployment_data.csv",
        "unemployment_output": f"{bucket}/processed/unemployment_data",
        "final_bq_table": f"{PROJECT_ID}.{BQ_DATASET}.analytics_final",
    }

    # 1️⃣ Run transformations
    df_mental = mental_health_transform.run(spark, paths["mental_health_input"], paths["mental_health_output"])
    df_gdp = gdp_transform.run(spark, paths["gdp_input"], paths["gdp_output"])
    df_unemp = unemployment_transform.run(spark, paths["unemployment_input"], paths["unemployment_output"])

    # 2️⃣ Register DataFrames as temporary views for SQL join
    if df_mental is not None:
        df_mental = df_mental.withColumn("year", df_mental["year"].cast("int")) \
                             .withColumn("country", df_mental["country"].cast("string"))
        df_mental.createOrReplaceTempView("mental_health")

    if df_gdp is not None:
        df_gdp = df_gdp.withColumn("year", df_gdp["year"].cast("int")) \
                       .withColumn("country", df_gdp["country"].cast("string"))
        df_gdp.createOrReplaceTempView("gdp")

    if df_unemp is not None:
        df_unemp = df_unemp.withColumn("year", df_unemp["year"].cast("int")) \
                           .withColumn("country", df_unemp["country"].cast("string"))
        df_unemp.createOrReplaceTempView("unemployment")

    # 3️⃣ Join datasets using Spark SQL
    sql_query = """
        SELECT m.*, g.gdp_percent, g.gdp, u.unemployment_rate
        FROM mental_health m
        INNER JOIN gdp g ON m.country = g.country AND m.year = g.year
        INNER JOIN unemployment u ON m.country = u.country AND m.year = u.year
    """
    df_final = spark.sql(sql_query)

    # 4️⃣ Write the final joined DataFrame directly to BigQuery
    write_to_bq(df_final, paths["final_bq_table"], BUCKET_NAME)

    print("✅ Pipeline completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()