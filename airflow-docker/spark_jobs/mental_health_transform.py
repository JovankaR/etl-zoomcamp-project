#!/usr/bin/env python
from pyspark.sql import types, functions as F
from pyspark.sql.window import Window

def run(spark, input_path: str, output_path: str):
    # 1️⃣ Define schema
    schema = types.StructType([
        types.StructField('index', types.StringType(), True),
        types.StructField('Entity', types.StringType(), True),
        types.StructField('Code', types.StringType(), True),
        types.StructField('Year', types.IntegerType(), True),
        types.StructField('Schizophrenia (%)', types.DoubleType(), True),
        types.StructField('Bipolar disorder (%)', types.DoubleType(), True),
        types.StructField('Eating disorders (%)', types.DoubleType(), True),
        types.StructField('Anxiety disorders (%)', types.DoubleType(), True),
        types.StructField('Drug use disorders (%)', types.DoubleType(), True),
        types.StructField('Depression (%)', types.DoubleType(), True),
        types.StructField('Alcohol use disorders (%)', types.DoubleType(), True)
    ])

    # 2️⃣ Read CSV
    df_raw = spark.read.option("header", "true").schema(schema).csv(input_path)

    # 3️⃣ Rename columns
    column_mapping = {
        "Entity": "country",
        "Year": "year",
        "Schizophrenia (%)": "schizophrenia",
        "Bipolar disorder (%)": "bipolar_disorder",
        "Eating disorders (%)": "eating_disorders",
        "Anxiety disorders (%)": "anxiety_disorders",
        "Drug use disorders (%)": "drug_use_disorders",
        "Depression (%)": "depression",
        "Alcohol use disorders (%)": "alcohol_use_disorders"
    }

    df_renamed = df_raw.select([F.col(old).alias(new) for old, new in column_mapping.items()]).dropDuplicates()

    # 4️⃣ Impute NULLs for all mental health columns (all except 'country' and 'year')
    window_spec = Window.partitionBy("country")
    cols_to_impute = [c for c in df_renamed.columns if c not in ["country", "year"]]

    for col_name in cols_to_impute:
        country_avg = F.avg(F.col(col_name)).over(window_spec)
        df_renamed = df_renamed.withColumn(col_name, F.coalesce(F.col(col_name), country_avg))

    # 5️⃣ Round all numeric columns
    df_final = df_renamed.select(
        "country",
        "year",
        *[F.round(F.col(c), 2).alias(c) for c in cols_to_impute]
    )

    # 6️⃣ Write output to Parquet
    df_final.coalesce(1).write.mode("overwrite").parquet(output_path)

    return df_final