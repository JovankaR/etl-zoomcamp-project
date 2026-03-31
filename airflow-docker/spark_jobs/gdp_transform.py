#!/usr/bin/env python
from pyspark.sql import types, functions as F

def run(spark, input_path: str, output_path: str):
    # 1️⃣ Define schema
    schema = types.StructType([
        types.StructField('year', types.IntegerType(), True),
        types.StructField('rank', types.IntegerType(), True),
        types.StructField('country', types.StringType(), True),
        types.StructField('state', types.StringType(), True),
        types.StructField('gdp', types.LongType(), True),
        types.StructField('gdp_percent', types.DoubleType(), True)
    ])

    # 2️⃣ Load data and drop unnecessary columns
    df = spark.read.option("header", "true").schema(schema).csv(input_path).drop("rank", "state")

    # 3️⃣ Round GDP percentage
    df_transformed = df.withColumn("gdp_percent", F.round(F.col("gdp_percent"), 5))

    # 4️⃣ Clean country names
    df_clean = df_transformed.withColumn(
        "country",
        F.when(F.col("country") == "C ô te d'Ivoire", "Cote d'Ivoire")
        .when(F.col("country") == "South Sultan", "South Sudan")
        .when(F.col("country") == "Fiji Islands", "Fiji")
        .when(F.col("country") == "Cura ç Ao", "Curacao")
        .when(F.col("country") == "the United States", "United States")
        .when(F.col("country") == "Sao Tome and Principe.", "Sao Tome and Principe")
        .when(F.col("country") == "Columbia", "Colombia")
        .when(F.col("country") == "Isle of man", "Isle of Man")
        .when(F.col("country") == "Guinea Bissau", "Guinea-Bissau")
        .when(F.col("country") == "Czech", "Czech Republic")
        .when(F.col("country") == "Congo (gold)", "Democratic Republic of Congo")
        .when(F.col("country") == "Congo (Brazzaville)", "Congo")
        .when(F.col("country") == "Central Africa", "Central African Republic")
        .otherwise(F.col("country"))
    )

    # 5️⃣ Write output to Parquet
    df_clean.coalesce(1).write.mode("overwrite").parquet(output_path)

    # 6️⃣ Return DataFrame for further processing
    return df_clean