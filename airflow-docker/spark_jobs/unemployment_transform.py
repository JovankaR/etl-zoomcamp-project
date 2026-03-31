#!/usr/bin/env python
from pyspark.sql import types, functions as F

def run(spark, input_path: str, output_path: str):
    """
    Reads wide-format unemployment CSV, unpivots years into long format,
    cleans country names, rounds unemployment rates, and writes to Parquet.
    """
    # 1️⃣ Define schema
    schema = types.StructType([
        types.StructField("Country Name", types.StringType(), True)
    ] + [
        types.StructField(str(year), types.DoubleType(), True)
        for year in range(1991, 2022)
    ])

    # 2️⃣ Read CSV with schema
    df_raw = spark.read.option("header", "true").schema(schema).csv(input_path)

    # 3️⃣ Unpivot (melt) year columns into 'year' and 'unemployment_rate'
    year_columns = [str(year) for year in range(1991, 2022)]
    df_long = df_raw.melt(
        ids="Country Name",
        values=year_columns,
        variableColumnName="year",
        valueColumnName="unemployment_rate"
    )

    # 4️⃣ Clean column names
    df_clean = df_long.withColumnRenamed("Country Name", "country") \
                      .withColumn("year", F.col("year").cast("int"))

    # 5️⃣ Replace country names with standardized versions
    country_map = {
        "Russian Federation": "Russia",
        "Eswatini": "Swaziland",
        "Turkiye": "Turkey",
        "Korea, Dem. People's Rep.": "North Korea",
        "Korea, Rep.": "South Korea",
        "Hong Kong SAR, China": "Hong Kong",
        "Macao SAR, China": "Macao",
        "Timor-Leste": "Timor",
        "North Macedonia": "Macedonia",
        "West Bank and Gaza": "Palestine",
        "Venezuela, RB": "Venezuela",
        "Bahamas, The": "Bahamas",
        "Gambia, The": "Gambia",
        "Slovak Republic": "Slovakia",
        "St. Lucia": "Saint Lucia",
        "St. Vincent and the Grenadines": "Saint Vincent and the Grenadines"
    }
    df_clean = df_clean.replace(country_map, subset=["country"])

    # 6️⃣ Round unemployment_rate and drop nulls
    df_final = df_clean.withColumn(
        "unemployment_rate",
        F.round(F.col("unemployment_rate"), 2)
    ).filter(F.col("unemployment_rate").isNotNull())

    # 7️⃣ Write to Parquet
    df_final.coalesce(1).write.mode("overwrite").parquet(output_path)

    return df_final