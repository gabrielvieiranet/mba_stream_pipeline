from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, IntegerType, StringType, StructField,
                               StructType)


def initialize_spark_session(app_name="SparkApplication"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_csv(spark, file_path):
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("id", IntegerType(), True),
        StructField("minutes", IntegerType(), True),
        StructField("contributor_id", IntegerType(), True),
        StructField("submitted", DateType(), True),
        StructField("tags", StringType(), True),
        StructField("nutrition", StringType(), True),
        StructField("n_steps", IntegerType(), True),
        StructField("steps", StringType(), True),
        StructField("description", StringType(), True),
        StructField("ingredients", StringType(), True),
        StructField("n_ingredients", IntegerType(), True)
    ])
    return spark.read.csv(file_path, header=True, schema=schema)
