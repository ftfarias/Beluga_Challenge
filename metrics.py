from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Beluga''s Challenge") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


flat = spark.read.load("data/flat.parquet", format="parquet")
flat.show(10)

