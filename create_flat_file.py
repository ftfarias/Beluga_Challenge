from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Beluga''s Challenge") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


fact = spark.read.load("data/fact.csv", format="csv", sep=",", inferSchema="true", header="true")
dim = spark.read.load("data/dim.csv", format="csv", sep=",", inferSchema="true", header="true")
merged = fact.join(dim, "product").drop(dim._c0)

# some names have spaces, so replace to underscore
for c in list(merged.columns):
    if ' ' in c:
        merged = merged.withColumnRenamed(c, c.replace(' ','_'))


merged.write.partitionBy("order_date").format("parquet").save("data/flat.parquet")

