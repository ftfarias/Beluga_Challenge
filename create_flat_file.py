from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession \
    .builder \
    .appName("Beluga''s Challenge") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


fact = spark.read.load("data/fact.csv", format="csv", sep=",", inferSchema="true", header="true")
dim = spark.read.load("data/dim.csv", format="csv", sep=",", inferSchema="true", header="true")
flat = fact.join(dim, "product").drop(dim._c0)

# some names have spaces, so replace to underscore
for c in list(flat.columns):
    if ' ' in c:
        flat = flat.withColumnRenamed(c, c.replace(' ', '_'))


def fix_gender(txt):
    txt = txt.lower()
    if 'bebés' in txt or 'bebes' in txt:
        return 'bebes'
    if 'boys' in txt or \
            'girls' in txt or \
            'infantil' in txt or \
            'niños' in txt or \
            'niñas' in txt or \
            'menino' in txt or \
            'menina' in txt:
        return 'infantil'

    if 'unisex' in txt or 'unissex' in txt:
        return 'unisex'

    if 'masculino' in txt:
        return 'masculino'

    if 'femenino' in txt or 'feminino' in txt:
        return 'feminino'

    return txt


gender_fix_udf = F.UserDefinedFunction(fix_gender, T.StringType())
flat = flat.withColumn('gender_fixed', gender_fix_udf('gender'))

# convert Order_date from int to datetime
flat = flat.select('*', F.unix_timestamp(flat['order_date'].cast('string'), 'yyyyMMdd').cast('timestamp').alias('order_date_iso'))

# verify with flat.groupBy('gender_fixed').count().sort('count').show(100)

flat = flat.withColumn('average_cost_per_item', flat.product_net_cost / flat.item_sold)
flat = flat.withColumn('absolute_margin', flat.product_net_cost - flat.product_net_revenue)
flat = flat.withColumn('percentage_margin', flat.absolute_margin / flat.product_net_cost)

# flat = flat.withColumn('average_price_per_item', flat. / flat.)
# flat = flat.withColumn('var_average_cost_per_item', flat. / flat.)

flat = flat.drop('gender')
flat = flat.drop('order_date')

flat.write.partitionBy("order_date_iso").format("parquet").save("data/flat.parquet")
flat.write.format("json").save("data/flat.json")




