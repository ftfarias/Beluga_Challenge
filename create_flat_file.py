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

# verify with flat.groupBy('gender_fixed').count().sort('count').show(100)

flat = flat.withColumn('average_cost_per_item', flat.product_net_cost / flat.item_sold)
flat = flat.withColumn('absolute_margin', flat.product_net_cost - flat.product_net_revenue)
flat = flat.withColumn('percentage_margin', flat.absolute_margin / flat.product_net_cost)
flat = flat.withColumn('average_price_per_item', flat.product_net_revenue / flat.item_sold)

flat = flat.select('*', F.unix_timestamp(flat['order_date'].cast('string'), 'yyyyMMdd').cast('timestamp').alias('order_date_iso'))

avg_day = flat.groupBy('order_date_iso').agg(F.avg('average_price_per_item'))

avg_day = avg_day.withColumn('next_day', F.date_add(avg_day.order_date_iso, 1).cast('timestamp')).cache()
avg_day = avg_day.withColumnRenamed('avg(average_price_per_item)', 'average_price_per_item_yesterday')

flat = flat.join(avg_day, flat.order_date_iso == avg_day.next_day).drop(avg_day.next_day).drop(flat.order_date_iso)
# to do: Transform this in a left join and take care of missing values


flat = flat.withColumn('var_average_price_per_item', (flat.average_price_per_item / flat.average_price_per_item_yesterday) - 1)

"""
avg_day.show()
+-------------------+--------------------------------+-------------------+      
|     order_date_iso|average_price_per_item_yesterday|           next_day|
+-------------------+--------------------------------+-------------------+
|2018-02-05 00:00:00|              3753.2424873274276|2018-02-06 00:00:00|
|2018-02-11 00:00:00|              3334.6888569539296|2018-02-12 00:00:00|
|2018-02-08 00:00:00|               3757.228272578487|2018-02-09 00:00:00|
|2018-02-06 00:00:00|              3262.1929569669637|2018-02-07 00:00:00|
|2018-02-07 00:00:00|              3347.5609365254863|2018-02-08 00:00:00|
|2018-02-12 00:00:00|               4161.830326558264|2018-02-13 00:00:00|
|2018-02-13 00:00:00|              10890.724625592758|2018-02-14 00:00:00|
|2018-02-04 00:00:00|              2945.9924421968985|2018-02-05 00:00:00|
|2018-02-09 00:00:00|               3379.331244957616|2018-02-10 00:00:00|
|2018-02-10 00:00:00|              3256.6051411139615|2018-02-11 00:00:00|
+-------------------+--------------------------------+-------------------+
"""

flat = flat.drop('gender')
flat = flat.drop('order_date')
flat = flat.drop('_c0')
flat = flat.drop('id')
flat = flat.drop('view_date')
flat = flat.drop('device')
flat = flat.drop('currency')
flat = flat.drop('channel')
flat = flat.drop('store')
flat = flat.drop('company')
flat = flat.drop('view_date')
flat = flat.drop('store')


#grouped = flat.groupBy("product","order_date_iso").agg(sum("item_sold"), sum("gross_total_volume"),sum("pageviews"))

flat.createOrReplaceTempView("vw_flat")


vw_flat = spark.sql("SELECT product, order_date_iso, sum(gross_total_volume) as  gross_total_volume, " +
                    "sum(product_net_cost) as  product_net_cost, sum(product_net_revenue) as  product_net_revenue, " +
                    "sum(gross_merchandise_volume) as  gross_merchandise_volume, sum(item_sold) as item_sold, " +
                    "sum(pageviews) as pageviews " +
                    "FROM vw_flat group by product, order_date_iso")
""" 
vw_flat.show()

+-------+-------------------+------------------+----------------+-------------------+------------------------+---------+---------+
|product|     order_date_iso|gross_total_volume|product_net_cost|product_net_revenue|gross_merchandise_volume|item_sold|pageviews|
+-------+-------------------+------------------+----------------+-------------------+------------------------+---------+---------+
| 103439|2018-02-04 00:00:00|               0.0|             0.0|                0.0|                     0.0|      0.0|        2|
| 269704|2018-02-04 00:00:00|               0.0|             0.0|                0.0|                     0.0|      0.0|        1|
| 479274|2018-02-04 00:00:00|             21.99|           10.51|               16.0|                   21.99|      1.0|       23|
| 561307|2018-02-04 00:00:00|              70.0|           42.65|              50.92|                    70.0|      1.0|       20|
| 602841|2018-02-04 00:00:00|               0.0|             0.0|                0.0|                     0.0|      0.0|        4|
| 866371|2018-02-04 00:00:00|               0.0|             0.0|                0.0|                     0.0|      0.0|        1|
| 965905|2018-02-04 00:00:00|               0.0|             0.0|                0.0|                     0.0|      0.0|        1|
|3594073|2018-02-04 00:00:00|               0.0|             0.0|                0.0|                     0.0|      0.0|        1|
|3855343|2018-02-04 00:00:00|               0.0|             0.0|                0.0|                     0.0|      0.0|        1|
| 303254|2018-02-05 00:00:00|               0.0|             0.0|                0.0|                     0.0|      0.0|       23|
| 394724|2018-02-05 00:00:00|               0.0|             0.0|                0.0|                     0.0|      0.0|       31|
| 563880|2018-02-05 00:00:00|               0.0|             0.0|                0.0|                     0.0|      0.0|       24|
| 625883|2018-02-05 00:00:00|               0.0|             0.0|                0.0|                     0.0|      0.0|       12|
| 942901|2018-02-05 00:00:00|               0.0|             0.0|                0.0|                     0.0|      0.0|        8|
|1031923|2018-02-05 00:00:00|               0.0|             0.0|                0.0|                     0.0|      0.0|       19|
|3492857|2018-02-05 00:00:00|               0.0|             0.0|                0.0|                     0.0|      0.0|        2|
|3877181|2018-02-05 00:00:00|               0.0|             0.0|                0.0|                     0.0|      0.0|       15|
|4075860|2018-02-05 00:00:00|               0.0|             0.0|                0.0|                     0.0|      0.0|        2|
|4275845|2018-02-05 00:00:00|               0.0|             0.0|                0.0|                     0.0|      0.0|        1|
|  17075|2018-02-06 00:00:00|               0.0|             0.0|                0.0|                     0.0|      0.0|       56|
+-------+-------------------+------------------+----------------+-------------------+------------------------+---------+---------+
"""

flat.write.partitionBy("order_date_iso").format("parquet").save("data/flat.parquet")
flat.write.format("json").save("data/flat.json")
vw_flat.write.format("json").save("data/flat_totais.json")

# extra question
product_order = spark.sql("SELECT product, order_date_iso, count(1) as quantidade FROM vw_flat group by product, order_date_iso")
product_order.write.csv("data/product_order.csv")


