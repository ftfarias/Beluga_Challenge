# Beluga_Challenge

Answer for my Data Engineering Challenge 

## Internal Structure

* A directory structure were created with CookieCutter template (https://drivendata.github.io/cookiecutter-data-science)

## Requirements

* Python 3.6 must be installed
* All python dependences must be installed (pip install -r requirements.txt)
* Both datafiles (dim.csv and fact.csv) should be in "data" directory

## Spark

* The solution was tested in Spark 2.3.0 (with Hadoop 2.7)
    * https://www.apache.org/dyn/closer.lua/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz
* The Spark was started as local server (sbin/start_master.sh) with one worker. For production more workers should be added and a true cluster manager (like Yarn)

## Pre-analysis

Using "head" command I checked the structure of both file:

```
Felipes-iMac:data ftfarias$ head dim.csv
,is_banner,is_tax,is_market_place,material_status,current_price_range,cmc_division,cmc_business_unit,gender,product
0,,,1,,02. 25-50,Marketplace,Marketplace,Unissex,2362269
1,,,1,,03. 50-75,Marketplace,Marketplace,Feminino,2416655
2,,,1,,02. 25-50,Sports Apparel,Sports,Masculino,2360014
3,,,1,,05. 100-150,Sports Shoes,Sports,Masculino,2433284
4,,0.0,0,,05. 100-150,Sports Apparel,Sports,Masculino,2367409
5,,,1,,04. 75-100,Marketplace,Marketplace,Masculino,2444089
6,,0.0,0,,06. 150-200,Sports Apparel,Sports,Masculino,2393759
7,,,1,,03. 50-75,Marketplace,Marketplace,Masculino,2453109
8,,,1,,08. 250-300,Sports Shoes,Sports,Masculino,2370662

Felipes-iMac:data ftfarias$ head fact.csv
,id,order_date,view_date,currency,device,product,channel,store,company,view_date,gross total volume,product net cost,product net revenue,gross merchandise volume,item sold,pageviews
0,185,20180209,20180209,20,1,1401768,,6,1,2018-02-09 00:00:00,33.19,12.74,21.67,29.8,0.4,6
1,185,20180205,20180205,20,1,3953112,,6,1,2018-02-05 00:00:00,83.6,29.15,60.81,83.6,1.0333333334,15
2,185,20180206,20180206,20,23,3676330,,6,1,2018-02-06 00:00:00,29.8,13.22,21.68,29.8,0.4,8
3,15,20180212,20180212,20,6,1585703,,6,1,2018-02-12 00:00:00,18.28,7.94,13.3,18.28,0.4571428572,1
4,15,20180209,20180209,20,23,3467861,,6,1,2018-02-09 00:00:00,27.45,7.48,18.0,24.75,1.0,4
5,15,20180205,20180205,20,23,3551836,,6,1,2018-02-05 00:00:00,61.95,30.18,36.92,50.76,0.4,2
6,15,20180207,20180204,20,23,4082855,,6,1,2018-02-04 00:00:00,2.5,0.98,1.77,2.5,0.05,9
7,15,20180206,20180111,20,23,3695229,,6,1,2018-01-11 00:00:00,1.29,0.75,0.94,1.29,0.0071428570999999995,1
8,15,20180212,20180212,20,23,4057131,,6,1,2018-02-12 00:00:00,9.68,4.25,6.31,7.89,0.1,1
```

## Spark Interactive

I opened a pyspark shell for the first tests with the database, and copying the results to create_flat_file.py as it progressed.

```
fact = spark.read.load("/Users/ftfarias/projects/Beluga_Challenge/data/fact.csv", format="csv", sep=",", inferSchema="true", header="true")
fact.show()

# Checking if the import is consistent
fact.printSchema()
root
 |-- _c0: integer (nullable = true)
 |-- id: integer (nullable = true)
 |-- order_date: integer (nullable = true)
 |-- view_date3: integer (nullable = true)
 |-- currency: integer (nullable = true)
 |-- device: integer (nullable = true)
 |-- product: integer (nullable = true)
 |-- channel: string (nullable = true)
 |-- store: integer (nullable = true)
 |-- company: integer (nullable = true)
 |-- view_date10: timestamp (nullable = true)
 |-- gross total volume: double (nullable = true)
 |-- product net cost: double (nullable = true)
 |-- product net revenue: double (nullable = true)
 |-- gross merchandise volume: double (nullable = true)
 |-- item sold: double (nullable = true)
 |-- pageviews: integer (nullable = true)

dim = spark.read.load("/Users/ftfarias/projects/Beluga_Challenge/data/dim.csv", format="csv", sep=",", inferSchema="true", header="true")
dim.show()

root
 |-- _c0: integer (nullable = true)
 |-- is_banner: double (nullable = true)
 |-- is_tax: double (nullable = true)
 |-- is_market_place: integer (nullable = true)
 |-- material_status: string (nullable = true)
 |-- current_price_range: string (nullable = true)
 |-- cmc_division: string (nullable = true)
 |-- cmc_business_unit: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- product: integer (nullable = true)


merged = fact.join(dim, fact.product == dim.product)
merged.take(10)
```
### Gender

The gender field has too many overlapping values, this needs to be reduced to field with at most 5 different values (use your own criteria to deduce this)


>>> gender = flat.select('gender').distinct().count()
87

>>> genders = flat.groupBy('gender').count().sort('count').cache()
>>> genders.show(100)
```
+--------------------+-------+
|              gender|  count|
+--------------------+-------+
...
|Bebés/Masculino/boys|    102|
|   Unisex/boys/girls|    124|
|        Niños/Unisex|    133|
|         Bebés/girls|    134|
|          boys/girls|    141|
|      Infantil/Niños|    153|
|      Infantil/girls|    170|
|      Infantil/Niñas|    229|
|Femenino/Masculin...|    370|
|Bebés/Femenino/Ma...|    401|
|    Masculino/Unisex|    425|
|     Masculino/Niños|    464|
|     Femenino/Unisex|    484|
|      Masculino/boys|    485|
|Femenino/Masculin...|    547|
|      Femenino/girls|    704|
|      Femenino/Niñas|   1030|
|     Unisex Infantil|   1149|
|  Femenino/Masculino|   1306|
|Femenino/Masculin...|   4661|
|               Bebés|   8145|
|                boys|  15343|
|            Infantil|  17160|
|               girls|  18596|
|               Niños|  22948|
|               Niñas|  32816|
|              Unisex| 105819|
|              menino|1021441|
|             unissex|1116933|
|           Masculino|1154544|
|              menina|1816657|
|            Femenino|2720894|
|           masculino|4405208|
|            feminino|8847235|
+--------------------+-------+
```

Using the count information, the five groups are:

```
masculino
feminino
unisex
infantil
bebes
```

## Running scripts:

### To create the flat file:

* ~/Downloads/spark/bin/spark-submit create_flat_file.py --master local[*]


The result table can be checked in the data file:

```
Felipes-iMac:Beluga_Challenge ftfarias$ ll data/flat.parquet/
total 8
drwxr-xr-x   14 ftfarias  staff    476 Apr 11 10:03 .
drwxr-xr-x    8 ftfarias  staff    272 Apr 11 10:00 ..
-rw-r--r--    1 ftfarias  staff      8 Apr 11 10:03 ._SUCCESS.crc
-rw-r--r--    1 ftfarias  staff      0 Apr 11 10:03 _SUCCESS
drwxr-xr-x  402 ftfarias  staff  13668 Apr 11 10:03 order_date=20180204
drwxr-xr-x  402 ftfarias  staff  13668 Apr 11 10:03 order_date=20180205
drwxr-xr-x  402 ftfarias  staff  13668 Apr 11 10:03 order_date=20180206
drwxr-xr-x  402 ftfarias  staff  13668 Apr 11 10:03 order_date=20180207
drwxr-xr-x  402 ftfarias  staff  13668 Apr 11 10:03 order_date=20180208
drwxr-xr-x  402 ftfarias  staff  13668 Apr 11 10:03 order_date=20180209
drwxr-xr-x  402 ftfarias  staff  13668 Apr 11 10:03 order_date=20180210
drwxr-xr-x  402 ftfarias  staff  13668 Apr 11 10:03 order_date=20180211
drwxr-xr-x  402 ftfarias  staff  13668 Apr 11 10:03 order_date=20180212
drwxr-xr-x  402 ftfarias  staff  13668 Apr 11 10:03 order_date=20180213

Felipes-iMac:Beluga_Challenge ftfarias$ head data/flat.json/part-00199-d5fd9bc3-8743-4748-a22a-7e21cb9df617-c000.json
{"product":422,"_c0":26894,"id":2451,"view_date3":20180207,"currency":0,"device":23,"store":6,"company":1,"view_date10":"2018-02-07T00:00:00.000-02:00","gross_total_volume":0.0,"product_net_cost":0.0,"product_net_revenue":0.0,"gross_merchandise_volume":0.0,"item_sold":0.0,"pageviews":1,"is_banner":0.0,"is_market_place":0,"current_price_range":"09. 300-400","cmc_division":"Women Apparel","cmc_business_unit":"Women","gender_fixed":"feminino","order_date_iso":"2018-02-07T00:00:00.000-02:00","absolute_margin":0.0}
{"product":422,"_c0":80035,"id":89,"view_date3":20180205,"currency":0,"device":23,"store":308,"company":1,"view_date10":"2018-02-05T00:00:00.000-02:00","gross_total_volume":0.0,"product_net_cost":0.0,"product_net_revenue":0.0,"gross_merchandise_volume":0.0,"item_sold":0.0,"pageviews":1,"is_banner":0.0,"is_market_place":0,"current_price_range":"09. 300-400","cmc_division":"Women Apparel","cmc_business_unit":"Women","gender_fixed":"feminino","order_date_iso":"2018-02-05T00:00:00.000-02:00","absolute_margin":0.0}
{"product":422,"_c0":246907,"id":47,"view_date3":20180206,"currency":0,"device":1,"store":6,"company":1,"view_date10":"2018-02-06T00:00:00.000-02:00","gross_total_volume":0.0,"product_net_cost":0.0,"product_net_revenue":0.0,"gross_merchandise_volume":0.0,"item_sold":0.0,"pageviews":1,"is_banner":0.0,"is_market_place":0,"current_price_range":"09. 300-400","cmc_division":"Women Apparel","cmc_business_unit":"Women","gender_fixed":"feminino","order_date_iso":"2018-02-06T00:00:00.000-02:00","absolute_margin":0.0}
{"product":422,"_c0":30335,"id":86,"view_date3":20180204,"currency":0,"device":1,"store":6,"company":1,"view_date10":"2018-02-04T00:00:00.000-02:00","gross_total_volume":0.0,"product_net_cost":0.0,"product_net_revenue":0.0,"gross_merchandise_volume":0.0,"item_sold":0.0,"pageviews":1,"is_banner":0.0,"is_market_place":0,"current_price_range":"09. 300-400","cmc_division":"Women Apparel","cmc_business_unit":"Women","gender_fixed":"feminino","order_date_iso":"2018-02-04T00:00:00.000-02:00","absolute_margin":0.0}
{"product":422,"_c0":218458,"id":2342,"view_date3":20180206,"currency":0,"device":10,"store":6,"company":1,"view_date10":"2018-02-06T00:00:00.000-02:00","gross_total_volume":0.0,"product_net_cost":0.0,"product_net_revenue":0.0,"gross_merchandise_volume":0.0,"item_sold":0.0,"pageviews":2,"is_banner":0.0,"is_market_place":0,"current_price_range":"09. 300-400","cmc_division":"Women Apparel","cmc_business_unit":"Women","gender_fixed":"feminino","order_date_iso":"2018-02-06T00:00:00.000-02:00","absolute_margin":0.0}
{"product":422,"_c0":252650,"id":3,"view_date3":20180208,"currency":20,"device":23,"store":6,"company":1,"view_date10":"2018-02-08T00:00:00.000-02:00","gross_total_volume":149.99,"product_net_cost":124.97,"product_net_revenue":109.12,"gross_merchandise_volume":149.99,"item_sold":1.0,"pageviews":0,"is_banner":0.0,"is_market_place":0,"current_price_range":"09. 300-400","cmc_division":"Women Apparel","cmc_business_unit":"Women","gender_fixed":"feminino","order_date_iso":"2018-02-08T00:00:00.000-02:00","average_cost_per_item":124.97,"absolute_margin":15.849999999999994,"percentage_margin":0.12683043930543325}


```

## Extras
```
Plot the distribution of the product dimension over the order_date
for the fact table. What kind of problems could you run into whenever
parallelizing processing using spark or hadoop using this field as a key?
How can you resolve such issues?
```

There are a non-uniform distribution of the values, which can make some servers in the cluster more loaded and
delay the entire processing. A Surrogate key can help to homogenaze the servers load

The graph of distribution is in notebook

```
How would you optimize the elasticsearch index taking into account
that it will not be used for full text search, only for timeseries
events queries? (mainly aggregations and field value retrieval)
```

There are few optimizations, first when you do search, you should use
Round Dates ( [https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-search-speed.html#_search_rounded_dates].


