from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext(appName="Clickstream project group 20")
spark = SparkSession.builder.getOrCreate()

from pyspark.sql.functions import explode, lower, desc, col, collect_set, input_file_name, collect_list, sort_array, expr, slice, struct, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

schema = StructType([
  StructField("from", StringType()),
  StructField("to", StringType()),
  StructField("type", StringType()),
  StructField("count", IntegerType()),
])

clickstreamData = [
    spark.read \
        .option("sep", "\t") \
        .schema(schema) \
        .csv(f'/user/s1934538/clickstream/clickstream-enwiki-{year}-{month:02d}.tsv.gz') \
            for year in range(2018, 2024) \
            for month in range(1, 13)
]

# ------------ STAGE 1: Put the data from each month in 1 big DataFrame ------------
popularPages = set()

motherframe = spark.createDataFrame([], schema=StructType([
    StructField("from", StringType()),
    StructField("to", StringType()),
    StructField("type", StringType()),
    StructField("count", IntegerType()),
    StructField("file", StringType()),
]))

# Use a motherframe for ultimate paralellisation
for monthData in clickstreamData:
    motherframe = motherframe.union(monthData.withColumn('file', input_file_name()))



# ------------ STAGE 2: Collect the amount of clicks to each page ------------
allLinkToClicks = motherframe \
    .filter('type = "link"') \
    .groupBy(['file', 'to']) \
    .sum('count') \
    .select('file', col('to').alias('url'), col('sum(count)').alias('toClicksLink'))

allExternalToClicks = motherframe \
    .filter('type = "external"') \
    .groupBy(['file', 'to']) \
    .sum('count') \
    .select('file', col('to').alias('url'), col('sum(count)').alias('toClicksExternal'))

allOtherToClicks = motherframe \
    .filter('type = "other"') \
    .groupBy(['file', 'to']) \
    .sum('count') \
    .select('file', col('to').alias('url'), col('sum(count)').alias('toClicksOther'))

allToClicks = allLinkToClicks \
    .join(allExternalToClicks, ['file', 'url']) \
    .join(allOtherToClicks, ['file', 'url']) \
    .withColumn('toClicks', col('toClicksLink') + col('toClicksOther') + col('toClicksExternal'))



# ------------ STAGE 3: Collect the top 10 pages for each month (measured with toClicks) ------------
popularPages = allToClicks.withColumn('rank', row_number().over(Window.partitionBy('file').orderBy(col('toClicks').desc()))) \
    .filter('rank <= 10') \
    .drop('rank')
    # rank stuff is for getting the top 10 urls per file regarding the toClicks
    # Taken from: https://sparkbyexamples.com/pyspark/pyspark-retrieve-top-n-from-each-group-of-dataframe/

popularPagesList = [row[0] for row in popularPages.select('url').distinct().collect()] # collect all (distinct) top 10 pages

popularToClicks = allToClicks.filter(col('url').isin(popularPagesList))



# ------------ STAGE 4: Collect the clicks from the popular pages to another page for each month ------------
popularFromClicks = motherframe \
    .filter(col('from').isin(popularPagesList)) \
    .groupBy(['file', 'from']) \
    .sum('count') \
    .select('file', col('from').alias('url'), col('sum(count)').alias('fromClicks'))



# ------------ STAGE 5: Write the collected data to a csv ------------
popularToClicks.join(popularFromClicks, ['file', 'url']) \
    .coalesce(1) \
    .write \
    .option("header", "true") \
    .csv('top10eachmonthpopular.csv', mode='overwrite')
