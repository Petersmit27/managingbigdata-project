from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext(appName="Clickstream project group 20 cleoclicks.py")
spark = SparkSession.builder.getOrCreate()

from pyspark.sql.functions import col, input_file_name, substring
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
motherframe = spark.createDataFrame([], schema=StructType([
    StructField("from", StringType()),
    StructField("to", StringType()),
    StructField("type", StringType()),
    StructField("count", IntegerType()),
    StructField("year", StringType()),
]))

# Use a motherframe for ultimate paralellisation
for monthData in clickstreamData:
    motherframe = motherframe.union(monthData.withColumn('year', substring(input_file_name(), 79, 4).cast(IntegerType()).alias('year')))

# Sum all the clicks in 1 year from cleopatra
summedLinkClicks = motherframe \
    .filter('type = "link"') \
    .drop('type') \
    .groupBy(['year', 'from', 'to']) \
    .sum('count').withColumnRenamed('sum(count)', 'count')

# for the Cleopatra page, get top 100 pages of all time from it 
popularToPages = summedLinkClicks \
    .filter('from = "Cleopatra"') \
    .groupBy(['from', 'to']) \
    .sum('count').withColumnRenamed('sum(count)', 'count') \
    .orderBy(col('count').desc()) \
    .limit(100) \
    .select('to') \
    .rdd.flatMap(lambda x: x).collect() # convert to python list of top 100 pages

popularToPages.append('Cleopatra')


# filter the summed motherframe on the cleopatra page and the top 100 related pages for each year. Write to a csv
summedLinkClicks \
    .filter(col('from').isin(popularToPages) | col('to').isin(popularToPages)) \
    .coalesce(1) \
    .write \
    .option("header", "true") \
    .option("delimiter", "\t") \
    .csv('cleoclicks.tsv', mode='overwrite')


