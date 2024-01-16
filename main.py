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

# ------------ STAGE 1: Collecting top 10 most popular pages each month ------------
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

# motherframe.show()
# motherframe.printSchema()
# print(f'Motherframe size == {motherframe.count()}')

# Calculate the to amount of clicks to a page for each month
allToClicks = motherframe \
    .groupBy(['file', 'to']) \
    .sum('count') \
    .select('file', col('to').alias('url'), col('sum(count)').alias('toClicks'))
# .filter('type != "external"' ) \ maybe to add later in between here 

# Get the top 10 most popular pages per month
popularPages = allToClicks.withColumn('rank', row_number().over(Window.partitionBy('file').orderBy(col('toClicks').desc()))) \
    .filter('rank <= 10') \
    .drop('rank')
    # rank stuff is for getting the top 10 urls per file regarding the toClicks
    # Taken from: https://sparkbyexamples.com/pyspark/pyspark-retrieve-top-n-from-each-group-of-dataframe/

popularPagesList = [row[0] for row in popularPages.select('url').distinct().collect()] # collect all (distinct) top 10 pages

popularToClicks = allToClicks.filter(col('url').isin(popularPagesList))
popularFromClicks = motherframe \
    .filter(col('from').isin(popularPagesList)) \
    .groupBy(['file', 'from']) \
    .sum('count') \
    .select('file', col('from').alias('url'), col('sum(count)').alias('fromClicks'))

popularFromClicks.join(popularToClicks, ['file', 'url']) \
    .coalesce(1) \
    .write \
    .option("header", "true") \
    .csv('top10eachmonthpopular.csv', mode='overwrite')
    








# toClicks = tweets \
#     .groupBy('to') \
#     .sum('count') \
#     .select(col('to').alias('url'), col('sum(count)').alias('toClicks'))

# fromClicks =  tweets \
#     .groupBy('from') \
#     .sum('count') \
#     .select(col('from').alias('url'), col('sum(count)').alias('fromClicks'))

# clicks = toClicks.join(fromClicks, 'url')

# clicks3 = clicks \
#     .where('toClicks > 10000 and fromClicks > 10000') \
#     .withColumn('ratio', col('toClicks') / col('fromClicks')) \
#     .sort('ratio', ascending = False)

