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
            for year in range(2023, 2024) \
            for month in range(10, 13)
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

for monthData in clickstreamData:
    motherframe = motherframe.union(monthData.withColumn('file', input_file_name()))

    # toClicks = monthData \
    #     .groupBy('to') \
    #     .sum('count') \
    #     .select(col('to').alias('url'), col('sum(count)').alias('toClicks')) \
    #     .sort('toClicks', ascending=False) \
    #     .limit(10) \
    #     .select('url').rdd.flatMap(lambda x: x).collect() # https://stackoverflow.com/questions/38610559/convert-spark-dataframe-column-to-python-list
    
    # popularPages.update(set(toClicks))

motherframe.show()
motherframe.printSchema()


windowDept = Window.partitionBy("url").orderBy(col("toClicks").desc())
df2=df.withColumn("row",row_number().over(windowDept))

popularPages = motherframe \
    .groupBy(['file', 'to']) \
    .sum('count') \
    .select('file', col('to').alias('url'), col('sum(count)').alias('toClicks')) \
    \
    .withColumn('rank', row_number().over(Window.partitionBy('file', "url").orderBy(col("toClicks").desc()))) \
    .filter('rank <= 10') \
    .drop('rank')
    # .groupBy('file').agg(slice(sort_array(collect_list(struct('toClicks', 'url')), asc=False),1,10)) \
    # .sort('toClicks', ascending=False) \
    # .limit(10) \
    .select('file', 'url').collect() # https://stackoverflow.com/questions/38610559/convert-spark-dataframe-column-to-python-list

print(popularPages)












toClicks = tweets \
    .groupBy('to') \
    .sum('count') \
    .select(col('to').alias('url'), col('sum(count)').alias('toClicks'))

fromClicks =  tweets \
    .groupBy('from') \
    .sum('count') \
    .select(col('from').alias('url'), col('sum(count)').alias('fromClicks'))

clicks = toClicks.join(fromClicks, 'url')

clicks3 = clicks \
    .where('toClicks > 10000 and fromClicks > 10000') \
    .withColumn('ratio', col('toClicks') / col('fromClicks')) \
    .sort('ratio', ascending = False)

