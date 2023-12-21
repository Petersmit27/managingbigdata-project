from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, lower, desc, col, collect_set

spark = SparkSession.builder.getOrCreate()

from pyspark.sql.types import StringType, IntegerType, StructType, StructField

schema = StructType([
  StructField("from", StringType()),
  StructField("to", StringType()),
  StructField("type", StringType()),
  StructField("count", IntegerType()),
])

tweets = spark.read \
    .option("sep", "\t") \
    .schema(schema) \
    .csv('/user/s2030918/click_oktober.tsv')



toClicks = tweets \
    .groupBy('to') \
    .sum('count') \
    .select(col('to').alias('url'), col('sum(count)').alias('toClicks'))

fromClicks =  tweets \
    .groupBy('from') \
    .sum('count') \
    .select(col('from').alias('url'), col('sum(count)').alias('fromClicks'))

clicks = toClicks.join(fromClicks, 'url')

clicks.withColumn('ratio', col('toClicks') / col('fromClicks'))