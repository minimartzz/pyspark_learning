from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

# Create streaming DataFrame that represents text data received from stream hosted at localhost, and
# transforms the DataFrame to calculate word count

lines = spark \
	.readStream \
	.format('socket') \
	.option('host', 'localhost') \
	.option('port', 9999) \
	.load()
words = lines.select(
	explode(
		split(lines.value, " ")
	).alias('word')
)
word_counts = words.groupBy('word').count()

# Print complete every time the DataFrame is updated
query = word_counts \
	.writeStream \
	.outputMode('complete') \
	.format('console') \
	.start()

query.awaitTermination()