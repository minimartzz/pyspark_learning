import pyspark.sql as spark

df = spark.read.json("log.json")
df.where("age > 21").select("name.first").show()