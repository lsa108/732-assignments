import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re, string
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('test').getOrCreate()
assert spark.version >= '3.0' # make sure we have Spark 3.0+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

data = [[1,2],[3,4],[5,6],[7,8]]
df = spark.createDataFrame(data).toDF('col1','col2')
df.show()
point = 3
result = df.agg({'col1':'sum'}).toPandas()['sum(col1)'][0]
print(result)

