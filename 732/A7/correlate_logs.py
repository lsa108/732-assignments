import sys
import re
import math
assert sys.version_info >= (3, 5) 
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import lit

# add more functions as necessary
def disassemble_lines(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    match = re.search(line_re,line)
    if match:
        host = match.group(1)
        bytes_transfer = float(match.group(4))
        return host,bytes_transfer
    return None

def main(inputs):
    # main logic starts here
    
    logs_schema = types.StructType([
    types.StructField('host', types.StringType()),
    types.StructField('bytes_transfer', types.FloatType()),
])
    
    text = sc.textFile(inputs).cache()
    text = text.map(lambda line:disassemble_lines(line)).filter(lambda x: x is not None)

    df = text.toDF(logs_schema)
    df = df.withColumn("count",lit(1))
    df_xy = df.groupBy('host').sum('bytes_transfer','count').withColumnRenamed('sum(bytes_transfer)','y').withColumnRenamed('sum(count)','x')
    df_xy = df_xy.withColumn('n',lit(1)).withColumn('x_square',df_xy['x']**2).withColumn('y_square',df_xy['y']**2).withColumn('xy',df_xy['x']*df_xy['y'])
   # df_xy.show(5)

    n = df_xy.agg({'n':'sum'}).toPandas()['sum(n)'][0]
    sum_x = df_xy.agg({'x':'sum'}).toPandas()['sum(x)'][0]
    sum_y = df_xy.agg({'y':'sum'}).toPandas()['sum(y)'][0]
    sum_xy = df_xy.agg({'xy':'sum'}).toPandas()['sum(xy)'][0]
    sum_xsquare = df_xy.agg({'x_square':'sum'}).toPandas()['sum(x_square)'][0]
    sum_ysquare = df_xy.agg({'y_square':'sum'}).toPandas()['sum(y_square)'][0]
    
   # print(n,sum_x,sum_y,sum_xy,sum_xsquare,sum_ysquare)

    r = (n*sum_xy - sum_x*sum_y)/(math.sqrt(n*sum_xsquare - sum_x**2) * math.sqrt(n*sum_ysquare - sum_y**2))
    
    print(r, r**2)

if __name__ == '__main__':
    inputs = sys.argv[1]
    conf = SparkConf().setAppName('correlate logs')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    spark = SparkSession.builder.appName('correlate logs').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    main(inputs)