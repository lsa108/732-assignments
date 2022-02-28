import sys, math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import lit

def main(keyspace, table):

    df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).load()
    df = df.withColumn("count",lit(1))
    
    df_xy = df.groupBy('host').sum('bytes','count').withColumnRenamed('sum(bytes)','y').withColumnRenamed('sum(count)','x')
    df_xy = df_xy.withColumn('n',lit(1)).withColumn('x_square',df_xy['x']**2).withColumn('y_square',df_xy['y']**2).withColumn('xy',df_xy['x']*df_xy['y'])
    
    n = df_xy.agg({'n':'sum'}).toPandas()['sum(n)'][0]
    sum_x = df_xy.agg({'x':'sum'}).toPandas()['sum(x)'][0]
    sum_y = df_xy.agg({'y':'sum'}).toPandas()['sum(y)'][0]
    sum_xy = df_xy.agg({'xy':'sum'}).toPandas()['sum(xy)'][0]
    sum_xsquare = df_xy.agg({'x_square':'sum'}).toPandas()['sum(x_square)'][0]
    sum_ysquare = df_xy.agg({'y_square':'sum'}).toPandas()['sum(y_square)'][0]

    r = (n*sum_xy - sum_x*sum_y)/(math.sqrt(n*sum_xsquare - sum_x**2) * math.sqrt(n*sum_ysquare - sum_y**2))
    
    print(r, r**2)

if __name__ == '__main__':
    keyspace = sys.argv[1]
    table = sys.argv[2]
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('correlate logs cassandra') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(keyspace, table)