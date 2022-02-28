import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions as f, types

def main(inputs):
    
    messages = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092') \
    .option('subscribe', inputs).load()
    values = messages.select(messages['value'].cast('string'))
    values = values.withColumn('tmp', f.split(values['value'],' '))
    df_xy = values.select(values['tmp'].getItem(0).alias('x'),values['tmp'].getItem(1).alias('y'))
    
    # data = df_xy.withColumn('n', f.expr(count(x)) \
    # .withColumn('sum_x', f.expr(sum(x))) \
    # .withColumn('sum_y', f.expr(sum(y))) \
    # .withColumn('sum_x_square', f.expr(sum(x*x))) \
    # .withColumn('sum_xy',f.expr(sum(x*y)))

    data = df_xy.select(
    f.count('x').alias('n'), f.sum('x').alias('sum_x'), f.sum('y').alias('sum_y'), 
    (f.sum(df_xy['x']*df_xy['y'])).alias('sum_xy'), 
    f.sum(f.pow(df_xy['x'],2)).alias('sum_x_square')
    )

    results = data.withColumn('beta',((data['sum_xy'] - data['sum_x'] * data['sum_y'] / data['n']) / (data['sum_x_square'] - data['sum_x'] * data['sum_x'] / data['n'])))
    results = results.withColumn('alpha', (results['sum_y']/results['n'])-results['beta']*(results['sum_x']/results['n']))
    final = results.select('beta','alpha')
    
    stream = final.writeStream.format('console').option("truncate", "false").outputMode('complete').start()
    stream.awaitTermination(600)

if __name__ == "__main__":
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('read stream').getOrCreate()
    assert spark.version >= '2.4' # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)