import sys, re, uuid
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from datetime import datetime
from pyspark.sql import SparkSession, functions, types

# add more functions as necessary
def disassemble_lines(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    match = re.search(line_re,line)
    if match:
        host = match.group(1)
        datetimes = datetime.strptime(match.group(2),'%d/%b/%Y:%H:%M:%S')
        path = match.group(3)
        bytes_transfer = int(match.group(4))
        return host,datetimes,path,bytes_transfer
    return None

@functions.udf(returnType=types.StringType())
def uuid_generator():
    return str(uuid.uuid4())

def main(input_dir, keyspace, table):
    # main logic starts here
    logs_schema = types.StructType([
    types.StructField('host', types.StringType()),
    types.StructField('datetime', types.TimestampType()),
    types.StructField('path', types.StringType()),
    types.StructField('bytes', types.IntegerType()),

    ])
    
    data = sc.textFile(input_dir)
    data = data.repartition(16)
    data = data.map(lambda line: disassemble_lines(line)).filter(lambda x: x is not None).cache()
    data_df = data.toDF(logs_schema).withColumn('id', uuid_generator())
    data_df.write.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).mode("append").save()



if __name__ == '__main__':
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('Spark Cassandra load logs') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_dir, keyspace, table)