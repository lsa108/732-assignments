import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re, string
from pyspark.sql import SparkSession, functions, types

# add more functions as necessary
def get_node_pair(line):
    re_split = re.compile(r':\s?')
    ls = re_split.split(line)
    result = []
    if len(ls) > 1:
        for i in ls[1].split(' '):
            if i != '':
                result.append((ls[0],i))
    return result    

def main(inputs, output, start, end):
    # main logic starts here
    nodes_schema = types.StructType([
        types.StructField('node', types.StringType()),
        types.StructField('neighbours', types.StringType()),
    ])

    path_schema = types.StructType([
        types.StructField('node', types.StringType()),
        types.StructField('source', types.StringType()),
        types.StructField('distance', types.IntegerType()),
    ])

    nodes = sc.textFile(inputs)
    nodes1 = nodes.map(get_node_pair).filter(lambda x: x is not None).flatMap(lambda x: x).cache()
    df_node = nodes1.toDF(nodes_schema)
    
    initial_path = (start, '0', 0)
    df_knownpath = spark.createDataFrame(data=[initial_path],schema=path_schema).cache()
    path = [df_knownpath]
    df_final = df_knownpath

    for i in range(6):
        path_i = path[i]
        new_path = path_i.join(df_node, df_node['node'] == path_i['node'], 'inner').select(df_node['node'],'neighbours','distance')
        new_path = new_path.withColumnRenamed('node','source').withColumnRenamed('neighbours','node').withColumn('distance',new_path['distance']+1).cache()
        new_path.coalesce(1).write.json(output + '/iter-' + str(i),mode='overwrite')
        path.append(new_path)
        df_final = df_final.unionByName(new_path).cache()
        if end in new_path.select('node').collect()[0]:
            break
    df_final1 = df_final.groupBy('node').min('distance')
    df_final2 = df_final.distinct()
    df_final3 = df_final2.join(df_final1,(df_final1['node'] == df_final2['node']) & (df_final1['min(distance)'] == df_final2['distance']),'leftsemi')
    
    df_final3.cache()
    
    if df_final3.where(df_final3['node'] == end).count() == 0:
        print("{} cannot reach {}".format(start,end))
    
    else:
        final_path = [end]
        i = 0
        while True:
            point = final_path[i]
            source_i = df_final3.where(df_final3['node'] == point).toPandas()['source'][0]
            final_path.append(source_i)
            i += 1
            if start == source_i:
                break
        
        final_path.reverse()
        sc.parallelize(final_path).coalesce(1).saveAsTextFile(output + '/path')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    start = sys.argv[3]
    end = sys.argv[4]
    spark = SparkSession.builder.appName('shortest path').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output, start, end)