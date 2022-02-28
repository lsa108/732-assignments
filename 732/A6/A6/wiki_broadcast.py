import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

wiki_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('views', types.LongType()),
    types.StructField('size', types.LongType()),
])

@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    filename = path.split("/")[-1]
    return filename[11:22]


def main(inputs,output):
    # main logic starts here
    wiki_data = spark.read.csv(inputs, sep=" ", schema=wiki_schema).withColumn('filename', functions.input_file_name())
    wiki_data = wiki_data.withColumn('hour', path_to_hour(wiki_data.filename)).cache()
    wiki_filter = wiki_data.filter((wiki_data.language == 'en') & (~ wiki_data.title.startswith('Special:')) & (wiki_data.title != 'Main_Page'))
    max_view = wiki_filter.groupBy('hour').max('views')
    max_view_bcast = functions.broadcast(max_view)
    wiki_popular = wiki_filter.join(max_view_bcast,(wiki_filter['hour'] == max_view_bcast['hour']) & (wiki_filter['views'] == max_view_bcast['max(views)']),"leftsemi")
    wiki_popular = wiki_popular.select('hour','title','views').sort('hour')
    #wiki_popular.explain()
    wiki_popular.coalesce(1).write.json(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('wiki broadcast').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs,output)