import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col

# add more functions as necessary

def main(inputs,output):
    # main logic starts here
    observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()),
])

    weather = spark.read.csv(inputs, schema=observation_schema).cache()
    
    qflag_null = weather.qflag.isNull()
    tmax = (weather.observation == 'TMAX')
    tmin = (weather.observation == 'TMIN')
    
    weather_tmax = weather.filter(qflag_null).filter(tmax).select('station','date',col('value').alias('tmax'))
    weather_tmin = weather.filter(qflag_null).filter(tmin).select('station','date',col('value').alias('tmin'))

   # tmax_bcast = functions.broadcast(weather_tmax)
   # tmin_bcast = functions.broadcast(weather_tmin)
    
    tmax_tmin = weather_tmax.join(weather_tmin,['station','date'],'inner')
    tmax_tmin = tmax_tmin.withColumn('tmax',tmax_tmin.tmax/10)
    tmax_tmin = tmax_tmin.withColumn('tmin',tmax_tmin.tmin/10)
    tmax_tmin = tmax_tmin.withColumn('range', functions.round((tmax_tmin.tmax - tmax_tmin.tmin),1)).cache()

    max_range = tmax_tmin.groupBy('date').max('range')
    weather_final = tmax_tmin.join(max_range,on = ['date']).filter(tmax_tmin['range'] == max_range['max(range)'])
    weather_final = weather_final.sort('date','station').select('date','station','range')

    weather_final.write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs,output)