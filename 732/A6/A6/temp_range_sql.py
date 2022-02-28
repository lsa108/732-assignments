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

    weather = spark.read.csv(inputs, schema=observation_schema)
    weather.createOrReplaceTempView("weather_df")
    df = spark.sql("SELECT * FROM weather_df WHERE qflag IS NULL AND (observation = 'TMAX' OR observation = 'TMIN')")
    df.createOrReplaceTempView("weather_df")
    df = spark.sql("SELECT *, IF(observation = 'TMAX', value/10, NULL) AS tmax FROM weather_df")
    df.createOrReplaceTempView("weather_df")
    df = spark.sql("SELECT *, IF(observation = 'TMIN', value/10, NULL) AS tmin FROM weather_df")
    df.createOrReplaceTempView("weather_df")
    tmax = spark.sql("SELECT station, date, tmax FROM weather_df")
    tmax.createOrReplaceTempView("tmax_df")
    tmin = spark.sql("SELECT station, date, tmin FROM weather_df")
    tmin.createOrReplaceTempView("tmin_df")

    tmax_tmin = spark.sql("SELECT tmax_df.date, tmax_df.station, tmax_df.tmax, tmin_df.tmin FROM tmax_df INNER JOIN tmin_df ON tmax_df.date = tmin_df.date AND tmax_df.station = tmin_df.station")
    tmax_tmin.createOrReplaceTempView("tmax_tmin_df")
    tmax_tmin = spark.sql("SELECT *, ROUND(tmax-tmin,1) AS range FROM tmax_tmin_df")
    tmax_tmin.createOrReplaceTempView("tmax_tmin_df")

    range_max = spark.sql("SELECT date, station, range, MAX(range) OVER(PARTITION BY date) as maxrange FROM tmax_tmin_df")
    range_max.createOrReplaceTempView("range_max_df")
    weather_final = spark.sql("SELECT date, station, range FROM range_max_df WHERE range = maxrange ORDER BY date")
    
    weather_final.write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range sql').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs,output)