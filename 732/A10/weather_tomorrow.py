import sys
import datetime
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather tomorrow').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator

tmax_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.DateType()),
        types.StructField('latitude', types.FloatType()),
        types.StructField('longitude', types.FloatType()),
        types.StructField('elevation', types.FloatType()),
        types.StructField('tmax', types.FloatType()),
    ])

def main(model_file):
    
    # load the model
    model = PipelineModel.load(model_file)
    
    data = [
        ('sfu', datetime.date(year=2020, month=11, day=19), 49.2771, -122.9146, 330.0, 12.0),
        ('sfu', datetime.date(year=2020, month=11, day=20), 49.2771, -122.9146, 330.0, 12.0)
        ]
    data_df = spark.createDataFrame(data, schema=tmax_schema)
    
    # use the model to make predictions
    predictions = model.transform(data_df)
    # predictions.show()
    prediction = predictions.collect()[0][7]
    print('Predicted tmax tomorrow', prediction)


if __name__ == '__main__':
    model_file = sys.argv[1]
    main(model_file)
