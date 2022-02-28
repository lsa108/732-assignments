from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import random

# add more functions as necessary
def e(x):
    iteration = 0
    sum = 0.0
    random.seed()
    while sum < 1:
        sum += random.random()
        iteration += 1
    return int (iteration)

def add(x, y):
    return x + y

def main(input, batches):
    # main logic starts here
    samples = int (input)
    rdd1 = sc.range(samples, numSlices = batches)
    rdd2 = rdd1.map(e)
    total_iteration = rdd2.reduce(add)
    print(total_iteration/samples)    

if __name__ == '__main__':
    conf = SparkConf().setAppName('euler')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    input = sys.argv[1]
    batches = sys.argv[2]
    main(input, batches)