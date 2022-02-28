from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary
def add_pairs(kv1, kv2):
    return (kv1[0]+kv2[0], kv1[1]+kv2[1])

def get_subreddit_score(line):
    subreddit = json.loads(line).get("subreddit")
    score = int (json.loads(line).get("score"))
    return(subreddit,(1, score))

def average_score(kv):
    k,v = kv
    return json.dumps((k,v[1]/v[0]))

def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    subreddit_scores = text.map(get_subreddit_score)
    counts = subreddit_scores.reduceByKey(add_pairs)
    outdata = counts.map(average_score)
    outdata.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('Reddit average score')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)