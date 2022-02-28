from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def get_lines(lines):
    result = json.loads(lines)
    return result

def add_pairs(kv1, kv2):
    return (kv1[0]+kv2[0], kv1[1]+kv2[1])

def average_score(kv):
    k,v = kv
    return (k,v[1]/v[0])

def relative_score_author(kv):
    k,v = kv
    return ((int (v[1]['score']))/v[0], v[1]['author'])

def get_key(kv):
    return kv[0]

def main(inputs, output):
    
    text = sc.textFile(inputs)
    commentdata = text.map(get_lines).cache()
    
    # to calculate average score
    data_subreddit_score = commentdata.map(lambda c: (c['subreddit'], (1, int (c['score']))))
    count = data_subreddit_score.reduceByKey(add_pairs)
    average = count.map(average_score)
    average_filter = average.filter(lambda x: x[1] > 0)
    
    # to get the relative score and author pair
    commentbysub = commentdata.map(lambda c: (c['subreddit'], c))
    join_average = average_filter.join(commentbysub)
    result = join_average.map(relative_score_author)

    outdata = result.sortBy(get_key, ascending=False)
    outdata.map(json.dumps).saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('relative score')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)