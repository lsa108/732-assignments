from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def get_subreddit_score_author(line):
    subreddit = json.loads(line).get("subreddit")
    score = json.loads(line).get("score")
    author = json.loads(line).get("author")
    return(subreddit, int(score), author)

def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    subreddit_scores_author = text.map(get_subreddit_score_author).filter(lambda x: "e" in x[0]).cache()
    result_positive = subreddit_scores_author.filter(lambda x: x[1] > 0)
    result_negative = subreddit_scores_author.filter(lambda x: x[1] <= 0)
    outdata_positive = result_positive.map(json.dumps)
    outdata_negative = result_negative.map(json.dumps)
    
    outdata_positive.saveAsTextFile(output + '/positive')
    outdata_negative.saveAsTextFile(output + '/negative')


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit etl')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)