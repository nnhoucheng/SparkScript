from pyspark import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as sf

if __name__=='__main__':
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    rdd = sc.textFile('twitter_1k.jsonl')
    dfTweets = sqlContext.read.load('twitter_1k.jsonl', format='json')
    hashtags = dfTweets.select('entities.hashtags.text')
    hashtags = hashtags.filter(sf.size('text')>0)
    hashtags = hashtags.select(sf.explode('text').alias('hashtag'))
    hashtags = hashtags.groupBy('hashtag').count().orderBy('count', ascending=False)
    hashtags.rdd.saveAsTextFile('output8')
    #with open('tmp.txt', 'wb') as fo:
    #    fo.write('%s\n' % hashtags.collect())
