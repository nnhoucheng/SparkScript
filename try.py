from pyspark import SparkContext

if __name__=='__main__':
    sc = SparkContext()
    f = sc.textFile('/tmp/citibike.csv', use_unicode=False).cache()
    for i,c in enumerate(f.first().split(',')):
        print("{} : {}".format(i,c)) 