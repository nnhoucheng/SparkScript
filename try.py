from pyspark import SparkContext

if __name__=='__main__':
    sc = SparkContext()
    f = sc.textFile('/tmp/citibike.csv', use_unicode=False).cache()
    with open('tmp.txt', 'wb') as fo:
        print '\n'*10
        print "{}".format(f.count())