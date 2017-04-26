from pyspark import SparkContext
import sys

if __name__=='__main__':
    sc = SparkContext()
    
    if len(sys.argv) == 2:
        f = sc.textFile(sys.argv[1], use_unicode=False).cache()
        with open('tmp.txt', 'wb') as fo:
            print '\n'*10
            print "{}".format(f.count())