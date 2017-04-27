from pyspark import SparkContext
import sys

if __name__=='__main__':
    sc = SparkContext()
    
    if len(sys.argv) == 2:
        f = sc.textFile(sys.argv[1], use_unicode=False).cache()
        cs = f.first().split(',')
        print '\n'*10
        for i,c in enumerate(cs):
            print "{} : {}".format(i,c)
        print '\n'*10