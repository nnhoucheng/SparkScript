from pyspark import SparkContext
import sys

if __name__=='__main__':
    sc = SparkContext()
    
    if len(sys.argv) == 2:
        f = sc.textFile(sys.argv[1], use_unicode=False).cache()
        print '\n'*10
        for i,c in enumerate(f.first()):
            print "{} : {}".format(i,c)