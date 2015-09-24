from pyspark import SparkContext, SparkConf
import sys, os
# sys.path.append(os.getcwd())

print "Testing"
conf = SparkConf().setAppName("testBatch")
sc = SparkContext(conf=conf)
data = sc.textFile("/home/ubuntu/project/GameofYouThrones/samplefiles/videos.json")
res = data.collect()

for line in res:
    print line


def videoStatDaily():
    print ""
    
def userActivity():
    print ""