from pyspark import SparkContext, SparkConf
from utility.constant import TOPIC_USER_VIEW, HDFS_MASTER_DNS, HDFS_DEFAULT_PATH, FILE_TYPE
from utility.helper import parseDateString, getTimestampNow
from utility.parser import parseActivityDaily, parseActivityMinute

 
conf = SparkConf().setAppName("testBatch")
sc = SparkContext(conf=conf)

def loadDataFromHDFS(dateStr, topic):
    dateStr = str(parseDateString(dateStr))
    if dateStr == '':
        dateStr = str(parseDateString(getTimestampNow()))
    filePath = HDFS_MASTER_DNS + HDFS_DEFAULT_PATH + '/' + topic + '/' + dateStr + FILE_TYPE
    return sc.textFile(filePath)
    
def videoStatDaily(dateStr, topic):
    data = loadDataFromHDFS(dateStr, topic)
    aggreVideoStat = data.map(lambda line : parseActivityDaily(line)).countByValue().items()

def videoStatMinute(dateStr, topic):
    data = loadDataFromHDFS(dateStr, topic)
    timeMinuteStat = data.map(lambda line : parseActivityMinute(line)).countByValue().items()
    
videoStatDaily()
print "Done"
