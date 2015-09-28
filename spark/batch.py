from pyspark import SparkContext, SparkConf
from utility.constant import TOPIC_USER_VIEW, HDFS_MASTER_DNS, HDFS_DEFAULT_PATH, FILE_TYPE
from utility.helper import parseDateString, getTimestampNow
from transform import transformActivity, transformActivityAggre
 
 

conf = SparkConf().setAppName("testBatch")
sc = SparkContext(conf=conf)

def loadDataFromHDFS(dateStr, topic):
    dateStr = str(parseDateString(dateStr))
    if dateStr == '':
        dateStr = str(parseDateString(getTimestampNow()))
    if topic == '':
        topic = TOPIC_USER_VIEW
    filePath = HDFS_MASTER_DNS + HDFS_DEFAULT_PATH + '/' + topic + '/' + dateStr + FILE_TYPE
    data = sc.textFile(filePath)
    data.filter(lambda line: line.strip(' \t\n\r') != '')
    return data
    
def videoStatBatchDaily(dateStr, topic):
    data = loadDataFromHDFS(dateStr, topic)
    dailyVideoStat = data.map(lambda line : (transformActivity(line), 1)).reduceByKey(lambda a, b : a + b).sortByKey().collect()
    dailyVideoStatAggre = transformActivityAggre(dailyVideoStat)
    
    
def videoStatBatchHourly(dateStr, topic):
    data = loadDataFromHDFS(dateStr, topic)
    hourlyVideoStat = data.map(lambda line : (transformActivity(line, hourly=True), 1)).reduceByKey(lambda a, b : a + b).sortByKey().collect()
    hourlyVideoStatAggre = transformActivityAggre(hourlyVideoStat, hourly=True)
    
def videoStatBatchMinute():
    data = ""
