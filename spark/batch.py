from pyspark import SparkContext, SparkConf
from utility.constant import TOPIC_USER_VIEW, HDFS_MASTER_DNS, HDFS_DEFAULT_PATH, FILE_TYPE
from utility.helper import parseDateString, getTimestampNow
from transform import transformActivity, transformActivityAccuSum
 
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
    
def useractivityBatch(dataRDD, hourly=False):
    # Sample result (userview:channelid:videoid:2015-09-28T12:02:20Z, 200)
    masterRDD = dataRDD.flatMap(lambda line : transformActivity(line, hourly))
    masterRDD = masterRDD.map(lambda line : (line, 1))
    masterStat = masterRDD.reduceByKey(lambda a, b : a + b).sortByKey().collect()
    return (masterStat, transformActivityAccuSum(masterStat, hourly))

