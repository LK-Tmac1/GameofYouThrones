from pyspark.context import SparkContext, SparkConf
from utility.constant import HDFS_MASTER_DNS, HDFS_DEFAULT_PATH, FILE_TYPE
from utility.helper import parseDateString, getTimestampNow
from transform import transformActivity, transformActivityAccuSum
 
conf = SparkConf().setAppName("testBatch")
sc = SparkContext(conf=conf)

def loadDataFromHDFS(dateStr, filePath=None):
    dateStr = str(parseDateString(dateStr))
    if dateStr == '':
        dateStr = str(parseDateString(getTimestampNow()))
    if filePath is None:
        filePath = HDFS_MASTER_DNS + HDFS_DEFAULT_PATH + '/' + dateStr + FILE_TYPE
    data = sc.textFile(filePath)
    data.filter(lambda line: line.strip(' \t\n\r') != '')
    return data
    
def useractivityBatch(dataRDD, hourly=False):
    # Sample result (userview:channelid:videoid:2015-09-28T12:02:20Z, 200)
    masterRDD = dataRDD.flatMap(lambda line : transformActivity(line, hourly))
    masterRDD = masterRDD.map(lambda line : (line, 1))
    masterStat = masterRDD.reduceByKey(lambda a, b : a + b).collect()  # .sortByKey().collect()
    for item in masterStat:
        print item

dataRDD = loadDataFromHDFS('2015-09-27', '~/Desktop/Sample_user_activity.txt')
useractivityBatch(dataRDD)
