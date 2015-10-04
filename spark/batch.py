from pyspark.context import SparkContext, SparkConf
from utility.constant import HDFS_MASTER_DNS, HDFS_DEFAULT_PATH, FILE_TYPE
from utility.helper import parseDateString, getTimestampNow
from transform import transformActivity, transformHourlyToDailyKey, \
        calculateAccuSum, parseTempKeyValueForAccu
 
conf = SparkConf().setAppName("testBatch")
sc = SparkContext(conf=conf)

def loadDataFromHDFS(dateStr, filePath=None):
    dateStr = str(parseDateString(dateStr))
    if dateStr == '':
        dateStr = str(parseDateString(getTimestampNow()))
    if filePath is None:
        filePath = HDFS_MASTER_DNS + HDFS_DEFAULT_PATH + '/' + dateStr + FILE_TYPE
    data = sc.textFile(filePath)
    for line in data.collect():
        if len(line.strip(' \t\n\r')) > 0:
            print len(line)
    data.filter(lambda line: len(line.strip(' \t\n\r')) > 0)
    return data
    
def getHourlyRDD(dataRDD):
    # Sample result (userview:channelid:videoid:2015-09-28T12:02:20Z, 200)
    hourlyRDD = dataRDD.flatMap(lambda line : transformActivity(line, True))
    hourlyRDD = hourlyRDD.map(lambda line : (line, 1))
    hourlyRDD = hourlyRDD.reduceByKey(lambda a, b : a + b)
    return hourlyRDD

def getDailyRDD(hourlyRDD):
    # Calculate daily stat based on hourly
    dailyRDD = hourlyRDD.map(lambda (K, V) : transformHourlyToDailyKey(K, V))
    dailyRDD = dailyRDD.reduceByKey(lambda a, b : a + b)
    return dailyRDD

def getHourlyAccuSumRDD(hourlyRDD):
    # Calculate hourly accum-sum based on hourly
    hourlyAccuRDD = hourlyRDD.sortByKey().map(lambda (K, V): \
        (parseTempKeyValueForAccu(K, V, True))).groupByKey()
    hourlyAccuRDD = hourlyAccuRDD.flatMap(lambda x:calculateAccuSum(x[0], x[1]))
    return hourlyAccuRDD

def getDailyAccuSumRDD(hourlyAccuRDD):
    # Calculate daily accum-sum based on hourly accum-sum
    dailyAccuRDD = hourlyAccuRDD.sortByKey().map(lambda (K, V) : transformHourlyToDailyKey(K, V))
    dailyAccuRDD = dailyAccuRDD.reduceByKey(lambda a, b : a + b).sortByKey()
    dailyAccuRDD = dailyAccuRDD.map(lambda (K, V): (parseTempKeyValueForAccu(K, V, False))).groupByKey()
    dailyAccuRDD = dailyAccuRDD.flatMap(lambda x:calculateAccuSum(x[0], x[1]))
    return dailyAccuRDD

dataRDD = loadDataFromHDFS('2015-09-27', '../../data/2015-09-28.txt')
print dataRDD.first()
"""
dataRDD = loadDataFromHDFS('2015-09-27', 'sample_user_activity2.txt')
hourlyRDD = getHourlyRDD(dataRDD)
dailyRDD = getDailyRDD(hourlyRDD)
hourlyAccuRDD = getHourlyAccuSumRDD(hourlyRDD)
dailyAccuRDD = getDailyAccuSumRDD(hourlyAccuRDD)
hourlyRDD.saveAsTextFile("output-hourly.txt")
dailyRDD.saveAsTextFile("output-daily.txt")
hourlyAccuRDD.saveAsTextFile("output-hourly-accu.txt")
dailyAccuRDD.saveAsTextFile("output-daily-accu.txt")

print "------"
"""