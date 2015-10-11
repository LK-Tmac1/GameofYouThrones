from pyspark.context import SparkContext, SparkConf
from transform import transformActivity, transformHourlyToDailyKey, calculateAccuSum, parseTempKeyValueForAccu

conf = SparkConf().setAppName("masterBatch")
sc = SparkContext(conf=conf)

def loadDataFromPath(filePath):
    data = sc.textFile(filePath)
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

def sample():
    filePath = ""
    dataRDD = loadDataFromPath(filePath + 'sample100.txt')
    hourlyRDD = getHourlyRDD(dataRDD)
    dailyRDD = getDailyRDD(hourlyRDD)
    hourlyAccuRDD = getHourlyAccuSumRDD(hourlyRDD)
    dailyAccuRDD = getDailyAccuSumRDD(hourlyAccuRDD)
    print "======Saving files..."
    hourlyRDD.saveAsTextFile(filePath + "/output-hourly")
    dailyRDD.saveAsTextFile(filePath + "/output-daily")
    hourlyAccuRDD.saveAsTextFile(filePath + "/output-hourly-accu")
    dailyAccuRDD.saveAsTextFile(filePath + "/output-daily-accu")
    print "------Done"
