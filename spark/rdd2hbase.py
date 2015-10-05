from spark.batch import loadDataFromPath, getDailyAccuSumRDD, getDailyRDD, \
    getHourlyAccuSumRDD, getHourlyRDD
from utility.constant import MODE_HOURLY_ACCU, MODE_HOURLY, MODE_DAILY_ACCU, MODE_DAILY, \
    HDFS_MASTER_DNS, HDFS_DEFAULT_PATH
from hbase.hbdao import putUseractivityStat


modeColumnSuffixDict = {
        MODE_HOURLY:'_hourly', MODE_HOURLY_ACCU:'_hourly_accum',
        MODE_DAILY:'_daily', MODE_DAILY_ACCU:'_daily_accum'}

def parseUseractivityRDD(mode, dataTuple):
    # Each dataList element will be a dict, where the key is the column qualifier
    # and the value is the number of activity
    dataList = []
    keyList = dataTuple[0].split(':')
    useractivity = keyList[0]
    uadatetime = ''
    rowkey = keyList[1]
    if mode == MODE_DAILY or mode == MODE_DAILY_ACCU:
        uadatetime = keyList[len(keyList) - 1]
        if len(keyList) == 4:
            rowkey = rowkey + ':' + keyList[2]
    elif mode == MODE_HOURLY or mode == MODE_HOURLY_ACCU:
        uadatetime = keyList[len(keyList) - 2] + ':' + keyList[len(keyList) - 1]
        if len(keyList) == 5:
            rowkey = rowkey + ':' + keyList[2]
    columnQualifer = useractivity + modeColumnSuffixDict[mode] + ':' + uadatetime
    dataList.append(columnQualifer)
    dataList.append(str(dataTuple[1]))
    return (rowkey, tuple(dataList))

def putToHBase(mode, dataRDD):
    dataTupleList = dataRDD.map(lambda line : parseUseractivityRDD(mode, line)).groupByKey()
    putUseractivityStat(dataTupleList.collect())
    
def putToHBaseBatch(filePath):
    dataRDD = loadDataFromPath(filePath)
    hourlyRDD = getHourlyRDD(dataRDD)
    hourlyAccuRDD = getHourlyAccuSumRDD(hourlyRDD)
    dailyRDD = getDailyRDD(hourlyRDD)
    dailyAccuRDD = getDailyAccuSumRDD(hourlyAccuRDD)
    print "Saving---------"
    putToHBase(MODE_HOURLY, hourlyRDD)
    putToHBase(MODE_HOURLY_ACCU, hourlyAccuRDD)
    putToHBase(MODE_DAILY, dailyRDD)
    putToHBase(MODE_DAILY_ACCU, dailyAccuRDD)
    print "Done====="
    

# filePath = '/Users/Kun/Git/GameofYouThrones/spark/sample/input.txt'
# filePath = '/home/ubuntu/project/sample.txt'
filePath = HDFS_MASTER_DNS + HDFS_DEFAULT_PATH + '/sample.txt'
putToHBaseBatch(filePath)