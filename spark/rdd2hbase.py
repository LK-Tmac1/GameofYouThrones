from spark.batch import loadDataFromPath, getDailyAccuSumRDD, getDailyRDD, \
    getHourlyAccuSumRDD, getHourlyRDD
from utility.constant import MODE_HOURLY_ACCU, MODE_HOURLY, MODE_DAILY_ACCU, MODE_DAILY
from hbase.hbdao import putUseractivityStat


modeColumnSuffixDict = {
        MODE_HOURLY:'_hourly', MODE_HOURLY_ACCU:'_hourly_accu',
        MODE_DAILY:'_daily', MODE_DAILY_ACCU:'_daily_accu'}

def parseUseractivityRDD(mode, dataTupleList):
    # Each dataDictMap element will be a dict, where the key is the column qualifier
    # and the value is the number of activity
    dataDictMap = {}
    for dataTuple in dataTupleList:
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
        if rowkey not in dataDictMap:
            dataDictMap[rowkey] = {}
        dataDictMap[rowkey][columnQualifer] = str(dataTuple[1])
    return dataDictMap

def putToHBase(mode, dataRDD):
    dataDictMap = parseUseractivityRDD(mode, dataRDD.collect())
    putUseractivityStat(dataDictMap)

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
    
    
#filePath = '/Users/Kun/Git/GameofYouThrones/spark/sample/input.txt'
filePath = '/home/ubuntu/project/sample.txt'
putToHBaseBatch(filePath)       
