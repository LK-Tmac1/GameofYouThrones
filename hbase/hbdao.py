import happybase
from etl.mysqldao import update
from utility.constant import HB_TB_MASTER, MODE_STAT_HOURLY_ACCU, \
    MODE_STAT_HOURLY, MODE_STAT_DAILY_ACCU, MODE_STAT_DAILY, \
    HB_VIDEO_METADATA_LIST, DB_NAME, DB_TB_VIDEO
from hbase.format import formatVideoId

connection = happybase.Connection('localhost')    
connection.open()

def formatUseractivityStat(dataTupleList):
    dataDictList = {}
    for data in dataTupleList:
        dataDict = {}
        dataList = data[0].split(':')
        vid = dataList[1]
        if vid not in dataDictList:
            dataDictList[vid] = []
        dataDict['useractivity'] = dataList[0]
        dataDict['timestamp'] = dataList[2]
        if len(dataList) > 3:  # If hourly
            dataDict['timestamp'] = dataDict['timestamp'] + ':' + dataList[3]
        dataDict['value'] = str(data[1])
        dataDictList[vid].append(dataDict)
    return dataDictList

def putUseractivityStat(mode, dataDictList):
    """
    :dataDictList should be a list of dict, where on each dict:
        Key=useractivity:row_composite_key:timestmp, value=count
        e.g., {userview:channelid:videoid:2015-09-28T12:02:20Z': 200}
    :mode the mode of this user activity, say hourly basis and accumulative sum
    """
    table = connection.table(HB_TB_MASTER)
    modeColumnSuffixDict = {
        MODE_STAT_HOURLY:'_hourly', MODE_STAT_HOURLY_ACCU:'_hourly_accu',
        MODE_STAT_DAILY:'_daily', MODE_STAT_DAILY_ACCU:'_daily_accu'}
    for dataKey, dataValueList in dataDictList.items():
        for dataValue in dataValueList:
            newDataDict = {}
            columnFamily = dataValue['useractivity']
            columnQualifer = columnFamily + modeColumnSuffixDict[mode] + ':' + dataValue['timestamp']
            newDataDict[columnQualifer] = dataValue['value']
            table.put(dataKey, newDataDict)

def scanDataByRowPrefix(prefix, columnFamilyMember=[]):
    dataDictList = [{}]
    rows = connection.table(HB_TB_MASTER).scan(row_prefix=prefix, \
                columns=columnFamilyMember, sorted_columns=True)
    for r in rows:
        print r[0]
    return rows

        
# DsuxXH8Q76o
#print scanDataByRowPrefix('v_', ['userview_hourly_aggre'])
