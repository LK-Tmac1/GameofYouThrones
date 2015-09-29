import happybase
from utility.constant import HB_TB_VIDEO_STAT, MODE_VIDEO_STAT_HOURLY_AGGRE, \
    MODE_VIDEO_STAT_HOURLY, MODE_VIDEO_STAT_DAILY_AGGRE, MODE_VIDEO_STAT_DAILY

connection = happybase.Connection('localhost')    
connection.open()
videoStatTable = connection.table(HB_TB_VIDEO_STAT)

def parseVideoStat(dataTupleList):
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

def putVideoStat(mode, dataDictList, table=''):
    table = connection.table(HB_TB_VIDEO_STAT if table == '' else table)
    for dataKey, dataValueList in dataDictList.items():
        for dataValue in dataValueList:
            newDataDict = {}
            columnFamily = dataValue['useractivity']
            if mode == MODE_VIDEO_STAT_HOURLY:
                columnFamily = columnFamily + '_hourly'
            elif mode == MODE_VIDEO_STAT_DAILY:
                columnFamily = columnFamily + '_daily'
            elif mode == MODE_VIDEO_STAT_DAILY_AGGRE:
                columnFamily = columnFamily + '_daily_aggre'
            elif mode == MODE_VIDEO_STAT_HOURLY_AGGRE:
                columnFamily = columnFamily + '_hourly_aggre'
            columnMember = columnFamily + ':' + dataValue['timestamp']
            newDataDict[columnMember] = dataValue['value']
            table.put(dataKey, newDataDict)

def getVideoStatByRowKey(rowKey, columnFamilyMember=[]):
    dataDict = {}
    row = videoStatTable.row(rowKey)
    if isinstance(columnFamilyMember, basestring):
        columnFamilyMember = [columnFamilyMember]
    if len(columnFamilyMember) > 0:
        for cfm in columnFamilyMember:
            dataDict[cfm] = row[cfm]
    else:
        dataDict = row
    return dataDict

print getVideoStatByRowKey('v_123', 'userlike_hourly_aggre:2015-09-27T17:00')
