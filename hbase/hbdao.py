import happybase
from integrate.mysqldao import update
from utility.constant import HB_TB_VIDEO_STAT, MODE_VIDEO_STAT_HOURLY_AGGRE, \
    MODE_VIDEO_STAT_HOURLY, MODE_VIDEO_STAT_DAILY_AGGRE, MODE_VIDEO_STAT_DAILY, \
    HB_VIDEO_METADATA_LIST, DB_NAME, DB_TB_VIDEO

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

def putVideoMetadata(videoTupleList):
    column_family = 'metadata:'
    for videoTuple in videoTupleList:
        columnData = {}
        videoid = videoTuple[0]
        for i in xrange(1, len(HB_VIDEO_METADATA_LIST)):
            column_member = column_family + HB_VIDEO_METADATA_LIST[i]
            columnData[column_member] = '' if videoTuple[i] is None else videoTuple[i].encode('utf-8')  # , 'ignore').decode('ascii')
        print videoid, columnData[column_family + HB_VIDEO_METADATA_LIST[1]]
        videoStatTable.put(videoid, columnData)
        update(DB_NAME, DB_TB_VIDEO, ['metadata'], ['id'], [{'id':videoid, 'metadata':'Y'}])
        

def putVideoStat(mode, dataDictList, table=''):
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
            videoStatTable.put(dataKey, newDataDict)

def getVideoByRowKey(rowKey, columnFamilyMember=[]):
    dataDict = {}
    row = videoStatTable.row(rowKey)
    columnFamilyMember = [columnFamilyMember]
    if len(columnFamilyMember) > 0:
        for cfm in columnFamilyMember:
            dataDict[cfm] = row[cfm]
    else:
        dataDict = row
    return dataDict

print getVideoByRowKey('DsuxXH8Q76o', 'metadata:title')
