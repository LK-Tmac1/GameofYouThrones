from hbdao import scanDataByRowPrefix
from utility.constant import HB_CHANNEL_PREFIX, HB_VIDEO_PREFIX
from random import randint

def queryVideoByChannel(channelid, topn, dateRangeList, useractivity, mode):
    columnQualiferList = [], columnQualiferAccumList = []
    columnQualifer = '%s%s' % (useractivity, mode)
    columnQualiferAccum = '%s%s_accum'
    for dateStr in dateRangeList:
        columnQualiferList.append(columnQualifer + ":" + dateStr)
        columnQualiferAccumList.append(columnQualiferAccum + ":" + dateStr)
    rows = scanDataByRowPrefix(HB_CHANNEL_PREFIX + HB_VIDEO_PREFIX , columnQualifer)
    rowsAccum = scanDataByRowPrefix(HB_CHANNEL_PREFIX + HB_VIDEO_PREFIX, columnQualiferAccum)
    rndIndex = randint(0, len(rowsAccum) - topn)
    # Return two tuples, the first one for non-accumulative basis, the second one
    # for accumulative basis of the mode
    return (parseHBaseTuple(rows[rndIndex:rndIndex + topn], False, dateRangeList),
            parseHBaseTuple(rowsAccum[rndIndex:rndIndex + topn], True, dateRangeList))
        
def parseHBaseTuple(rows, isAccum, dateRangeList):
    # Return a tuple, where each element is also a tuple, that has values 
    # corresponding to the date range list, i.e. sorted
    dataList = []
    for row in rows:
        dateValueMap = {dateStr:0 for dateStr in dateRangeList}
        for dateStr in dateRangeList:
            columnDate = row[0].split(':')[1]
            if columnDate in dateValueMap:
                dateValueMap[columnDate] = row[1]
        dateValueList = [dateValueMap[dateRangeList[0]]]
        for i in xrange(1, len(dateRangeList)):
            value = dateValueMap[dateRangeList[i]]
            if isAccum and value == 0:
                value = dateValueList[i - 1]  
            dateValueList.append(value)
        dataList.append(dateValueList)
    return tuple(dataList)
