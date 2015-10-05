from hbdao import scanDataByRowPrefix
from utility.helper import getDateFromStart, getTimestampNow, getDateRangeList
from utility.constant import HB_CHANNEL_PREFIX, HB_VIDEO_PREFIX
from random import randint

def queryVideoByChannel(channelid, topn, daterange, useractivity, mode):
    startDate = getTimestampNow()
    endDate = str(getDateFromStart(str(startDate), int(daterange), True)) + 'T'
    columnQualiferList = [], columnQualiferAccumList = []
    dateRangeList = getDateRangeList(startDate, endDate, offset=1)
    columnQualifer = '%s%s' % (useractivity, mode)
    columnQualiferAccum = '%s%s_accum'
    for dateStr in dateRangeList:
        columnQualiferList.append(columnQualifer + ":" + dateStr)
        columnQualiferAccumList.append(columnQualiferAccum + ":" + dateStr)
    # rows = scanDataByRowPrefix(HB_CHANNEL_PREFIX +  HB_VIDEO_PREFIX , columnQualifer)
    rowsAccum = scanDataByRowPrefix(HB_CHANNEL_PREFIX + HB_VIDEO_PREFIX, columnQualiferAccum)
    rndIndex = randint(0, len(rowsAccum) - topn)
    rowsAccum = rowsAccum[rndIndex:rndIndex + topn]
    for data in rowsAccum:
        columnDate = data[0].split(':')[1]
        value = int(data[1])
        
        
