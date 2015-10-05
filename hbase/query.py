from hbdao import scanDataByRowPrefix
from utility.helper import getDateFromStart, getTimestampNow, getDateRangeList
from utility.constant import HB_CHANNEL_PREFIX
from random import randint

def queryVideoByChannel(channelid, topn, daterange, useractivity, mode):
    startDate = getTimestampNow()
    endDate = str(getDateFromStart(str(startDate), int(daterange), True)) + 'T'
    columnQualiferList = []
    dateRangeList = getDateRangeList(startDate, endDate, offset=1)
    columnQualifer = '%s%s' % (useractivity, mode)
    # columnQualiferAccum = '%s%s_accum'
    for dateStr in dateRangeList:
        columnQualiferList.append(columnQualifer + ":" + dateStr)
        # columnQualiferList.append(columnQualiferAccum + ":" + dateStr)
    rows = scanDataByRowPrefix(HB_CHANNEL_PREFIX, columnQualifer)
    count = len(rows)
    rndRow = ['channel_channelid']  # No ':video_videoid'
    while rndRow[0].find(':') < 0:
        rndRow = rows[randint(0, count - 1)]
    for data in rndRow[1]:
        column = data[0].split(':')[1]
        value = int(data[1])
        
        
