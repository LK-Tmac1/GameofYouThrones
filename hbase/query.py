from hbdao import scanDataByRowPrefix
from utility.helper import getDateFromStart, getTimestampNow, getDateRangeList
from utility.constant import HB_CHANNEL_PREFIX
from random import randint

def queryVideoByChannel(channelid, topn, daterange, useractivity, mode):
    startDate = getTimestampNow()
    endDate = str(getDateFromStart(str(startDate), int(daterange), True)) + 'T'
    columnQualifer = []
    dateRangeList = getDateRangeList(startDate, endDate, offset=1)
    for dateStr in dateRangeList:
        columnQualifer.append('%s%s:%s' % (useractivity, mode, dateStr))
        columnQualifer.append('%s%s_accum:%s' % (useractivity, mode, dateStr))
    rows = scanDataByRowPrefix(HB_CHANNEL_PREFIX, columnQualifer)
    count = len(rows)
    rndRow = rows[randint(0, count - 1)]
    for data in rndRow[0]:
        