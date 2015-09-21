'''
Created on Sep 17, 2015

@author: Kun
'''
from client import getJSONData
from db.mysqldao import update, execute_query, select
from utility.environment import DB_TB_VIDEO, DB_NAME, DB_TB_CHANNEL
from utility.helper import getDateRangeList, getTimestampNow, parseDateString
from utility.parser import parseVideoIdByActivityJSON

def getVIdByChannelActivity(channelId, dateStr1, dateStr2):
    dList = getDateRangeList(dateStr1, dateStr2)
    timestamp = "T00:00:0Z"
    resource = "activities"
    part = "contentDetails"
    Filter = "channelId=" + channelId
    vIdList = []
    for i in xrange(0, len(dList) - 1):
        dateFilter = "&publishedAfter" + dList[i] + timestamp + "&publishedBefore" + dList[i + 1] + timestamp
        data = getJSONData(resource, Filter + dateFilter, part, True)
        while data is not None:
            vIdList = vIdList + parseVideoIdByActivityJSON(data)
            if 'nextPageToken' in data:
                nextPageToken = data["nextPageToken"]
                data = getJSONData(resource, Filter, part, True , nextPageToken)
                vIdList = vIdList + parseVideoIdByActivityJSON(data)
    return vIdList

def saveVIdByChannelActivity(channelId, ALL=False):
    dateStr1 = "activityDate"
    if ALL:
        dateStr1 = "publishedAt"
    channel = select(DB_NAME, DB_TB_CHANNEL, [dateStr1], ['id'], [{'id':'channelId'}])
    if len(channel) > 0:
        dateStr1 = parseDateString(channel[0][0])
    else:
        dateStr1 = "2005-02-14T00:00:0Z"
    idList = getVIdByChannelActivity(channelId, dateStr1, getTimestampNow())
    insertQ = "insert into " + DB_NAME + "." + DB_TB_VIDEO + " (id) values "
    for i in xrange(0, len(idList) - 1):
        insertQ = insertQ + "(" + idList[i] + "),"
    insertQ = insertQ + "(" + idList[len(idList) - 1] + ");"
    execute_query(insertQ)
    update(DB_NAME, DB_TB_CHANNEL, ['activityDate'], ['id'], [{'id':channelId, 'activityDate':getTimestampNow()}])
    
def saveVIdByAllChannel():
    channelList = select(DB_NAME, DB_TB_CHANNEL, ["id"])
    for ID in channelList:
        saveVIdByChannelActivity(ID[0])
