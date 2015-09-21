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
    # Return a set of video id during a activity time span
    dList = getDateRangeList(dateStr1, dateStr2)
    timestamp = "T00:00:0Z"
    resource = "activities"
    part = "contentDetails"
    Filter = "channelId=" + channelId
    vIdSet = set([])
    for i in xrange(0, len(dList) - 1):
        dateFilter = "&publishedAfter" + dList[i] + timestamp + "&publishedBefore" + dList[i + 1] + timestamp
        data = getJSONData(resource, Filter + dateFilter, part, True)
        while data is not None:
            idList = parseVideoIdByActivityJSON(data)
            vIdSet = vIdSet.union(set(idList))
            if 'nextPageToken' in data:
                nextPageToken = data["nextPageToken"]
                data = getJSONData(resource, Filter, part, True , nextPageToken)
                vIdSet = vIdSet.union(set(parseVideoIdByActivityJSON(data)))
            else:
                break
    return vIdSet

def saveVIdByChannelActivity(channelId, ALL=False):
    channel = select(DB_NAME, DB_TB_CHANNEL, ["publishedAt", "activityDate"], ['id'], [{'id':channelId}])
    if len(channel) > 0:
        dateStr = ""
        if ALL or (len(channel[0][1]) == 0):
            dateStr = channel[0][0]
        else:
            dateStr = channel[0][1]
        idSet = set(getVIdByChannelActivity(channelId, dateStr, getTimestampNow()))
        if len(idSet) > 0:
            insertQ = "insert into " + DB_NAME + "." + DB_TB_VIDEO + " (id) values "
            for i in xrange(0, len(idSet) - 1):
                insertQ = insertQ + "('" + idSet.pop() + "'),"
            insertQ = insertQ + "('" + idSet.pop() + "');"
            print insertQ
            execute_query(insertQ)
        update(DB_NAME, DB_TB_CHANNEL, ['activityDate'], ['id'], [{'id':channelId, 'activityDate':getTimestampNow()}])
        
def saveVIdByAllChannel():
    channelList = select(DB_NAME, DB_TB_CHANNEL, ["id"])
    for ID in channelList:
        saveVIdByChannelActivity(ID[0])

print saveVIdByChannelActivity('UCivQRRN7GPy0rqoRiobx9Sw')
# UCxOuw7Mt_5drSKdyjh7R07w
