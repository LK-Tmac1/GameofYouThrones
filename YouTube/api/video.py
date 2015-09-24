#!/usr/bin/python

from client import getJSONData
from mysql.mysqldao import update, execute_query, select, insert
from utility.environment import DB_TB_VIDEO, DB_NAME, DB_TB_CHANNEL, DATE_OFFSET, MAX_RESULT
from utility.helper import getDateRangeList, getTimestampNow
from utility.parser import parseVideoIdByActivityJSON, parseVideoJSON

def getVideoById(vIds):
    vIdList = []
    videoList = []
    if isinstance(vIds, (set, list)):
        vIdList = list(set(vIds))
    else:
        vIdList.append(str(vIds))
    part = "id,snippet,statistics,contentDetails"
    resource = "videos"
    i = 0
    print "vIdList=", vIdList
    while i + MAX_RESULT <= len(vIdList):
        Filter = "id=" + ','.join(vIdList[i:i + MAX_RESULT])
        data = getJSONData(resource, Filter, part, True)
        if data is not None:
            parseVideoJSON(data, videoList)
        i = i + MAX_RESULT
    Filter = "id=" + ','.join(vIdList[i:])
    data = getJSONData(resource, Filter, part, True)
    if data is not None:
        parseVideoJSON(data, videoList)
    return videoList   

def saveVideoByMostPopCategory():
    categoryIdList = range(0, 50)
    for categoryId in categoryIdList:
        part = "id,snippet,statistics,contentDetails"
        resource = "videos"
        Filter = "chart=mostPopular&videoCategoryId=" + str(categoryId)
        data = getJSONData(resource, Filter, part, True)
        while data is not None:
            videolist = parseVideoJSON(data)
            insert(DB_NAME, DB_TB_VIDEO, videolist)
            if 'nextPageToken' in data:
                nextPageToken = data["nextPageToken"]
                data = getJSONData(resource, Filter, part, True , nextPageToken)
            else:
                data = None

def getVIdByChannelActivityDate(channelId, dateStr1, dateStr2):
    # Return a set of video id during a activity time span
    dList = getDateRangeList(dateStr1, dateStr2, offset=DATE_OFFSET)
    timestamp = "T00:00:0Z"
    resource = "activities"
    part = "contentDetails"
    Filter = "channelId=" + channelId
    vIdSet = set([])
    for i in xrange(0, len(dList) - 1):
        dateFilter = "&publishedAfter" + dList[i] + timestamp + "&publishedBefore" + dList[i + 1] + timestamp
        data = getJSONData(resource, Filter + dateFilter, part, True)
        while data is not None:
            parseVideoIdByActivityJSON(data, vIdSet)
            if 'nextPageToken' in data:
                nextPageToken = data["nextPageToken"]
                data = getJSONData(resource, Filter, part, True , nextPageToken)
                parseVideoIdByActivityJSON(data, vIdSet)
            else:
                break
        print "========", dList[i], "  ", dList[i + 1], " size=", len(vIdSet)
    return vIdSet

def getVIdByChannelActivity(channelId, ALL=False):
    channel = select(DB_NAME, DB_TB_CHANNEL, ["publishedAt", "activityDate"], ['id'], [{'id':channelId}])
    if len(channel) > 0:
        dateStr = ""
        if ALL or (len(channel[0][1]) == 0):
            dateStr = channel[0][0]
        else:
            dateStr = channel[0][1]
        return getVIdByChannelActivityDate(channelId, dateStr, getTimestampNow())
    return ([])

def saveVIdByChannelActivity(channelId, ALL=False):
    idSet = getVIdByChannelActivity(channelId, ALL)
    if len(idSet) > 0:
        insertQ = "insert into " + DB_NAME + "." + DB_TB_VIDEO + " (id, channelid) values "
        for i in xrange(0, len(idSet) - 1):
            insertQ = insertQ + "('" + idSet.pop() + "','" + channelId + "'),"
        insertQ = insertQ + "('" + idSet.pop() + "','" + channelId + "');"
        print insertQ
        execute_query(insertQ)
    update(DB_NAME, DB_TB_CHANNEL, ['activityDate'], ['id'], [{'id':channelId, 'activityDate':getTimestampNow()}])
        
def saveVIdByAllChannel():
    channelList = select(DB_NAME, DB_TB_CHANNEL, ["id"], ['activityDate'], [{'activityDate':''}])
    # for ID in channelList:
    if True:
        ID = channelList[0]
        print "~~~~~~~Channel ID=", ID[0]
        saveVIdByChannelActivity(ID[0])
