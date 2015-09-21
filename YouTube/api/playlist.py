'''
Created on Sep 17, 2015

@author: Kun
'''
from client import getJSONData
from utility.parser import parsePlaylistJSON, parseVideoJSON
from utility.environment import DB_NAME, DB_TB_CHANNEL, DB_TB_PLAYLIST, DB_TB_VIDEO, MAX_RESULT
from db.mysqldao import update, insert, select, execute_query


def savePlaylistByChannel(channelId):
    Filter = "channelId=" + channelId
    part = "id,snippet"
    resource = "playlists"
    data = getJSONData(resource, Filter, part, True)
    while data is not None:
        playlistList = parsePlaylistJSON(data, channelId)
        insert(DB_NAME, DB_TB_PLAYLIST, playlistList)
        if 'nextPageToken' in data:
            nextPageToken = data["nextPageToken"]
            data = getJSONData(resource, Filter, part, True , nextPageToken)
        else:
            break
    update(DB_NAME, DB_TB_CHANNEL, ['playlistFlag'], ['id'], [{'playlistFlag':'Y', 'id':channelId}])

def saveAllPlaylistByChannel():
    idList = select(DB_NAME, DB_TB_CHANNEL, ["id"], ['playlistFlag'], [{'playlistFlag':'N'}])
    for ID in idList:
        savePlaylistByChannel(ID[0])
        

def saveVideoByMostPopCategory():
    categoryIdList = range(0, 50)
    for categoryId in categoryIdList:
        part = "id,snippet,statistics,contentDetails"
        resource = "videos"
        Filter = "chart=mostPopular&videoCategoryId=" + str(categoryId)
        data = getJSONData(resource, Filter, part, True)
        while data is not None:
            videolist = parseVideoJSON(data)
            print videolist
            insert(DB_NAME, DB_TB_VIDEO, videolist)
            if 'nextPageToken' in data:
                nextPageToken = data["nextPageToken"]
                data = getJSONData(resource, Filter, part, True , nextPageToken)
            else:
                data = None
      
def getVideoByIdList(videoIdList):
    part = "id,snippet,statistics,contentDetails"
    resource = "videos"
    if len(videoIdList) > 50:
        videoIdList = videoIdList[0:50]
    Filter = "id=" + ','.join(videoIdList)
    data = getJSONData(resource, Filter, part, True)
    if data is not None:
        return parseVideoJSON(data)
    
def saveVideoByPlaylistDefault():
    idList = execute_query("select id, defaultvideoid from " + 
                           DB_NAME + "." + DB_TB_PLAYLIST + 
                           " where videoflag = 'N'")
    start = 0
    count = 0
    while start < len(idList):
        playlistIdList = []
        videoIdList = []
        videolist = []
        for i in xrange(0, 20):
            for j in xrange(start, start + MAX_RESULT):
                videoIdList.append(idList[j][1])
                playlistIdList.append({'videoflag':'Y', 'id':idList[j][0]})
                count = count + 1
                if count > len(idList):
                    break
            videolist = videolist + getVideoByIdList(videoIdList)
        insert(DB_NAME, DB_TB_VIDEO, videolist)
        update(DB_NAME, DB_TB_PLAYLIST, ['videoflag'], ['id'], playlistIdList)
        print "----------start=", start, "total=", len(idList)
