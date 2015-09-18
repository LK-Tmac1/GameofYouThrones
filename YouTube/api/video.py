'''
Created on Sep 17, 2015

@author: Kun
'''
from client import *
from db.mysqldao import *

def parseVideoJSON(JSONData):
    # Return a list of video key-value pair
    videoList = []
    if "items" in JSONData:
        for item in JSONData["items"]:
            snippet=item['snippet']
            stat=item['statistics']
            videoDict = {
            "id":item["id"], "title":snippet["title"],
            "publishedat":snippet["publishedAt"],
            "description":snippet["description"],
            "channelid":snippet["channelId"],
            "imageurl":snippet['thumbnails']['default'],
            "tags":snippet['tags'],
            'categoryId':snippet['categoryId'],
            'viewcount':stat['viewCount'],
            'likecount':stat['likeCount'],
            'dislikecount':stat['dislikeCount'],
            'favoritecount':stat['favoriteCount'],
            'commentcount':stat['commentCount']
            }
            videoList.append(videoDict)
    return videoList

def savePlaylistByChannel(channelId):
    Filter = "channelId=" + channelId
    part = "id,snippet"
    resource = "playlists"
    count = getDataCount(resource, Filter)
    print "----------------", count, "playlist data of channel", channelId
    if count > 0:
        data = getJSONData(resource, Filter, part, True)
        playlistList = parsePlaylistJSON(data, channelId)
        insert(DB_NAME, DB_TB_PLAYLIST, playlistList)
        while count > MAX_RESULT:
            nextPageToken = data["nextPageToken"]
            data = getJSONData(resource, Filter, part, True , nextPageToken)
            channelList = parsePlaylistJSON(data, channelId)
            insert(DB_NAME, DB_TB_PLAYLIST, channelList)
            count = count - MAX_RESULT
    update(DB_NAME, DB_TB_CHANNEL, ['playlistFlag'], ['id'], [{'playlistFlag':'Y', 'id':channelId}])

def saveAllPlaylistByChannel():
    idList = select(DB_NAME, DB_TB_CHANNEL, ["id"], ['playlistFlag'], [{'playlistFlag':'N'}])
    for ID in idList:
        savePlaylistByChannel(ID[0])


saveAllPlaylistByChannel()
print "~~~~~~~~~~~~~~~~~"