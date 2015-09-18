'''
Created on Sep 17, 2015

@author: Kun
'''
from client import *
from db.mysqldao import *

def parsePlaylistJSON(JSONData, channelId):
    # Return a list of channel key-value pair
    playlistList = []
    if "items" in JSONData:
        for item in JSONData["items"]:
            playlistDict = {
            "id":item["id"], "title":item["snippet"]["title"],
            "publishedat":item["snippet"]["publishedAt"],
            "description":item["snippet"]["description"],
            "channelid":item["snippet"]["channelId"],
            "videoFlag":"N" }
            playlistList.append(playlistDict)
    return playlistList

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