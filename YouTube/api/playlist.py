'''
Created on Sep 17, 2015

@author: Kun
'''
from client import *
from db.mysqldao import *

def parseVideoId(url):
    url = url[0:url.rfind("/")]
    url = url[url.rfind("/") + 1:len(url)]
    return url
    
def parsePlaylistJSON(JSONData, channelId):
    # Return a list of channel key-value pair
    playlistList = []
    if JSONData is not None and "items" in JSONData:
        for item in JSONData["items"]:
            snippet = item["snippet"]
            playlistDict = {
            "id":item["id"], "title":snippet["title"],
            "publishedat":snippet["publishedAt"],
            "description":snippet["description"],
            "channelid":channelId,
            "defaultvideoid":parseVideoId(snippet["thumbnails"]["default"]["url"]),
            'videoflag':'N'
            }
            if playlistDict['defaultvideoid'] != 'img':
                playlistList.append(playlistDict)
    return playlistList

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
        

saveAllPlaylistByChannel()
print "~~~~~~~~~~~~~~~~~"
