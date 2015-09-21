'''
Created on Sep 17, 2015

@author: Kun
'''
from client import getJSONData
from utility.parser import parsePlaylistJSON
from utility.environment import DB_NAME, DB_TB_CHANNEL, DB_TB_PLAYLIST
from db.mysqldao import update, insert, select


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