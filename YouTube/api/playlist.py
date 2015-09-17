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
    Filter="channelId="+channelId
    part="id,snippet"
    resource="playlists"
    count = getDataCount(resource, Filter)

