'''
Functions parse JSON data from YouTube client.
@author: Kun
'''
from helper import parseListToString, parseVIdByImageURL

def parseChannelJSON(JSONData, categoryId):
    # Return a list of channel key-value pair
    channelList = []
    if "items" in JSONData:
        for item in JSONData["items"]:
            snippet = item["snippet"]
            stat = item["statistics"]
            channelDict = {
            "id":item["id"], "title":snippet["title"],
            "description":snippet["description"],
            "viewcount":stat["viewCount"],
            "commentcount":stat["commentCount"],
            "subscribercount":stat["subscriberCount"],
            "videocount":stat["videoCount"],
            "categoryid":categoryId,
            "playlistFlag":'N' }
            if 'publishedAt' not in snippet:
                snippet["publishedAt"] = "null"
            channelDict["publishedat"] = snippet["publishedAt"]
            channelList.append(channelDict)
    return channelList

def parseVideoJSON(JSONData):
    # Return a list of video key-value pair
    videoList = []
    if "items" in JSONData:
        for item in JSONData["items"]:
            if 'statistics' in item:
                # Otherwise this video is unavailable
                stat = item['statistics']
                snippet = item['snippet']
                videoDict = {
                "id":item["id"], "title":snippet["title"],
                "publishedat":snippet["publishedAt"],
                "description":snippet["description"],
                "channelid":snippet["channelId"],
                "imageurl":snippet['thumbnails']['default']['url'],
                'categoryid':snippet['categoryId'],
                'viewcount':stat['viewCount'],
                'likecount':stat['likeCount'],
                'dislikecount':stat['dislikeCount'],
                'favoritecount':stat['favoriteCount'],
                'commentcount':stat['commentCount'],
                'duration':item['contentDetails']['duration'],
                'definition':item['contentDetails']['definition']}
                videoDict['tags'] = ''
                if 'tags' in snippet:
                    videoDict['tags'] = parseListToString(snippet['tags'])
                videoList.append(videoDict)
    return videoList

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
            "defaultvideoid":parseVIdByImageURL(snippet["thumbnails"]["default"]["url"]),
            'videoflag':'N'
            }
            if playlistDict['defaultvideoid'] != 'img':
                playlistList.append(playlistDict)
    return playlistList

def parseVideoIdByActivityJSON(JSONData, VIdSet=None):
    # Return a set of video id in the JSON data, if such id is not in the VIdSet yet
    if VIdSet is None:
        VIdSet = set([])
    if JSONData is not None and "items" in JSONData:
        for item in JSONData['items']:
            if 'contentDetails' in item and 'upload' in item['contentDetails']:
                ID = item["contentDetails"]["upload"]["videoId"].encode('utf-8')
                if ID not in VIdSet:
                    VIdSet.add(ID)
    return VIdSet
            
            
