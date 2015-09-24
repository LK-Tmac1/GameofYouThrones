#!/usr/bin/python

from helper import parseListToString, parseVIdByImageURL
import json

def parseUserActivityJSON(JSONData, uaList=[]):
    JSONData = json.loads(JSONData)
    if 'useractivity' in JSONData:
        for ua in JSONData['useractivity']:
            data = {'videoid':ua['videoid'], 'userid':ua['userid'], 'channelid':ua['channelid'], \
            'activitydate':ua['activitydate'], 'topic':ua['topic']}
            uaList.append(data)
    return uaList

def parseChannelJSON(JSONData, categoryId, channelList=[]):
    # Return a list of channel key-value pair
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
            "activityDate":"",
            "playlistFlag":'N' }
            if 'publishedAt' not in snippet:
                snippet["publishedAt"] = "null"
            channelDict["publishedat"] = snippet["publishedAt"]
            channelList.append(channelDict)
    return channelList

def parseVideoJSON(JSONData, videoList=[]):
    # Return a list of video key-value pair
    # If videoList is provided, then append new video to it
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

def parsePlaylistJSON(JSONData, channelId, playlistList=[]):
    # Return a list of channel key-value pair
    if JSONData is not None and "items" in JSONData:
        for item in JSONData["items"]:
            snippet = item["snippet"]
            playlistDict = {
            "id":item["id"], "title":snippet["title"],
            "publishedat":snippet["publishedAt"],
            "description":snippet["description"],
            "channelid":channelId,
            "videoid":parseVIdByImageURL(snippet["thumbnails"]["default"]["url"]),
            'videoflag':'N'
            }
            if playlistDict['videoid'] != 'img':
                playlistList.append(playlistDict)
    return playlistList

def parseVideoIdByActivityJSON(JSONData, VIdSet=set([])):
    # Return a set of video id in the JSON data, if such id is not in the VIdSet yet
    if JSONData is not None and "items" in JSONData:
        for item in JSONData['items']:
            if 'contentDetails' in item:
                ID = ''
                content = item["contentDetails"]
                if 'upload' in content:
                    ID = content["upload"]["videoId"]
                elif 'like' in content:
                    ID = content['like']['resourceId']['videoId']
                if ID != '' and ID not in VIdSet:
                    VIdSet.add(ID)
    return VIdSet
