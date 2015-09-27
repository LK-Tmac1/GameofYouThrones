#!/usr/bin/python

from utility.helper import transformListToString, parseVIdByImageURL
import ast, json
from utility.helper import parseDateString, parseDateTimeMinute

def parseLineToDict(line):
    if not isinstance(line, dict):
        line = ast.literal_eval(str(line))
    return line

def parseActivityAggre(line):
    activity = ast.literal_eval(str(line))
    print "activity: ", activity
    dateKey = str(parseDateString(activity['activitydate']))
    return dateKey + ":" + activity['videoid'] + ":" + \
         activity['channelid'] + ":" + activity['topic']

def parseActivityMinute(line):
    activity = parseLineToDict(line)
    timestamp = parseDateTimeMinute(activity['activitydate'])
    return timestamp + ":" + activity['videoid'] + ':' + activity['topic']

def parseUserActivityJSON(JSONData):
    print "~~~~~~~~", JSONData
    JSONData = ast.literal_eval(str(JSONData))
    uaList = []
    if 'useractivity' in JSONData:
        for ua in JSONData['useractivity']:
            uaList.append(str(ua))
    return '\n'.join(uaList)

# JSONData = "{'useractivity': [{'topic': 'userview', 'channelid': 'c_8067', 'userid': 'u_1714371', 'videoid': 'v_51796', 'activitydate': '2015-09-24T22:53:15Z'}, {'topic': 'userview', 'channelid': 'c_9149', 'userid': 'u_7875945', 'videoid': 'v_16292', 'activitydate': '2015-09-24T22:53:15Z'}, {'topic': 'userview', 'channelid': 'c_9921', 'userid': 'u_8813132', 'videoid': 'v_45140', 'activitydate': '2015-09-24T22:53:15Z'}, {'topic': 'userview', 'channelid': 'c_3919', 'userid': 'u_3875289', 'videoid': 'v_18414', 'activitydate': '2015-09-24T22:53:15Z'}, {'topic': 'userview', 'channelid': 'c_7475', 'userid': 'u_7809244', 'videoid': 'v_76435', 'activitydate': '2015-09-24T22:53:15Z'}, {'topic': 'userview', 'channelid': 'c_8711', 'userid': 'u_4706076', 'videoid': 'v_66865', 'activitydate': '2015-09-24T22:53:15Z'}, {'topic': 'userview', 'channelid': 'c_1644', 'userid': 'u_3121991', 'videoid': 'v_11385', 'activitydate': '2015-09-24T22:53:15Z'}, {'topic': 'userview', 'channelid': 'c_5402', 'userid': 'u_826376', 'videoid': 'v_55557', 'activitydate': '2015-09-24T22:53:15Z'}, {'topic': 'userview', 'channelid': 'c_5624', 'userid': 'u_4566523', 'videoid': 'v_55522', 'activitydate': '2015-09-24T22:53:15Z'}, {'topic': 'userview', 'channelid': 'c_4721', 'userid': 'u_6786679', 'videoid': 'v_11758', 'activitydate': '2015-09-24T22:53:15Z'}]}"
# JSONData = "{'useractivity': [{'topic': 'userview', 'channelid': 'c_4554', 'userid': 'u_7542118', 'videoid': 'v_71162', 'activitydate': '2015-09-24T22:53:14Z'}, {'topic': 'userview', 'channelid': 'c_8381', 'userid': 'u_8935585', 'videoid': 'v_1428', 'activitydate': '2015-09-24T22:53:14Z'}, {'topic': 'userview', 'channelid': 'c_6366', 'userid': 'u_7247424', 'videoid': 'v_96060', 'activitydate': '2015-09-24T22:53:14Z'}, {'topic': 'userview', 'channelid': 'c_1402', 'userid': 'u_5600706', 'videoid': 'v_73964', 'activitydate': '2015-09-24T22:53:14Z'}, {'topic': 'userview', 'channelid': 'c_3478', 'userid': 'u_7224356', 'videoid': 'v_1365', 'activitydate': '2015-09-24T22:53:14Z'}, {'topic': 'userview', 'channelid': 'c_7445', 'userid': 'u_1913683', 'videoid': 'v_25920', 'activitydate': '2015-09-24T22:53:14Z'}, {'topic': 'userview', 'channelid': 'c_7477', 'userid': 'u_9736978', 'videoid': 'v_70591', 'activitydate': '2015-09-24T22:53:14Z'}, {'topic': 'userview', 'channelid': 'c_1899', 'userid': 'u_619195', 'videoid': 'v_90287', 'activitydate': '2015-09-24T22:53:14Z'}, {'topic': 'userview', 'channelid': 'c_8780', 'userid': 'u_6526357', 'videoid': 'v_72924', 'activitydate': '2015-09-24T22:53:14Z'}, {'topic': 'userview', 'channelid': 'c_234', 'userid': 'u_9055083', 'videoid': 'v_15733', 'activitydate': '2015-09-24T22:53:14Z'}]}"
# print parseUserActivityJSON(JSONData)


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
                    videoDict['tags'] = transformListToString(snippet['tags'])
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
