from utility.helper import transformListToString

def parseChannelJSON(JSONData, categoryId='', channelList=None):
    if channelList is None:
        channelList = []
    # Return a list of channel key-value pair
    if "items" in JSONData:
        for item in JSONData["items"]:
            snippet = item["snippet"]
            channelDict = {
            "id":item["id"], "title":snippet["title"],
            "description":snippet["description"],
            "categoryid":categoryId}
            if "statistics" in item:
                stat = item["statistics"] 
                channelDict["viewcount"] = stat["viewCount"]
                channelDict["commentcount"] = stat["commentCount"]
                channelDict["subscribercount"] = stat["subscriberCount"]
                channelDict["videocount"] = stat["videoCount"] 
            channelDict["publishedat"] = "null" if 'publishedAt' not in snippet \
                            else snippet["publishedAt"]
            channelList.append(channelDict)
    return channelList

def is_ascii(mystring):
    try:
        mystring.decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True

def parseSearchJSON(JSONData, idName):
    dataList = []
    # Return a list of channel key-value pair
    if "items" in JSONData:
        for item in JSONData["items"]:
            snippet = item["snippet"]
            dataDict = {idName:item["id"][idName].encode('utf-8'),
                        "title":snippet["title"].encode('utf-8')}
            if is_ascii(dataDict['title']):
                dataList.append(dataDict)
    return dataList

def parseVideoJSON(JSONData, videoList=None):
    print "JSONData:", JSONData
    videoList = [] if videoList is None else videoList
    # Return a list of video key-value pair
    # If videoList is provided, then append new video to it
    if "items" in JSONData:
        for item in JSONData["items"]:
            if 'statistics' in item:
                # Otherwise this video is unavailable
                stat = item['statistics']
            snippet = item['snippet']
            videoDict = {
                "id":item["id"]['videoId'], "title":snippet["title"],
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
    print "videoList:", videoList
    return videoList

