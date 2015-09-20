'''
Created on Sep 17, 2015

@author: Kun
'''
from client import getJSONData, parseListToString
from db.mysqldao import *
from utility.environment import DB_TB_PLAYLIST, DB_TB_VIDEO

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
    # idList = select(DB_NAME, DB_TB_PLAYLIST, ["id", "defaultVideoId"], ['videoFlag'], [{'videoFlag':'N'}])
    query = "select id, defaultvideoid from " + DB_NAME + "." + DB_TB_PLAYLIST + " where videoflag = 'N'"
    print query
    idList = execute_query(query)
    start = 0
    count = 50
    end = start + count
    while start < len(idList):
        playlistIdList = []
        videoIdList = []
        videolist = []
        batch = 20
        while batch > 0 and start < len(idList):
            for j in xrange(start, end):
                videoIdList.append(idList[j][1])
                playlistIdList.append({'videoflag':'Y', 'id':idList[j][0]})
            start = end
            end = end + count   
            videolist = videolist + getVideoByIdList(videoIdList)
            batch = batch - 1
        insert(DB_NAME, DB_TB_VIDEO, videolist)
        update(DB_NAME, DB_TB_PLAYLIST, ['videoflag'], ['id'], playlistIdList)
        batch = 20
        print "----------start=", start, "end=", end, "total=", len(idList)

saveVideoByPlaylistDefault()
print "~~~~~~~~~~~~~~~~~"
