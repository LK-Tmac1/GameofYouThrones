'''
Created on Sep 17, 2015

@author: Kun
'''
from client import *
from db.mysqldao import *

def parseChannelJSON(JSONData, categoryId):
    # Return a list of channel key-value pair
    channelList = []
    if "items" in JSONData:
        for item in JSONData["items"]:
            snippet = item["snippet"]
            channelDict = {
            "id":item["id"], "title":item["snippet"]["title"],
            "description":item["snippet"]["description"],
            "viewcount":item["statistics"]["viewCount"],
            "commentcount":item["statistics"]["commentCount"],
            "subscribercount":item["statistics"]["subscriberCount"],
            "videocount":item["statistics"]["videoCount"],
            "categoryid":categoryId,
            "playlistFlag":'N' }
            if 'publishedAt' not in snippet:
                snippet["publishedAt"] = "null"
            channelDict["publishedat"]=item["snippet"]["publishedAt"]
            channelList.append(channelDict)
    return channelList  
    
def saveChannelByCategory(categoryId):
    categoryId=str(categoryId)
    Filter = "categoryId=" + categoryId
    part = "id,snippet,statistics"
    resource = "channels"
    count = getDataCount(resource, Filter)
    print "----------------", count, "channel data of category", categoryId
    if count > 0:
        data = getJSONData(resource, Filter, part, True)
        channelList = parseChannelJSON(data, categoryId)
        insert(DB_NAME, DB_TB_CHANNEL, channelList)
        while count > MAX_RESULT:
            nextPageToken = data["nextPageToken"]
            data = getJSONData(resource, Filter, part, True , nextPageToken)
            channelList = parseChannelJSON(data, categoryId)
            insert(DB_NAME, DB_TB_CHANNEL, channelList)
            count = count - MAX_RESULT
            print "====", count
    update(DB_NAME, DB_TB_CHANNEL_CATEGORY, ['channelFlag'], ['id'], [{'channelFlag':'Y', 'id':str(categoryId)}])
            
def saveAllChannelByCategory():
    idList = select(DB_NAME, DB_TB_CHANNEL_CATEGORY, ["id"], ['channelFlag'], [{'channelFlag':'N'}])
    for ID in idList:
        saveChannelByCategory(ID[0])


#saveAllChannelByCategory()
print "~~~~~~~~~~~~~~~~~"

