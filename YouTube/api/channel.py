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
            print item;
            channelDict = {
            "id":item["id"], "title":item["snippet"]["title"],
            "publishedat":item["snippet"]["publishedAt"],
            "description":item["snippet"]["description"],
            "viewcount":item["statistics"]["viewCount"],
            "commentcount":item["statistics"]["commentCount"],
            "subscribercount":item["statistics"]["subscriberCount"],
            "videocount":item["statistics"]["videoCount"],
            "categoryid":categoryId }
            channelList.append(channelDict)
    return channelList  
    
def saveChannelByCategory(categoryId):
    Filter = "categoryId=" + categoryId
    part = "id,snippet,statistics"
    resource = "channels"
    count = getDataCount(resource, Filter)
    print "----------------", count, "channel data of category", categoryId
    data = getJSONData(resource, Filter, part, True)
    channelList = parseChannelJSON(data, categoryId)
    # print channelList[0],"==================="
    insert(DB_NAME, DB_TB_CHANNEL, channelList)
    print "====", count
    while count > MAX_RESULT:
        nextPageToken = data["nextPageToken"]
        data = getJSONData(resource, Filter, part, True , nextPageToken)
        channelList = parseChannelJSON(data, categoryId)
        insert(DB_NAME, DB_TB_CHANNEL, channelList)
        count = count - MAX_RESULT
        print "====", count
        update(DB_NAME, DB_TB_CHANNEL_CATEGORY, ['categoryId'], [{'channelFlag':'N', 'id':categoryId}])
            
def saveAllChannelByCategory():
    idList = select(DB_NAME, DB_TB_CHANNEL_CATEGORY, ["id"], ['channelFlag'], [{'channelFlag':'N'}])
    for ID in idList:
        print "-------------ID", ID[0]
        saveChannelByCategory(ID[0])
