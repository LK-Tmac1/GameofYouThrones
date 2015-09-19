'''
Created on Sep 17, 2015

@author: Kun
'''
from client import getJSONData
from db.mysqldao import insert, select, update, DB_NAME, DB_TB_CHANNEL, DB_TB_CATEGORY

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
    
def saveChannelByCategory(categoryId):
    categoryId = str(categoryId)
    Filter = "categoryId=" + categoryId
    part = "id,snippet,statistics"
    resource = "channels"
    data = getJSONData(resource, Filter, part, True)
    while data is not None:
        channelList = parseChannelJSON(data, categoryId)
        insert(DB_NAME, DB_TB_CHANNEL, channelList)
        if 'nextPageToken' in data:
            nextPageToken = data["nextPageToken"]
            data = getJSONData(resource, Filter, part, True , nextPageToken)
        else:
            break
    update(DB_NAME, DB_TB_CATEGORY, ['channelFlag'], ['id'], [{'channelFlag':'Y', 'id':str(categoryId)}])
            
def saveAllChannelByCategory():
    idList = select(DB_NAME, DB_TB_CATEGORY, ["id"], ['channelFlag'], [{'channelFlag':'N'}])
    for ID in idList:
        saveChannelByCategory(ID[0])


# saveAllChannelByCategory()
print "~~~~~~~~~~~~~~~~~"

