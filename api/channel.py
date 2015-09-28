#!/usr/bin/python

from client import getJSONData
from utility.constant import  DB_NAME, DB_TB_CHANNEL, DB_TB_CATEGORY
from api.parser import parseChannelJSON
from mysql.mysqldao import insert, select, update
 
    
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
    #idList = execute_query("select id from " + DB_NAME + "." + DB_TB_CATEGORY + " where channelFlag='N'")
    idList = select(DB_NAME, DB_TB_CATEGORY, ["id"], ['channelFlag'], [{'channelFlag':'N'}])
    for ID in idList:
        saveChannelByCategory(ID[0])
