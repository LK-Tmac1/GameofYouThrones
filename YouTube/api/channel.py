'''
Created on Sep 17, 2015

@author: Kun
'''
from client import getJSONData
from utility.environment import  DB_NAME, DB_TB_CHANNEL, DB_TB_CATEGORY
from utility.parser import parseChannelJSON
from db.mysqldao import insert, select, update
 
    
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
