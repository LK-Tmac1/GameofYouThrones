'''
Created on Sep 17, 2015

@author: Kun
'''
from client import *
from db.mysqldao import *

# print getDataCount("channels", Filter="categoryId=GCTXVzaWM", True)
def saveAllCategory():
    catMap = getAllCategoryMap()
    dataMapList = []
    for key in catMap:
        dataMapList.append({"id":key.encode('utf-8'), "title":catMap[key].encode('utf-8')})
    print dataMapList
    insert(DB_NAME, DB_TB_CHANNEL_CATEGORY, dataMapList)
    
def getAllCategoryMap():
    data = getJSONData("guideCategories", "regionCode=US", "snippet")
    if 'error' not in data:
        dictCategory = {}
        for item in data['items']:
            dictCategory[item['id']] = item['snippet']['title']
        return dictCategory
