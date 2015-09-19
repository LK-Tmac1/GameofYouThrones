'''
Created on Sep 17, 2015

@author: Kun
'''
from client import *
from db.mysqldao import *

def saveAllCategory():
    catMap = getAllCategoryMap()
    dataMapList = []
    for key in catMap:
        catDict = {"id":key.encode('utf-8'),
                   "title":catMap[key].encode('utf-8'),
                   "channelFlag":'N',
                   "mostPopVideoFlag":"N"}
        dataMapList.append(catDict)
    insert(DB_NAME, DB_TB_CATEGORY, dataMapList)
    
def getAllCategoryMap():
    data = getJSONData("guideCategories", "regionCode=US", "snippet")
    if 'error' not in data:
        dictCategory = {}
        for item in data['items']:
            dictCategory[item['id']] = item['snippet']['title']
        return dictCategory
