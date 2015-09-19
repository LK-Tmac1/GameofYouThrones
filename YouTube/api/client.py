'''
Created on Sep 17, 2015

@author: Kun
'''
from utility.environment import *
import requests
import json

def getJSONData(resource, Filter, part="id", maxResults=False, pageToken=None):
    requestURL = API_HOME + resource + "?key=" + API_KEY + "&part=" + part + "&" + Filter
    if maxResults:
        requestURL = requestURL + "&maxResults=50"
    if pageToken is not None:
        requestURL = requestURL + "&pageToken=" + pageToken
    print requestURL
    req = requests.get(requestURL)
    data = json.loads(req.text)
    if 'error' not in data:
        return data
    return None

def getDataCount(resource, Filter):
    data = getJSONData(resource, Filter)
    # print data
    if data is not None:
        return data["pageInfo"]["totalResults"]
    else:
        return 0
            
def parseListToString(strList):
    if strList is not None:
        for i in xrange(0, len(strList)):
            strList[i] = strList[i].encode('utf-8')
        return ','.join(strList)
    
