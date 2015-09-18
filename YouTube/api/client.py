'''
Created on Sep 17, 2015

@author: Kun
'''
from general.environment import *
from db.mysqldao import *
import requests
import json

def getJSONData(resource, Filter, part="id", maxResults=False, pageToken=None):
    requestURL = API_HOME + resource + "?key=" + API_KEY + "&part=" + part + "&" + Filter
    if maxResults:
        requestURL = requestURL + "&maxResults=50"
    if pageToken is not None:
        requestURL = requestURL + "&pageToken=" + pageToken
#    print requestURL
    req = requests.get(requestURL)
    data = json.loads(req.text)
    if 'error' not in data:
        return data
    return None

def getDataCount(resource, Filter):
    data = getJSONData(resource, Filter)
    print data
    if data is not None:
        return data["pageInfo"]["totalResults"]
    else:
        return 0

def dic_look_up(dic, key):
    if isinstance(dic, dict):
        if key in dic:
            return dic[key]
        for k, v in dic.items():
            if isinstance(v, dict):
                return dic_look_up(v, key)
            

