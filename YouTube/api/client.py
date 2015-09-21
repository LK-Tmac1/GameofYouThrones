'''
API client for retrieving data from YouTube, by resource, part and filter parameters
@author: Kun
'''
from utility.environment import API_HOME, API_KEY
import requests
import json

def getJSONData(resource, Filter, part="id", maxResults=False, pageToken=None):
    requestURL = API_HOME + resource + "?key=" + API_KEY + "&part=" + part + "&" + Filter
    if maxResults:
        requestURL = requestURL + "&maxResults=50"
    if pageToken is not None:
        requestURL = requestURL + "&pageToken=" + pageToken
    #print requestURL
    req = requests.get(requestURL)
    data = json.loads(req.text)
    if 'error' not in data:
        return data
    return None

def getDataCount(resource, Filter):
    data = getJSONData(resource, Filter)
    if data is not None:
        return data["pageInfo"]["totalResults"]
    return 0
            

    
