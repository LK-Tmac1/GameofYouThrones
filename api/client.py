from utility.constant import API_HOME, API_KEY
import requests
import json

def getJSONData(resource, Filter, part="id", maxResults=False, pageToken=None):
    requestURL = API_HOME + resource + "?key=" + API_KEY + "&part=" + part + "&" + Filter
    print requestURL
    if maxResults:
        requestURL = requestURL + "&maxResults=50"
    if pageToken is not None:
        requestURL = requestURL + "&pageToken=" + pageToken
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
