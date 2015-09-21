'''
General helper functions.
@author: Kun
'''
from datetime import date, datetime, timedelta as td

def parseDateString(dateStr):
    # Should be in the format of "2008-10-13T12:00:02.0Z"
    year = int(dateStr[0:dateStr.find('-')])
    month = int(dateStr[dateStr.find('-') + 1:dateStr.rfind('-')])
    day = int(dateStr[dateStr.rfind('-') + 1:dateStr.find('T')])
    return date(year, month, day)

def getDateRangeList(dateStr1, dateStr2):
    date1 = parseDateString(dateStr1)
    date2 = parseDateString(dateStr2)
    if date1 > date2:
        date3 = date1
        date1 = date2
        date2 = date3
    delta = date2 - date1
    dateList = []
    for i in range(delta.days + 1):
        dateList.append(str(date1 + td(days=i)))
    return dateList

def parseVIdByImageURL(url):
    url = url[0:url.rfind("/")]
    url = url[url.rfind("/") + 1:len(url)]
    return url

def parseListToString(strList):
    # Returns a string that concatenates all objects in a list by comma
    if strList is not None:
        for i in xrange(0, len(strList)):
            strList[i] = strList[i].encode('utf-8')
        return ','.join(strList)
    
def getTimestampNow():
    strTimestamp = str(datetime.now()).replace(' ', 'T')
    return strTimestamp[0:strTimestamp.rfind(".")] + "Z"
