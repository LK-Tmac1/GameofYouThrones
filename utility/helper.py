
from datetime import date, datetime, time, timedelta as td
from utility.constant import USER_ACTIVETY_MINUTE_UNIT
from random import randint

def getTimestampNow():
    strTimestamp = str(datetime.now()).replace(' ', 'T')
    return strTimestamp[0:strTimestamp.rfind(".")] + "Z"

def parseDateTimeMinute(timestamp):
    # Given a timestamp, "2015-9-15T12:00:02.0Z", parse it to a given unit level
    timestamp = str(timestamp)
    if len(timestamp) < 10:
        return ''
    timestamp = timestamp[0:timestamp.rfind(':')]
    minute = int(timestamp[timestamp.rfind(':') + 1: len(timestamp) + 1])
    timeUnit = str(USER_ACTIVETY_MINUTE_UNIT * (minute / USER_ACTIVETY_MINUTE_UNIT))
    if len(timeUnit) < 2:
        timeUnit = '0' + timeUnit
    return timestamp[0:timestamp.rfind(':')] + ':' + timeUnit

def parseDateString(dateStr):
    # Should be in the format of "2015-9-15T12:00:02.0Z"
    if len(str(dateStr)) < 11:
        return dateStr
    year = int(dateStr[0:dateStr.find('-')])
    month = int(dateStr[dateStr.find('-') + 1:dateStr.rfind('-')])
    day = int(dateStr[dateStr.rfind('-') + 1:dateStr.find('T')])
    return date(year, month, day)

def getDatetimeFromStartList(count, startDatetime=None, ago=True):
    dateTimeList = []
    if startDatetime is None:
        startDatetime = datetime.now()
    for i in xrange(0, count):
        diff = str(datetime.now() - td(minutes=USER_ACTIVETY_MINUTE_UNIT * (i + 8)))
        diff = parseDateTimeMinute(diff)
        dateTimeList.append(diff[diff.find(' ') + 1:len(diff)])
    return list(reversed(dateTimeList))
        

def getDateFromStart(startDateStr, offset, ago):
    startDate = parseDateString(startDateStr)
    if ago:
        return startDate - td(days=int(offset))
    else:
        return startDate + td(days=int(offset))

def getDateRangeList(dateStr1, dateStr2, offset=7):
    # Return a list of date between two dates, with offset as the range unit 
    date1 = parseDateString(dateStr1)
    date2 = parseDateString(dateStr2)
    if date1 > date2:
        date3 = date1
        date1 = date2
        date2 = date3
    delta = date2 - date1
    dateList = []
    i = 0
    while i < delta.days:
        dateList.append(str(date1 + td(days=i)))
        i = i + offset   
    return dateList


def transformListToString(strList):
    # Returns a string that concatenates all objects in a list by comma
    if strList is not None:
        for i in xrange(0, len(strList)):
            strList[i] = strList[i].encode('utf-8')
        return ','.join(strList)
    
def generateRandomTimeStr(dateStr, count=1):
    timeList = []
    for i in xrange(0, count):
        timeList.append("%sT%02d:%02d:%02d" % (dateStr, randint(0, 23), randint(0, 59), randint(0, 59)))
    return timeList
