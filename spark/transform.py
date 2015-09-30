from utility.constant import DE_USER_ACTIVITY_DELIMITER
from utility.helper import parseDateString, parseDateTimeMinute

def transformActivity(line, hourly=False):
    itemList = line.split(DE_USER_ACTIVITY_DELIMITER)
    if len(itemList) > 3:
        timestamp = ''
        if hourly:
            timestamp = parseDateTimeMinute(itemList[0])
        else:
            timestamp = str(parseDateString(itemList[0]))
        return itemList[1] + ":" + itemList[2] + ":" + ":" + timestamp

def transformActivityAggre(videoStatList, hourly=False):
    aggreVideoStat = []
    if isinstance(videoStatList, list):
        videoBucket = {}
        for dailyVideo in videoStatList:
            bucketKey = dailyVideo[0]
            bucketKey = bucketKey[0:bucketKey.rfind(':')]
            if hourly:
                bucketKey = bucketKey[0:bucketKey.rfind(':')]
            if bucketKey not in videoBucket:
                videoBucket[bucketKey] = []
            videoBucket[bucketKey].append(dailyVideo)
        for bucketKey, videoList in videoBucket.items():
            count = 0
            for dailyVideo in videoList:
                count = count + int(dailyVideo[1])
            aggreVideoStat.append((videoList[len(videoList) - 1][0]+'', count))
            for i in xrange(len(videoList) - 2, -1, -1):
                count = count - int(videoList[i][1])
                aggreVideoStat.append((videoList[i][0], count))
    return aggreVideoStat
