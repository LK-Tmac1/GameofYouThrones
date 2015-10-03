from utility.helper import parseDateString, parseDateTimeMinute

def parseTempKeyValueForAccu(K, V, hourly):
    if hourly:
        # key=userview:video_1:2015-09-28T12:30 value=1 to
        # key=userview:video_1:2015-09-28 value=T12:30_1
        return (K[0:K.rfind('T')], K[K.rfind('T'):len(K)] + '_' + str(V))
    else:
        # key=userview:video_1:2015-09-28 value=1 to
        # key=userview:video_1 value=:2015-09-28_1
        return (K[0:K.rfind(":")], K[K.rfind(':'):len(K)] + "_" + str(V))
        

def transformHourlyToDailyKey(hourlyKey, value):
    # userview:video_1:2015-09-28T12:30 to
    # userview:video_1:2015-09-28
    dailyKey = hourlyKey[0:hourlyKey.rfind('T')] if hourlyKey.rfind('T') > 0 else "unknown"
    return (dailyKey, value)

def calculateAccuSum(K, V):
    """
    Return a list of tuple, where the first element is the key 
    and the second is the accumulative sum.
    """
    valueList = list(V)
    keyValueTupleList = list()
    accuSum = 0
    for i in xrange(0, len(valueList)):
        tempList = valueList[i].split('_')
        accuSum = accuSum + int(tempList[1]) 
        keyValueTupleList.append((K + tempList[0], accuSum))
    return keyValueTupleList

def transformActivity(line, hourly=False):
    # Sample line: 2015-09-30T16:40:00Z category channel video userview
    itemList = line.split(' ')
    if len(itemList) >= 5:
        timestamp = parseDateTimeMinute(itemList[0]) if hourly else str(parseDateString(itemList[0]))
        categoryId = itemList[1]
        channelId = itemList[2]
        videoId = itemList[3]
        useractivity = itemList[4]
        categoryVideoId = categoryId + ":" + videoId
        channelVideoId = channelId + ":" + videoId
        categoryChannelId = categoryId + ":" + channelId
        prefix_suffix = useractivity + ':%s:' + timestamp
        videoRow = prefix_suffix % videoId
        channelRow = prefix_suffix % channelId
        categoryVideoRow = prefix_suffix % categoryVideoId
        categoryChannelRow = prefix_suffix % categoryChannelId
        channelVideoRow = prefix_suffix % channelVideoId
        return [videoRow , channelRow, categoryVideoRow, categoryChannelRow, channelVideoRow]
