from utility.constant import DE_USER_ACTIVITY_DELIMITER
from utility.helper import parseDateString, parseDateTimeMinute
from hbase.format import formatCategoryId, formatChannelId, formatVideoId, \
formatCategoryVideoIdPair, formatChannelVideoIdPair, formatCategoryChannelIdPair

def transformActivity(line, hourly=False):
    itemList = line.split(DE_USER_ACTIVITY_DELIMITER)
    # Sample line: 2015-09-30T16:40:00Z category channel video userview
    if len(itemList) >= 5:
        timestamp = parseDateTimeMinute(itemList[0]) if hourly else str(parseDateString(itemList[0]))
        categoryId = formatCategoryId(itemList[1])
        channelId = formatChannelId(itemList[2])
        videoId = formatVideoId(itemList[3])
        useractivity = itemList[4]
        categoryVideoId = formatCategoryVideoIdPair(videoId, categoryId)
        channelVideoId = formatChannelVideoIdPair(videoId, channelId)
        categoryChannelId = formatCategoryChannelIdPair(categoryId, channelId)
        prefix_suffix = useractivity + ':%s:' + timestamp
        # useractivity:categoryid:channelid:videoid:timestamp
        masterRow = prefix_suffix % categoryId + ":" + channelId + ":" + videoId
        # useractivity:categoryid:videoid:timestamp
        categoryVideoRow = prefix_suffix % categoryVideoId
        # useractivity:categoryid:channelid:timestamp
        categoryChannelRow = prefix_suffix % categoryChannelId
        # useractivity:channelid:videoid:timestamp
        channelVideoRow = prefix_suffix % channelVideoId
        return [masterRow, categoryVideoRow, categoryChannelRow, channelVideoRow]
        

def transformActivityAccuSum(useractivityList, hourly=False):
    """
    # Each element: tuple of (useractivity:rowkey:timestamp, count)
    # Daily sample: (userview:channelid:videoid:2015-09-28, 200)
    # Hourly sample: (userview:channelid:videoid:2015-09-28T12:30, 200)
    Use bucket, i.e. a list of dictionary to compute accumulative sums.
    Return a list of tuple: key and the accumulative sum
    """ 
    aggreVideoStat = []
    if isinstance(useractivityList, list):
        accuBucket = {}
        for useractivity in useractivityList:
            bucketKey = useractivity[0]
            bucketKey = bucketKey[0:bucketKey.rfind(':')]
            if hourly:
                # Further split the time to hour level
                bucketKey = bucketKey[0:bucketKey.rfind(':')]
            if bucketKey not in accuBucket:
                accuBucket[bucketKey] = []
            accuBucket[bucketKey].append(useractivity)
        for bucketKey, useractivityList in accuBucket.items():
            count = 0
            # Calculate the accumulative sum backwards
            for useractivity in useractivityList:
                count = count + int(useractivity[1])
                aggreVideoStat.append((useractivity[0], count))
    return aggreVideoStat
