from mysql.mysqldao import select, update
from utility.constant import DB_TB_VIDEO, DB_NAME, TOPIC_USER_VIEW, \
    TOPIC_USER_LIKE, TOPIC_USER_SHARE, TOPIC_USER_SUBSCRIBE, TOPIC_USER_COMMENT
from utility.helper import getTimestampNow, getDateRangeList, generateRandomTimeStr
from kafkaingest.producer import userActivityRandom
from kafkaingest.consumer import flush2Local
from random import randint

topicRndSeedDict = {
          TOPIC_USER_VIEW:20,
          TOPIC_USER_LIKE:8,
          TOPIC_USER_SHARE:8,
          TOPIC_USER_COMMENT:6,
          TOPIC_USER_SUBSCRIBE:2}

def getRandomValueList(count, useractivity, mode):
    value = topicRndSeedDict[useractivity]
    dataList = []
    offset = 1
    if mode != '_hourly':
        offset = 2 * randint(0, topicRndSeedDict[useractivity])
    for i in xrange(0, count):
        dataList.append(randint(0, value / 2) * randint(1, value) + randint(0, offset))
    dataAccumList = range(0, count)
    dataAccumList[0] = dataAccumList[0] + randint(20, 100 * (offset + 1))
    for i in xrange(1, count):
        dataAccumList[i] = dataAccumList[i - 1] + dataList[i]
    return (dataList, dataAccumList)
    
def getRandomVideoId():
    videoIdList = ['kf85BiUmMk8', 'S9srYOd8vEE', 'SgcDGsfHbuM', 'HFqal5AWXgU', 'Gq88VGoIF7I']
    return videoIdList[randint(0, len(videoIdList) - 1)]

def getRandomChannelID():
    channelIdList = ['UCbEIp4Dn6qSepBpp7vWPUIQ', 'UCwwMcOpDNorLbDjhSaM8AZg', 'UCdMJU0WAzxaz8HUUmHtvL1w', 'h2gfaGp-lFjx5bBCtaw']
    return channelIdList[randint(0, len(channelIdList) - 1)]

def userActivityGenerate(startDate=''):
    videoList = select(DB_NAME, DB_TB_VIDEO, ['id', 'channelid', 'categoryid'],
                        ['useractivityflag'], [{'useractivityflag':'N'}])
    count = 0
    batchNum = 100
    for video in videoList:
        startDate = "2015-08-01T00:00:00"
        endDate = getTimestampNow()
        for dateValue in getDateRangeList(startDate, endDate, 1):
            dataSet = []
            for topic, value in topicRndSeedDict.items():
                for dateTime in generateRandomTimeStr(dateValue, randint(1, value / 3) * randint(1, value)):
                    dataSet.append(userActivityRandom(topic, vid=video[0], cid=video[1],
                                caid=video[2], dateStr=dateTime))
            flush2Local(batchNum, ''.join(dataSet))
        count = count + 1
        print '----', count
        update(DB_NAME, DB_TB_VIDEO, ['useractivityflag'], ['id'], [{'useractivityflag':'Y', 'id':video[0]}])
        if count > 99:
            break
            batchNum = batchNum + 1
            count = 0
            
# userActivityGenerate()            
