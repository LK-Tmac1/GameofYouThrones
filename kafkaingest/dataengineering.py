from mysql.mysqldao import select, update
from utility.constant import DB_TB_VIDEO, DB_NAME, TOPIC_USER_VIEW, \
    TOPIC_USER_LIKE, TOPIC_USER_SHARE, TOPIC_USER_SUBSCRIBE, TOPIC_USER_COMMENT
from utility.helper import getTimestampNow, getDateRangeList, generateRandomTimeStr
from random import randint
from kafkaingest.producer import userActivityRandom
from kafkaingest.consumer import flush2Local

def userActivityGenerate(startDate=''):
    videoList = select(DB_NAME, DB_TB_VIDEO, ['id', 'channelid', 'categoryid'],
                        ['useractivityflag'], [{'useractivityflag':'N'}])
    topicRndSeedDict = {
          TOPIC_USER_VIEW:100,
          TOPIC_USER_LIKE:10,
          TOPIC_USER_SHARE:5,
          TOPIC_USER_COMMENT:5,
          TOPIC_USER_SUBSCRIBE:3}
    count = 0
    batchNum = 5
    for video in videoList:
        startDate = "2014-01-01T00:00:00"
        endDate = getTimestampNow()
        for dateValue in getDateRangeList(startDate, endDate):
            dataSet = []
            for topic, value in topicRndSeedDict.items():
                for dateTime in generateRandomTimeStr(dateValue, randint(0, value)):
                    dataSet.append(userActivityRandom(topic, vid=video[0], cid=video[1],
                                caid=video[2], dateStr=dateTime))
            flush2Local(batchNum, ''.join(dataSet))
        count = count + 1
        print '----', count
        update(DB_NAME, DB_TB_VIDEO, ['useractivityflag'], ['id'], [{'useractivityflag':'Y', 'id':video[0]}])
        if count > 1200:
            batchNum = batchNum + 1
            count = 0
            
userActivityGenerate()              
