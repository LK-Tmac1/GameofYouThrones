from etl.mysqldao import select
from utility.constant import DB_TB_VIDEO, DB_NAME, HB_VIDEO_METADATA_LIST, TOPIC_USER_VIEW, \
    TOPIC_USER_LIKE, TOPIC_USER_SHARE, TOPIC_USER_SUBSCRIBE, TOPIC_USER_COMMENT
from hbase.hbdao import putMetadata
from utility.helper import parseDateString, getTimestampNow, getDateRangeList, generateRandomTimeStr
from random import randint
from kafkaingest.producer import userActivityRandom

def videoMetadataETL():
    videoList = select(DB_NAME, DB_TB_VIDEO, \
                       HB_VIDEO_METADATA_LIST, ['metadataflag'], [{'metadataflag':'N'}])
    putMetadata(videoList)

def userActivityETL(startDate='', endDate=''):
    videoList = select(DB_NAME, DB_TB_VIDEO, ['id', 'publishedat'])
    for video in videoList:
        if startDate == '':
            sDate = parseDateString(video[1])
        if endDate == '':
            eDate = parseDateString(getTimestampNow())
        for dateValue in getDateRangeList(sDate, eDate):
            for dateTime in generateRandomTimeStr(dateValue, randint(0, 200)):
                userActivityRandom(TOPIC_USER_VIEW, video[0], dateTime)
            for dateTime in generateRandomTimeStr(dateValue, randint(0, 10)):
                userActivityRandom(TOPIC_USER_LIKE, video[0], dateTime)
            for dateTime in generateRandomTimeStr(dateValue, randint(0, 5)):
                userActivityRandom(TOPIC_USER_SHARE, video[0], dateTime)
            for dateTime in generateRandomTimeStr(dateValue, randint(0, 20)):
                userActivityRandom(TOPIC_USER_SUBSCRIBE, video[0], dateTime)
            for dateTime in generateRandomTimeStr(dateValue, randint(0, 200)):
                userActivityRandom(TOPIC_USER_COMMENT, video[0], dateTime)
        
        
        
videoMetadataETL()
