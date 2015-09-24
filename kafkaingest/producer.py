#!/usr/bin/python

from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
from utility.environment import MasterPublicIP, CHANNEL_MINE, DB_TB_CHANNEL, DB_NAME, \
    DB_TB_VIDEO
from mysql.mysqldao import select, execute_query
from api.video import getVIdByChannelActivity, getVideoById
from utility.helper import parseDateString, getTimestampNow
from utility.dataengineering import videoStatDaily

def dataProducer(topic, msg):
    producer = SimpleProducer(KafkaClient(MasterPublicIP + ":9092"))
    producer.send_messages(topic, str(msg).encode('utf-8'))

def produceVideoByChannel(channelId, ALL=False):
    vIdSet = getVIdByChannelActivity(channelId, ALL)
    if vIdSet is not None:
        videoList = getVideoById(vIdSet)
        for video in videoList:
            dataProducer("video", video)

def produceVideoByAllChannel(ALL=False):
    # Batch for video daily aggregrate data
    channelId = execute_query("select id from " + DB_NAME + "." + DB_TB_CHANNEL)
    for cID in channelId:
        produceVideoByChannel(cID[0], ALL)
        print "----------Done for channel " + cID[0]
        # update ETL table in mysql
        
def produceVideoStatByDay(videoId, dateStr=''):
    # Produce the statistics data for a video on a given date
    video = select(DB_NAME, DB_TB_VIDEO, ['publishedAt'], ['id'], [{'id':videoId}])
    if len(video) > 0:
        dateStr = str(parseDateString(dateStr))
        if dateStr == '':
            dateStr = str(parseDateString(getTimestampNow()))
        data = videoStatDaily()
        data["id"] = videoId
        data["publishedat"] = video[0][0]
        data["statdate"] = dateStr
        dataProducer("videostat", data)

for i in xrange(0, 100000):
    print produceVideoStatByDay("WYuTmbH9BQI")
