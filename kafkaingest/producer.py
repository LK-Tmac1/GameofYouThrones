#!/usr/bin/python

from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
from utility.constant import MasterPublicIP, DB_TB_CHANNEL, DB_NAME, TOPIC_USER_VIEW
from mysql.mysqldao import  execute_query
from api.video import getVIdByChannelActivity, getVideoById
from dataengineering import userActivityRandomBatch

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

def produceUserActivity(topic=TOPIC_USER_VIEW, videoId='', dateStr='', count=1):
    # Produce user activity data for a given video on a given date
    useractivity = userActivityRandomBatch(topic=topic, vid=videoId, dateStr=dateStr, size=count)
    dataProducer(topic, useractivity)

    
