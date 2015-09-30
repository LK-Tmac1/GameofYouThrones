#!/usr/bin/python

from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
from utility.constant import MasterPublicIP, TOPIC_USER_VIEW, DE_USER_ACTIVITY_DELIMITER
from random import randint
from utility.helper import getTimestampNow

def dataProducer(topic, msg):
    producer = SimpleProducer(KafkaClient(MasterPublicIP + ":9092"))
    producer.send_messages(topic, str(msg).encode('utf-8'))

def produceUserActivity(topic=TOPIC_USER_VIEW, useractivityList):
    # Produce user activity data for a given video on a given date
    dataProducer(topic, '\n'.join(useractivityList))

def userActivityRandom(topic=TOPIC_USER_VIEW, vid='', cid='', dateStr=''):
    dataList = []
    dataList.append(dateStr if dateStr != '' else getTimestampNow())
    dataList.append(topic if topic != '' else TOPIC_USER_VIEW)
    dataList.append(vid if vid != '' else "v_" + str(randint(1, 100000)))
    dataList.append(cid if cid != '' else "c_" + str(randint(1, 10000)))
    dataList.append("u_" + str(randint(1, 10000000)))
    return DE_USER_ACTIVITY_DELIMITER.join(dataList)    
