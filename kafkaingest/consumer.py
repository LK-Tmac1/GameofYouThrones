#!/usr/bin/python

from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from utility.constant import MasterPublicIP, TOPIC_USER_VIEW, HDFS_DEFAULT_PATH, FILE_TYPE, \
    MAX_BUFFER_SIZE, LOCAL_TEMP_PATH
from utility.helper import parseDateString, getTimestampNow
import os

def dataConsumer(topic, group='default', count=1, dateStr=''):
    kafka_consumer = SimpleConsumer(KafkaClient(MasterPublicIP + ":9092"), group, topic, max_buffer_size=MAX_BUFFER_SIZE)
    messages = kafka_consumer.get_messages(count=count)
    dataList = []
    for message in messages:
        dataList.append(message.message.value)
    if len(dataList) > 0:
        flush2HDFS(topic, dataList, dateStr)
    
def flush2HDFS(topic, dataSet, dateStr=''):
    dateStr = parseDateString(dateStr)
    if dateStr == "":
        dateStr = parseDateString(getTimestampNow())
    localPath = LOCAL_TEMP_PATH + '/' + topic
    localFilePath = localPath + "/" + str(dateStr) + FILE_TYPE
    hdfsPath = HDFS_DEFAULT_PATH + '/' + topic
    if not os.path.exists(localPath):
        os.system('sudo mkdir ' + localPath)
    if not os.path.exists(localFilePath):
        os.mknod(localFilePath)
    tempfile = open(localFilePath, "w")
    for data in dataSet:
        tempfile.write(data + "\n")
    os.system("hdfs dfs -put -f %s %s" % (localFilePath, hdfsPath))
    # os.remove(localFilePath) 

#dataConsumer(TOPIC_USER_VIEW, count=100)

