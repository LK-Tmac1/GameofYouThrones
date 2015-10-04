#!/usr/bin/python

from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from utility.constant import MasterPublicIP, HDFS_DEFAULT_PATH, FILE_TYPE, \
    MAX_BUFFER_SIZE, LOCAL_TEMP_PATH
from utility.helper import parseDateString, getTimestampNow
import os

def dataConsumer(topic, group='default', count=1, dateStr=''):
    kafka_consumer = SimpleConsumer(KafkaClient(MasterPublicIP + ":9092"), \
                                    group, topic, max_buffer_size=MAX_BUFFER_SIZE)
    messages = kafka_consumer.get_messages(count=count)
    dataList = []
    for message in messages:
        dataList.append(message.message.value)
    if len(dataList) > 0:
        flush2HDFS(dataList, dateStr)
    
def flush2HDFS(dataSet, dateStr=''):
    """
    dateStr = parseDateString(dateStr)
    if dateStr == "":
        dateStr = parseDateString(getTimestampNow())
    localPath = LOCAL_TEMP_PATH + "/"
    localFilePath = localPath + "/" + str(dateStr) + FILE_TYPE
    hdfsPath = HDFS_DEFAULT_PATH + '/'
    if not os.path.exists(localPath):
        os.system('sudo mkdir ' + localPath)
    if not os.path.exists(localFilePath):
        os.mknod(localFilePath)
    else:
        os.system("hdfs dfs -rm %s " % (hdfsPath))
    """
    tempfile = open("/Users/Kun/Git/GameofYouThrones/sample.txt", "a")  # append mode
    # for data in dataSet:
    tempfile.write(dataSet + "\n")
    # os.system("hdfs dfs -put -f %s %s" % (localFilePath, hdfsPath))
    tempfile.close()

