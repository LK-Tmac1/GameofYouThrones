#!/usr/bin/python

from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from utility.environment import MasterPublicIP
from utility.helper import parseDateString, getTimestampNow
import os
def dataConsumer(topic, group, count=1, address=MasterPublicIP + ":9092"):
    kafka_consumer = SimpleConsumer(KafkaClient(address), group, topic)
    messages = kafka_consumer.get_messages(count=count)
    for message in messages:
        print(message.message.value)
    print "===="
    
def flush2HDFS(topic, dataSet, dateStr="", outputdir=""):
    if dateStr == "" or parseDateString(dateStr) == "":
        dateStr = parseDateString(getTimestampNow())
    else:
        dateStr = parseDateString(dateStr)
    hadoopPath = "%s/%s/%s" % (outputdir, topic, dateStr)
    filePath = hadoopPath + ".txt"
    if os.path.exists(hadoopPath):
        if not os.path.exists(filePath):
            os.mknod(filePath)
        tempfile = open(filePath, "w")
        for data in dataSet:
            tempfile.write(data + "\n")


dataConsumer("video", "test", 100)
