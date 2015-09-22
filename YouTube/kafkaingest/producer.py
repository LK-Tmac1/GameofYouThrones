from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
from utility.environment import MasterPublicIP, CHANNEL_MINE
from api.video import getVIdByChannelActivity
import logging

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.DEBUG
)

def dataProducer(topic, key, msg):
    producer = SimpleProducer(KafkaClient(MasterPublicIP + ":9092"))
    producer.send_messages(topic, str(msg).encode('utf-8'))

def produceVideo(channelId, ALL=False):
    msgSet = getVIdByChannelActivity(channelId, ALL)
    if msgSet is not None:
        for msg in msgSet:
            dataProducer("channel", channelId, msg)

produceVideo(CHANNEL_MINE, True)
print "~~~~~"
