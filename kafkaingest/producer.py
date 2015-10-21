from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
from utility.constant import MasterPublicIP

def dataProducer(topic, msg):
    producer = SimpleProducer(KafkaClient(MasterPublicIP + ":9092"))
    producer.send_messages(topic, str(msg).encode('utf-8'))

def produceUserActivity(topic, useractivityList):
    # Produce user activity data for a given video on a given date
    dataProducer(topic, '\n'.join(useractivityList))

