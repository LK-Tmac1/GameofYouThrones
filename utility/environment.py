# Constant for all parameters
# MySQL
DB_HOST = "localhost"
DB_USER = 'root'
DB_PASSWD = ''
DB_NAME = "youtube"
DB_TB_CATEGORY = "category"
DB_TB_CHANNEL = 'channel'
DB_TB_PLAYLIST = 'playlist'
DB_TB_VIDEO = 'video'
# YouTube API
API_HOME = "https://www.googleapis.com/youtube/v3/"
API_KEY = "AIzaSyAD4GzwWB16ilVKfsDymCfaZBodtUzA7-Y"
MAX_RESULT = 50
CHANNEL_MINE = "UCxOuw7Mt_5drSKdyjh7R07w"
DATE_OFFSET = 10
# AWS EC2
MasterPublicIP = "54.174.174.113"
PROJECT_PATH = "/home/ubuntu/project/GameofYouThrones/YouTube"
SPARK_CMD = """export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.8.2.1-src.zip:$PYTHONPATH
export PYTHONPATH="%s" """ % (PROJECT_PATH)
# Kafka topics set up
TOPIC_CATEGORY = 'category'
TOPIC_CHANNEL = 'channel'
TOPIC_CHANNEL_ACTIVITY = 'channelactivity'
TOPIC_USER = 'user'
TOPIC_VIDEOSTAT = 'videostat'
TOPIC_VIDEO = 'video'
TOPIC_USER_ACTIVITY = 'useractivity'

