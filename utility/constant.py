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
# HDFS
MasterPublicIP = "54.174.174.113"
HDFS_MASTER_DNS = "hdfs://ec2-54-174-174-113.compute-1.amazonaws.com:9000"
HDFS_DEFAULT_PATH = '/data'
LOCAL_DEFAULT_PATH = "/home/ubuntu/project"
PROJECT_PATH = LOCAL_DEFAULT_PATH + "/GameofYouThrones"
LOCAL_TEMP_PATH = LOCAL_DEFAULT_PATH + HDFS_DEFAULT_PATH
FILE_TYPE = '.txt'
SPARK_CMD = """export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.8.2.1-src.zip:$PYTHONPATH
export PYTHONPATH="%s" """ % (PROJECT_PATH)
# Kafka set up
MAX_BUFFER_SIZE = None
TOPIC_CATEGORY = 'category'
TOPIC_CHANNEL = 'channel'
TOPIC_USER = 'user'
TOPIC_VIDEO = 'video'
TOPIC_USER_VIEW = 'userview'
TOPIC_USER_SHARE = 'usershare'
TOPIC_USER_SUBSCRIBE = 'usersubscribe'
TOPIC_USER_LIKE = 'userlike'
TOPIC_USER_COMMENT = 'usercomment'
DE_USER_ACTIVITY_DELIMITER = ' '
USER_ACTIVETY_MINUTE_UNIT = 30
# HBase
HB_VIDEO_METADATA_LIST = ['id', 'title', 'channelid', 'publishedat', 'description', 'imageurl', 'categoryid']
HB_TB_MASTER = 'youtube'
MODE_DAILY = 'MODE_DAILY'
MODE_HOURLY = 'MODE_HOURLY'
MODE_DAILY_ACCU = 'MODE_DAILY_ACCU'
MODE_HOURLY_ACCU = 'MODE_HOURLY_ACCU'
COMP_KEY_DELIMITER = ':'
HB_VIDEO_PREFIX = "video_"
HB_CHANNEL_PREFIX = "channel_"
HB_CATEGORY_PREFIX = 'category_'
# Web UI
