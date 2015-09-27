from utility.parser import parseUserActivityJSON
from pyspark import SparkContext, SparkConf
from utility.constant import TOPIC_USER_VIEW, HDFS_MASTER_DNS, HDFS_DEFAULT_PATH, FILE_TYPE
from utility.helper import parseDateString, getTimestampNow
from utility.parser import parseActivityAggre, parseActivityMinute

 
conf = SparkConf().setAppName("testBatch")
sc = SparkContext(conf=conf)

def videoStatDaily(dateStr='', topic=TOPIC_USER_VIEW):
    dateStr = str(parseDateString(dateStr))
    if dateStr == '':
        dateStr = str(parseDateString(getTimestampNow()))
    filePath = HDFS_MASTER_DNS + HDFS_DEFAULT_PATH + '/' + topic + '/' + '2015-09-24' + FILE_TYPE
    data = sc.textFile(filePath)
    useractivity = data.map(lambda line: parseUserActivityJSON(line))
    aggreVideoStat = useractivity.map(lambda line : parseActivityAggre(line)).countByValue()
    #timeMinuteStat = useractivity.map(lambda line : parseActivityMinute(line)).countByValue()
    # 2015-09-24:v_51796:c_8067:userview
    # 2015-09-24T22:53:v_51796:userview
    print '========'
    print aggreVideoStat.collect()
    print '--------'
    #print timeMinuteStat.collect()
    
videoStatDaily()
print "Done"
