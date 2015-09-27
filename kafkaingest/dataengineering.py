#!/usr/bin/python
from random import randint
from mysql.mysqldao import execute_query, select
from utility.constant import DB_NAME, DB_TB_VIDEO, TOPIC_USER_VIEW, DE_DEFAULT_DELIMITER
from utility.helper import getTimestampNow

def userActivityRandomBatch(topic=TOPIC_USER_VIEW, vid='', dateStr='', uid='', size=1):
    dataList = []
    video = select(DB_NAME, DB_TB_VIDEO, ['id', 'channelid'], ['id'], [{'id':vid}])
    if len(video) == 0:
        video = execute_query("SELECT id, channelid FROM " + DB_NAME + "." + DB_TB_VIDEO + \
                            " ORDER BY RAND() LIMIT 0," + str(size))
    if len(video) > 0:
        for i in xrange(0, len(video)):
            dataList.append(userActivityRandom(topic, video[i][0], video[i][1], uid, dateStr))
    else:
        for i in xrange(0, size):
            dataList.append(userActivityRandom(topic, '', '', uid, dateStr))
    dataList = ['2015-09-27T17:45:49Z userview v_74517 c_8062 u_1551128',
                       '2015-09-27T17:48:00Z userview v_74517 c_8062 u_1551129',
                       '2015-09-27T17:55:00Z userview v_74517 c_8062 u_1551130',
                       '2015-09-27T17:59:00Z userview v_74517 c_8062 u_1551131']
    return '\n'.join(dataList)

def userActivityRandom(topic=TOPIC_USER_VIEW, vid='', cid='', uid='', dateStr=''):
    dataList = []
    dataList.append(dateStr if dateStr != '' else getTimestampNow())
    dataList.append(topic if topic != '' else TOPIC_USER_VIEW)
    dataList.append(vid if vid != '' else "v_" + str(randint(1, 100000)))
    dataList.append(cid if cid != '' else "c_" + str(randint(1, 10000)))
    dataList.append(uid if uid != '' else "u_" + str(randint(1, 10000000)))
    print dataList
    return DE_DEFAULT_DELIMITER.join(dataList)
