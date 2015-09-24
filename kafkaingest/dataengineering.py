#!/usr/bin/python
from random import randint
from mysql.mysqldao import execute_query, select
from utility.constant import DB_NAME, DB_TB_VIDEO, TOPIC_USER_VIEW
from utility.helper import getTimestampNow

def videoStatDaily():
    view = randint(0, randint(0, 100))
    share = randint(0, randint(0, 3))
    subscb = randint(0, randint(0, 5))
    data = {"viewdaily":view, "sharedaily":share, "subscribeDaily":subscb}
    return data

def userActivityRandomBatch(topic=TOPIC_USER_VIEW, vid='', dateStr='', uid='', size=1):
    dataList = {'useractivity':[]}
    video = select(DB_NAME, DB_TB_VIDEO, ['id', 'channelid'], ['id'], [{'id':vid}])
    if len(video) == 0:
        video = execute_query("SELECT id, channelid FROM " + DB_NAME + "." + DB_TB_VIDEO + \
                            " ORDER BY RAND() LIMIT 0," + str(size))
    if len(video) > 0:
        for i in xrange(0, len(video)):
            dataList['useractivity'].append(userActivityRandom(topic, video[i][0], video[i][1], uid, dateStr))
    else:
        for i in xrange(0, size):
            dataList['useractivity'].append(userActivityRandom(topic, '', '', uid, dateStr))
    return dataList

def userActivityRandom(topic, vid, cid, uid, dateStr):
    data = {'videoid':vid, 'userid':uid, 'channelid':cid, \
            'activitydate':dateStr, 'topic':topic}
    if data['activitydate'] == '':
        data['activitydate'] = getTimestampNow()
    if data['userid'] == '':
        data['userid'] = "u_" + str(randint(1, 10000000))
    if data['topic'] == '':
        data['topic'] = TOPIC_USER_VIEW
    if data['videoid'] == '':
        data['videoid'] = "v_" + str(randint(1, 100000))
    if data['channelid'] == '':
        data['channelid'] = "c_" + str(randint(1, 10000))
    print data
    return data
    
