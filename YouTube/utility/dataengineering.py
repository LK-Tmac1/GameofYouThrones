#!/usr/bin/python
from random import randint
from mysql.mysqldao import execute_query, select
from utility.environment import DB_NAME, DB_TB_VIDEO
from utility.helper import getTimestampNow
import json

def videoStatDaily():
    view = randint(0, randint(0, 100))
    share = randint(0, randint(0, 3))
    subscb = randint(0, randint(0, 5))
    data = {"viewdaily":view, "sharedaily":share, "subscribeDaily":subscb}
    return data

def userActivity(action='', vid='', dateStr='', uid=''):
    # Sample data {"action": "view", "channelid": "UCiH828EtgQjTyNIMH6YiOSw", "userid": "u_1334125", 
    # "videoid": "EcbLM6xjVpA", "activitydate": "2015-09-23T22:53:27Z"}
    data = {}
    video = select(DB_NAME, DB_TB_VIDEO, ['id', 'channelid'], ['id'], [{'id':vid}])
    if len(video) == 0:
        video = execute_query("SELECT id, channelid FROM" + DB_NAME + "." + DB_TB_VIDEO + \
                            " ORDER BY RAND() LIMIT 0,1;")
    if len(video) > 0:
        data['videoid'] = video[0][0]
        data['channelid'] = video[0][1]
    else:
        data['videoid'] = "v_" + getTimestampNow() + str(randint(1, 1000))
        data['channelid'] = "c_" + getTimestampNow() + str(randint(1, 1000))
    if dateStr == '':
        data['activitydate'] = getTimestampNow()
    if uid == '':
        data['userid'] = "u_" + str(randint(1, 10000000))
    if action == '':
        data['action'] = 'view'
    else:
        data['action'] = action
    return json.dumps(data)
    
print userActivity(vid='EcbLM6xjVpA')       
        
    
