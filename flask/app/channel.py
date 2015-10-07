from flask import render_template
from flask.globals import request
from app import app
from api.client import getJSONData
from api.parser import parseSearchJSON
import urllib
from hbase.query import scanVideoByChannel
from utility.helper import getDateFromStart, getTimestampNow, getDateRangeList
from kafkaingest.dataengineering import getRandomChannelID
from utility.jsonData import jsonifyVideo

@app.route('/channel')
def channel():
    return render_template("channel.html", title='Channel')

@app.route('/channel', methods=['POST'])
def channel_search():
    if 'channelkeyword' in request.form:
        keyword = request.form["channelkeyword"].strip()
        keyword = 'youtube' if keyword == ''  else keyword
        Filter = str(urllib.urlencode({"q":keyword, 'type':'channel'}))
        channelJSON = getJSONData('search', Filter, part='snippet', maxResults=True)
        channelDataList = parseSearchJSON(channelJSON, 'channelId')
        return render_template("channel.html", channelList=channelDataList)
    else:
        topn = int(request.form["topn"])
        channelInfo = request.form["channelinfo"]
        channelId = channelInfo[0:channelInfo.rfind(':')]
        channelTitle = channelInfo[channelInfo.rfind(':') + 1:len(channelInfo)]
        startDate = getTimestampNow()
        useractivity = request.form["activitytype"]
        endDate = str(getDateFromStart(str(startDate), int(request.form["daterange"]), True)) + 'T'
        dateRangeList = getDateRangeList(startDate, endDate, offset=1)
        resultTuple = scanVideoByChannel(channelid=getRandomChannelID, topn=topn,
                            useractivity=useractivity, mode='_daily',
                            dateRangeList=dateRangeList)
        useractivity = useractivity[0:len('user')] + ' ' + useractivity[len('user') :len(useractivity)]
        Filter = str(urllib.urlencode({"channelId":channelId, 'type':'video'}))
        videoJSON = getJSONData('search', Filter, part='snippet', maxResults=True)
        videoList = parseSearchJSON(videoJSON, 'videoId')[0:topn]
        videoDictList = jsonifyVideo(videoList=videoList, dataList=resultTuple[0])
        videoDictAccumList = jsonifyVideo(videoList=videoList, dataList=resultTuple[1])
        videoWeightList = []
        for i in xrange(0, len(videoDictAccumList)):
            valueList = videoDictAccumList[i]['data']
            value = valueList[len(valueList) - 1] - valueList[0]
            videoWeightList.append([videoDictAccumList[i]['name'], value])
        return render_template('channelvideo.html', channelTitle=channelTitle, useractivity=useractivity,
                        topn=topn, daterange=request.form["daterange"], dateRangeList=dateRangeList,
                        videoDictList=videoDictList, videoDictAccumList=videoDictAccumList,
                        videoWeightList=videoWeightList)
