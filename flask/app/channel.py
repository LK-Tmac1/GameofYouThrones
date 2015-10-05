from flask import render_template
from flask.globals import request
from app import app
from api.client import getJSONData
from api.parser import parseChannelJSON, parseVideoJSONSearch
import urllib
from hbase.query import queryVideoByChannel
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
        channelDataList = parseChannelJSON(channelJSON)
        return render_template("channel.html", channelList=channelDataList)
    else:
        channelTitle = request.form["channelinfo"]
        channelId = channelTitle[0:channelTitle.rfind(':')]
        channelTitle = channelTitle[channelTitle.rfind(':') + 1:len(channelTitle)]
        startDate = getTimestampNow()
        useractivity = request.form["activitytype"]
        endDate = str(getDateFromStart(str(startDate), int(request.form["daterange"]), True)) + 'T'
        dateRangeList = getDateRangeList(startDate, endDate, offset=1)
        resultTuple = queryVideoByChannel(channelid=getRandomChannelID, topn=request.form["topn"],
                            useractivity=useractivity, mode='_daily',
                            dateRangeList=dateRangeList)
        useractivity = useractivity[0:len('user')] + ' ' + useractivity[len('user') :len(useractivity)]
        Filter = str(urllib.urlencode({"channelId":channelId, 'type':'video'}))
        videoJSON = getJSONData('search', Filter, part='snippet', maxResults=True)
        videoList = parseVideoJSONSearch(videoJSON)[0:int(request.form["topn"])]
        videoDictList = jsonifyVideo(videoList=videoList, dataList=resultTuple[0])
        videoDictAccumList = jsonifyVideo(videoList=videoList, dataList=resultTuple[1])
        return render_template('channelvideo.html', channelTitle=channelTitle, useractivity=useractivity,
                        topn=request.form["topn"], daterange=request.form["daterange"], dateRangeList=dateRangeList,
                        videoDictList=videoDictList, videoDictAccumList=videoDictAccumList)
