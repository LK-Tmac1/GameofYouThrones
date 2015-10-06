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

@app.route('/video')
def video_home():
    return render_template("video.html", title='Video')


@app.route('/video', methods=['POST'])
def video_search():
    if 'videokeyword' in request.form:
        keyword = request.form["videokeyword"].strip()
        keyword = 'youtube' if keyword == ''  else keyword
        Filter = str(urllib.urlencode({"q":keyword, 'type':'video'}))
        videoJSON = getJSONData('search', Filter, part='snippet', maxResults=True)
        videoDataList = parseSearchJSON(videoJSON)
        return render_template("video.html", videoList=videoDataList)
    else:
        videoInfo = request.form["videoinfo"]
        for video in videoInfo:
            print video
        return render_template("video.html")
        """
        videoId = videoInfo[0:videoInfo.rfind(':')]
        videoTitle = videoInfo[videoInfo.rfind(':') + 1:len(videoInfo)]
        mode = request.form["mode"]
        useractivity = request.form["activitytype"]
        videoCount = request.form['videoinfo']
        
        startDate = "2015-10-04T00:00:00"  # getTimestampNow
        endDate = str(getDateFromStart(str(startDate), int(request.form["daterange"]), True)) + 'T'
        dateRangeList = getDateRangeList(startDate, endDate, offset=1)
        resultTuple = scanVideoByChannel(channelid=getRandomChannelID, topn=request.form["topn"],
                            useractivity=useractivity, mode=mode,
                            dateRangeList=dateRangeList)
        resultTuple = scanVideoByIds(topn=topn)
        useractivity = useractivity[0:len('user')] + ' ' + useractivity[len('user') :len(useractivity)]
        Filter = str(urllib.urlencode({"videoId":videoId, 'type':'video'}))
        videoJSON = getJSONData('search', Filter, part='snippet', maxResults=True)
        videoList = parseSearchJSON(videoJSON)[0:topn]
        videoDictList = jsonifyVideo(videoList=videoList, dataList=resultTuple[0])
        videoDictAccumList = jsonifyVideo(videoList=videoList, dataList=resultTuple[1])
        return render_template('channelvideo.html', videoTitle=videoTitle, useractivity=useractivity,
                        topn=request.form["topn"], daterange=request.form["daterange"], dateRangeList=dateRangeList,
                        videoDictList=videoDictList, videoDictAccumList=videoDictAccumList)
        """

