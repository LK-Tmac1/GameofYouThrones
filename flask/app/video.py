from flask import render_template
from flask.globals import request
from app import app
from api.client import getJSONData
from api.parser import parseSearchJSON
import urllib
from hbase.query import getVideoById
from utility.helper import getDateFromStart, getTimestampNow, getDateRangeList, getDatetimeFromStartList
from kafkaingest.dataengineering import getRandomVideoId, getRandomValueList

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
        videoDataList = parseSearchJSON(videoJSON, 'videoId')
        return render_template("video.html", videoList=videoDataList)
    else:
        videoInfo = request.form.getlist("videoinfo")
        videoInfo = videoInfo[0]
        videoTitle = videoInfo[videoInfo.rfind(':') + 1:len(videoInfo)]
        mode = request.form["mode"]
        useractivity = str(request.form["activitytype"])
        videoStatCount = int(request.form['datetimerange'])
        datetimeRangeList = []
        startDate = getTimestampNow()
        if mode == '_hourly':
            videoStatCount = videoStatCount * 2
            datetimeRangeList = getDatetimeFromStartList(count=videoStatCount)
        else:
            endDate = getDateFromStart(startDateStr=startDate, offset=videoStatCount, ago=True)
            datetimeRangeList = getDateRangeList(startDate, endDate, offset=1)
        resultTuple = getVideoById(videoId=getRandomVideoId(), videoStatCount=videoStatCount, useractivity=useractivity, mode=mode)
        resultTuple = getRandomValueList(count=int(videoStatCount), useractivity=useractivity, mode=mode)
        videoDictList = [{'name':videoTitle, 'data':resultTuple[0]}]
        videoDictAccumList = [{'name':videoTitle, 'data':resultTuple[1]}]
        # total = int(resultTuple[1][len(resultTuple[1] - 1)]) - int(resultTuple[1][0])
        total = videoDictAccumList[0]['data']
        total = total[len(total) - 1] - total[0]
        if mode == '_hourly':
            mode = 'hours'
        else:
            mode = 'days'
        useractivity = useractivity[0:len('user')] + ' ' + useractivity[len('user') :len(useractivity)]
        return render_template('videostat.html', videoTitle=videoTitle, videoDictList=videoDictList,
                               videoDictAccumList=videoDictAccumList, datetimeRangeList=datetimeRangeList,
                               useractivity=useractivity, datetimerange=request.form['datetimerange'], mode=mode, total=total)
