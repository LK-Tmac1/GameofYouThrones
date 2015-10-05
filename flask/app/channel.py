from flask import render_template
from flask.globals import request
from app import app
from api.client import getJSONData
from api.parser import parseChannelJSON
import urllib
from hbase.query import queryVideoByChannel
from utility.helper import getDateFromStart, getTimestampNow, getDateRangeList


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
        startDate = getTimestampNow()
        endDate = str(getDateFromStart(str(startDate), int(request.form["daterange"]), True)) + 'T'
        dateRangeList = getDateRangeList(startDate, endDate, offset=1)
        resultTuple = queryVideoByChannel(channeldid=request.form["channelid"], topn=request.form["topn"],
                            activitytype=request.form["activitytype"], mode='_daily',
                            dateRangeList=dateRangeList)
        print resultTuple[0], '\n======\n'
        print resultTuple[1], '\n------\n'
        return render_template("home.html")
