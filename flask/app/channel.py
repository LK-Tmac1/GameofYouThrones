from flask import render_template
from flask.globals import request
from app import app
from api.client import getJSONData
from api.parser import parseChannelJSON
import urllib
from hbase.query import queryVideoByChannel


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
        queryVideoByChannel(channeldid=request.form["channelid"], topn=request.form["topn"],
                            activitytype=request.form["activitytype"], mode='_daily',
                            daterange=request.form["daterange"])
        return render_template("home.html")
