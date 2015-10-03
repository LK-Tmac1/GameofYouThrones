from flask import render_template
from flask.globals import request
from app import app
from api.client import getJSONData
from api.parser import parseChannelJSON
import urllib

@app.after_request
def add_header(response):
    """
    Add headers to both force latest IE rendering engine or Chrome Frame,
    and also to cache the rendered page for 10 minutes.
    """
    response.headers['X-UA-Compatible'] = 'IE=Edge,chrome=1'
    response.headers['Cache-Control'] = 'public, max-age=0'
    return response

@app.route('/channel')
def channel():
    return render_template("channel.html", title='Channel')


@app.route('/channel', methods=['POST'])
def channel_search():
    keyword = request.form["channelkeyword"].strip()
    keyword = 'youtube' if keyword == ''  else keyword
    Filter = str(urllib.urlencode({"q":keyword, 'type':'channel'}))
    channelJSON = getJSONData('search', Filter, part='snippet', maxResults=True)
    channelDataList = parseChannelJSON(channelJSON)
    return render_template("channel.html", title='Channel', channelList=channelDataList)
