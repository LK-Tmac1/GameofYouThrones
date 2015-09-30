from flask import render_template
from flask.globals import request
from hbase.hbdao import getVideoStatByRowKey
from app import app

@app.route('/video')
def video_home():
    return render_template("video.html", title='Video')


@app.route('/video', methods=['POST'])
def video_search():
    keyword = request.form["videokeyword"]
    keyword = 'v_123'
    print getVideoStatByRowKey(keyword)
    return render_template("channel.html", title='Video')

