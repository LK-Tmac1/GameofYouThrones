from flask import render_template
from flask.globals import request
from hbase.hbdao import scanDataByRowPrefix
from app import app

@app.route('/video')
def video_home():
    return render_template("video.html", title='Video')


@app.route('/video', methods=['POST'])
def video_search():
    keyword = request.form["videokeyword"]
    if keyword.strip() == '':
        keyword = 'F50yjSws9gQ'
    video = scanDataByRowPrefix('v_')
    print video
    return render_template("video.html", title='Video', video=video)

