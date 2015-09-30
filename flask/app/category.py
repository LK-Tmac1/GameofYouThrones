from flask import render_template
from flask.globals import request
from app import app

@app.route('/category')
def category():
    return render_template("category.html", title='Category')