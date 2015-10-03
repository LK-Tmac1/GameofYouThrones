from app import app
from flask import render_template

@app.route('/')
@app.route('/home')
def home():
    return render_template("home.html", title='Home')


@app.route('/test')
def test():
    return render_template("test.html")

