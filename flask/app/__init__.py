from flask import Flask
app = Flask(__name__)
from app import home
from app import video
from app import channel
from app import category

