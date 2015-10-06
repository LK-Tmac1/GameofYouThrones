# GameofYouThrones

##Story
Like many users, I often watch videos on YouTube, subscribe to some channels, and sometimes get subscription emails. It seems those channel owners can make money from not only ads, but also subscribers: http://bit.ly/1M8xzcD, http://bit.ly/1KWZYA4.

So I was curious if there was a way that could help channel owners to know the popularity of their videos and channels by user activity on YouTube: there are 8000~ channels, 400~ millions videos, and 1~ billion users, so you can imagine how many user activities are there each day, and it will be technically challenging and analytically interesting.

##Pipeline

![Pipeline](image/pipeline.jpg)

The data of user activity from YouTube will be classified by Kafka, a distributed messenger, based on different types of user activties, say a user viewed a video on a channel, or a user subscribed to a channel from a video. The message will then be stored on HDFS, then transformed by Spark, and denormalized into HBase as a NoSQL schema. Finally, the Flask web framework will handle the front end query jobs.

One thing should be mentioned is that, although YouTube provides data of videos and channels by their API, user activity is private. So I had to generate such data by myself, and the size is roughly 100~200 GB level.

##Chanllenges
The biggest chanllenge in this project, is more than programming, i.e. how to use technologies: know which technology should be used to resolve a certain problem and why should we use, is more important.




