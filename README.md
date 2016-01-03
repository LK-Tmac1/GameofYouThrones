# GameofYouThrones

##About Insight
This program is a 7 weeks intensive project based training program that helps engineers to prepare their career as data engineers. More details can be found here: www.insightdataengineering.com

##Story
Like many users, I often watch videos on YouTube, subscribe to some channels, and sometimes get subscription emails. It seems those channel owners can make money from not only ads, but also subscribers: http://bit.ly/1M8xzcD, http://bit.ly/1KWZYA4.

So my project's goal was to build an analytical tool that could help channel owners to know the trends of their videos and channels based on user activities. There are 8000~ channels, 400~ millions videos, 1~ billion users, and unknown numbers of user activities each day. Thus, for me, this project was not only analytically interesting but also technically challenging.

##Dashboard
Here are two more concrete questions, i.e. queries that could be answered by my project:

#####By a given time span, which are the top X videos of channel Y that had the most time of views from users?

![Query](image/query1-1.jpg)

![Query](image/query1-2.jpg)

#####By a given time span, how many subscribers a given video X had driven for the channel it belonged to?

![Query](image/query2-1.jpg)


##Pipeline

![Pipeline](image/pipeline.jpg)

The data of user activity from YouTube will be classified by Kafka, a distributed messenger, based on different types of user activties, say a user viewed a video on a channel, or a user subscribed to a channel from a video. The message will then be transformed by Spark, and denormalized into HBase as a NoSQL schema. Finally, the Flask web framework will handle the front end query jobs. All those were deployed on AWS EC2 instances, with one master node, and three worker nodes.

One thing should be mentioned is that, although YouTube provides data of videos and channels by their API, user activity is private. So I had to generate such data by myself, and the size is roughly 100~200 GB level.

##Chanllenges
The biggest chanllenge in this project, was to resolve the problem of scalability. For instance, for each video, there will be many new data regarding the activites assoicated with it: as there are 24 hours, and the lowest granuality of each activity is on hourly basis, there will be 25 more data regarding a given activitiy of a single video (24 hours plus 1 day); thus, if there are millions of videos, more than 5 types of activities of each video, the growth of data is significant.

In this case, it will not be a good idea to store the data in a RDBMS way. But instead of growing the table in a "taller" way, NoSQL can resolve the scalability issue by a "wider" way.

####HBase columns

Compared to RDBMS, NoSQL is schema free. When the data size is scaling up, NoSQL will be like a wide table, while RDBMS will be a tall table. Also, HBase is a columnar database, making queries on columns lower latency than RDBMS.

Motivated by this benefit, in my HBase, for each row, the predefined column families are the combination of user activity and statistics basis, say user view on daily or hourly basis, or user comment on daily but accumulative sum basis. The column member of each family will be the date or date time if hourly basis of that activity, and the value will be the number of activities happended on that date/time. The trade off is that the accumulative sum needs to be precalculated, making the data transformation more complicated. (See Spark batch)

![Query](image/hbase-columns.jpg)

####HBase rows

Since in NoSQL, there is no foreign key reference, what if we want to know the statictics of videos that belong to a given channel? 

Solution: All rows are sorted automatically on HBase, and there is a function called "Scan" that could be used to return only a range of rows, say all rows begin with a prefix. Thus, by making the row key consisted of channelid_videoid, similar to the idea of "composite key", when we want to know all the videos assoicated with a channel, we could use the scan method to scan all rows begin that "channelid". The trade off is that there will be duplication of data, as now for each video, it will be duplicated twice.

![Query](image/hbase-rows.jpg)

####Spark batch

To denormalize the table row key and precalculate the accumulative sum statistics, Spark batch transform the raw data of user activity by the word-count-like method, i.e. mapping data to key value pair, and then reducing, sorting and grouping by keys. The most complicated part was to reduce the complexity of the program. For instance, the hourly (or half-hourly) basis data will be calculated first, then the accumulative sum of each half-/hour, then the daily basis, and finally the daily accumulative sum of each day. By doing so, the amount of work could be optimized.
 
![Query](image/spark-batch.jpg)

##Conclusion

There are many tools for big data, but an awesome data engineer should know which to be used to resolve the problem, why should be used, the trade off and like forth. Building a pipeline system is not easy, and thus challenging and exciting.
