
a = 'cat ~/.ssh/id_rsa.pub | ssh -o "StrictHostKeyChecking no" -i ~/.ssh/*.pem ubuntu@'
b = " 'cat >> ~/.ssh/authorized_keys'"

listDN = [
"ec2-54-174-174-113.compute-1.amazonaws.com",
"ec2-54-175-136-253.compute-1.amazonaws.com",
"ec2-54-175-95-231.compute-1.amazonaws.com",
"ec2-54-175-139-43.compute-1.amazonaws.com"
]

listIP = [
"172-31-16-238",
"172-31-16-237",
"172-31-16-235",
"172-31-16-236"
]


cmd1 = """<property>
   <name>mapreduce.jobtracker.address</name>
   <value>"""
cmd2 = """":54311</value>
</property>"""
s = ""
def zookeeperclientport():
    for i in xrange(1, len(listDN) + 1):
        print listDN[i - 1] 

def kafkaclientport():
    s = ""
    for i in xrange(0, len(listDN) - 1):
        s = s + listDN[i] + ":2181,"
    s = s + listDN[len(listDN) - 1] + ":2181"
    print s
    
kafkaclientport()
