listDN = [
"ec2-54-164-77-150.compute-1.amazonaws.com",
"ec2-54-85-139-12.compute-1.amazonaws.com",
"ec2-54-88-56-16.compute-1.amazonaws.com",
"ec2-54-85-19-171.compute-1.amazonaws.com"

]

a = 'cat ~/.ssh/id_rsa.pub | ssh -o "StrictHostKeyChecking no" -i ~/.ssh/*.pem ubuntu@'
b = " 'cat >> ~/.ssh/authorized_keys'"

listIP = [
"172-31-28-154",
"172-31-28-155",
"172-31-28-153",
"172-31-28-156"
]

cmd1 = """<property>
   <name>mapreduce.jobtracker.address</name>
   <value>"""
cmd2 = """:54311</value>
</property>"""

for i in xrange(0, len(listDN)):
    print "ip-" + listIP[i]

