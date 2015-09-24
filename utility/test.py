from utility.constant import TOPIC_USER_ACTIVITY_LIST
from random import randint
for x in range(0, 1000):
    a = randint(0, len(TOPIC_USER_ACTIVITY_LIST))
    print a
print "======="