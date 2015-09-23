#!/usr/bin/python

from api.category import saveAllCategory
from api.channel import saveAllChannelByCategory
from api.playlist import saveAllPlaylistByChannel

saveAllCategory()
print "___________________saveAllCategory"
saveAllChannelByCategory()
print "___________________saveAllChannelByCategory"
saveAllPlaylistByChannel()
print "___________________saveAllPlaylistByChannel"


