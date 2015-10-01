from utility.constant import COMP_KEY_DELIMITER

def formatVideoId(videoid):
    return "v_" + videoid

def formatChannelId(channelid):
    return "ch_" + channelid

def formatCategoryId(categoryid):
    return 'ca_' + categoryid

def formatChannelVideoIdPair(videoid, channelid):
    return formatChannelId(channelid) + COMP_KEY_DELIMITER + formatVideoId(videoid)

def formatCategoryVideoIdPair(videoid, categoryid):
    return formatCategoryId(categoryid) + COMP_KEY_DELIMITER + formatVideoId(videoid)

def formatCategoryChannelIdPair(categoryid, channelid):
    return formatCategoryId(categoryid) + COMP_KEY_DELIMITER + formatChannelId(channelid)