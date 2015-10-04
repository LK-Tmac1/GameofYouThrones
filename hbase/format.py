from utility.constant import COMP_KEY_DELIMITER, HB_CATEGORY_PREFIX \
HB_CHANNEL_PREFIX, HB_VIDEO_PREFIX


def formatVideoId(videoid):
    return HB_VIDEO_PREFIX + videoid

def formatChannelId(channelid):
    return HB_CHANNEL_PREFIX + channelid

def formatCategoryId(categoryid):
    return HB_CATEGORY_PREFIX + categoryid

def formatChannelVideoIdPair(videoid, channelid):
    return formatChannelId(channelid) + COMP_KEY_DELIMITER + formatVideoId(videoid)

def formatCategoryVideoIdPair(videoid, categoryid):
    return formatCategoryId(categoryid) + COMP_KEY_DELIMITER + formatVideoId(videoid)

def formatCategoryChannelIdPair(categoryid, channelid):
    return formatCategoryId(categoryid) + COMP_KEY_DELIMITER + formatChannelId(channelid)