from datetime import date, datetime, timedelta as td

def getTimestampNow():
    strTimestamp = str(datetime.now()).replace(' ', 'T')
    return strTimestamp[0:strTimestamp.rfind(".")] + "Z"

channel = [()]
print channel[0][0]
    
