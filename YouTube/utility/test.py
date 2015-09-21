from datetime import date, timedelta as td

def parseDateString(dateStr):
    year = int(dateStr[0:dateStr.find('-')])
    month = int(dateStr[dateStr.find('-') + 1:dateStr.rfind('-')])
    day = int(dateStr[dateStr.rfind('-') + 1:dateStr.find('T')])
    return date(year, month, day)

def getDateRangeList(dateStr1, dateStr2):
    date1 = parseDateString(dateStr1)
    date2 = parseDateString(dateStr2)
    if date1 > date2:
        date3 = date1
        date1 = date2
        date2 = date3
    delta = date2 - date1
    dateList = []
    for i in range(delta.days + 1):
        dateList.append(str(date1 + td(days=i)))
    return dateList

print getDateRangeList("2008-10-13T12:00:02.0Z", "2008-09-15T12:00:02.0Z")
