
def jsonifyVideo(videoList, dataList):
    # assume video list and data list is in the same length 
    for i in xrange(0, len(dataList)):
        for j in xrange(0, len(dataList[i])):
            dataList[i][j] = int(dataList[i][j])
    dataDictList = []
    for i in xrange(0, len(videoList)):
        dataDict = {}
        dataDict['name'] = str(videoList[i].encode('utf-8'))
        dataDict['data'] = dataList[i]
        dataDictList.append(dataDict)
    return dataDictList
