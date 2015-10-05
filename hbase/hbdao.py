import happybase
from utility.constant import HB_TB_MASTER

connection = happybase.Connection('localhost')    
connection.open()

def putUseractivityStat(dataTupleList):
    """
    :dataTupleList a list of tuple; in each tuple, the first element is the row key,
                   the second element is another tuple, where the first element is the
                   column qualifer, say 'userview_hourly:2015-09-30T16:30', and the value
                   is the number of activity, say '2'
    """
    table = connection.table(HB_TB_MASTER)
    for dataTuple in dataTupleList:
        rowKey = dataTuple[0]
        dataDict = {}
        for data in dataTuple[1]:
            dataDict[data[0]] = data[1]
        table.put(rowKey, dataDict)
        print rowKey

def scanDataByRowPrefix(prefix, columnFamilyMember=[]):
    return connection.table(HB_TB_MASTER).scan(row_prefix=prefix, \
                columns=columnFamilyMember, sorted_columns=True)
        # each row is a tuple, where the first element is the row key
        # and the second is an OrderedDict/Tuple of column:value
        # Example: [('userview_hourly:2015-07-01T00:00', '4'), ('userview_hourly:2015-07-01T00:30', '8')...]

"""
# video_K_mpQtzFjC4
# video_GyKimbTbHjo
# channel_UCWLQqXdXHqzoJqu22QgMIlA:video_ODOROmPUXWQ
# category_26:video_kZaf7D8SnnI
# category_2:video_bVHEq1nccNQ
# category_2:video_5cvdtUqN_RY
rows = scanDataByRowPrefix('video_K_mpQtzFjC4', ['userview_daily:2015-07-01',
                                                'userview_daily:2015-10-01',
                                                'userview_daily:2015-01-01'])
for row in rows:
    for i in row:
        print i                                                
print "====="
"""