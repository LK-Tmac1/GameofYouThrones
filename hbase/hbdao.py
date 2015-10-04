import happybase
from utility.constant import HB_TB_MASTER

connection = happybase.Connection('localhost')    
connection.open()

def putUseractivityStat(dataDictMap):
    """
    :dataDictMap a map of dict; the key is the row key, the value is a dict, with
                 column qualifier as the key, and number of activity as the value
    """
    table = connection.table(HB_TB_MASTER)
    for dataKey, dataDict in dataDictMap.items():
        table.put(dataKey, dataDict)

def scanDataByRowPrefix(prefix, columnFamilyMember=[]):
    dataDictList = [{}]
    rows = connection.table(HB_TB_MASTER).scan(row_prefix=prefix, \
                columns=columnFamilyMember, sorted_columns=True)
    for r in rows:
        print r[0]
    return rows

        
# DsuxXH8Q76o
# print scanDataByRowPrefix('v_', ['userview_hourly_aggre'])
