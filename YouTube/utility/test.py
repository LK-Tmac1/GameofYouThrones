list1 = [1, 1, 2, 3, 4, 2]
list2 = [5, 7, 9, 9]
vIdSet = set([])
s1 = set(list1)
s2 = set(list2)
print s2
    
def addSet(set1, data):
    set1.add(data)

addSet(s2, 10)
addSet(s2, 100)

print s2