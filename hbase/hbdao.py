import happybase

connection = happybase.Connection('localhost')

connection.open()
print connection.tables()
