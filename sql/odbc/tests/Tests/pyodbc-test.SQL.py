import pyodbc

cnxn = pyodbc.connect('DSN=MonetDB-Test')
crs = cnxn.cursor()
crs.executemany("""select *, cast(? as string), cast(? as integer) from sys._tables;""", [ ('test', 1), ('test2', 2) ])
data = crs.fetchall()
