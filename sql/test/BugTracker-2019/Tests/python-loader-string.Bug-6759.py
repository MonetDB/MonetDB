import sys, os, pymonetdb

db = os.getenv("TSTDB")
port = int(os.getenv("MAPIPORT"))

client1 = pymonetdb.connect(database=db, port=port, autocommit=True)
cur1 = client1.cursor()
cur1.execute('''
CREATE LOADER json_loader() LANGUAGE PYTHON {
    import json
    _emit.emit(json.loads('{"col1": ["apple", "peer"], "col2": ["orange", "banana nananana"]}'))
};
CREATE TABLE tbl FROM LOADER json_loader();
SELECT * FROM tbl;
''')
if cur1.fetchall() != [('apple','orange'),('peer','banana nananana')]:
    sys.stderr.write('Expected result: [(\'apple\',\'orange\'),(\'peer\',\'banana nananana\')]')

cur1.execute('''
DROP TABLE tbl;
DROP LOADER json_loader;
''')
cur1.close()
client1.close()
