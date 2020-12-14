import os, sys, zipfile, pymonetdb

TSTTRGDIR = os.environ['TSTTRGDIR']

archive = 'utf8bom.Bug-3436.zip'

def mkpardir(path):
    i = path.find('/')
    d = ''
    while i >= 0:
        d = os.path.join(d, path[:i])
        if not os.path.exists(d):
            os.mkdir(d)
        path = path[i + 1:]
        i = path.find('/')

z = zipfile.ZipFile(archive)
for name in z.namelist():
    mkpardir(name)
    data = z.read(name)
    f = open(name, 'wb')
    f.write(data)
    f.close()

dbh = pymonetdb.connect(port=int(os.getenv('MAPIPORT')),hostname=os.getenv('MAPIHOST'),database=os.getenv('TSTDB'), autocommit=True)
cursor = dbh.cursor()

cursor.execute('''
start transaction;
create table utf8bom (city string,id integer);
''')
if cursor.execute("copy into utf8bom from r'%s' using delimiters ',',E'\\n','\"';" % os.path.join(TSTTRGDIR, 'utf8bom.csv')) != 2:
    sys.stderr.write("Expected 2 rows inserted")
cursor.execute("select * from utf8bom order by id;")
if cursor.fetchall() != [('Montréal', 1621), ('New York', 8392)]:
    sys.stderr.write("Expected [('Montréal', 1621), ('New York', 8392)]")
cursor.execute("rollback")

cursor.close()
dbh.close()
