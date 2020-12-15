import os, sys, shutil, pymonetdb

src = os.environ['RELSRCDIR']
dst = os.environ['TSTTRGDIR']
shutil.copyfile(os.path.join(src,'n_nationkey.sorted'), os.path.join(dst,'n_nationkey.sorted'))
shutil.copyfile(os.path.join(src,'n_regionkey.sorted'), os.path.join(dst,'n_regionkey.sorted'))

port = int(os.environ['MAPIPORT'])
db = os.environ['TSTDB']
cli = pymonetdb.connect(port=port,database=db,autocommit=True)
cur = cli.cursor()
try:
    cur.execute('''
    start transaction;
    create table bug (n_nationkey INTEGER,n_regionkey INTEGER);
    ''')
    cur.execute('copy binary into bug from E\'{}\', E\'{}\';\n'.format(
            os.path.join(dst, 'n_nationkey.sorted').replace('\\', '\\\\'),
            os.path.join(dst, 'n_regionkey.sorted').replace('\\', '\\\\')))
    sys.stderr.write('Exception expected')
except pymonetdb.exceptions.DatabaseError as e:
    if 'Binary files for table \'bug\' have inconsistent counts' not in str(e):
         sys.stderr.write('Exception: Binary files for table \'bug\' have inconsistent counts expected')
cur.close()
cli.close()
