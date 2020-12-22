import sys, os, platform, pymonetdb

db = os.getenv("TSTDB")
port = int(os.getenv("MAPIPORT"))

conn1 = pymonetdb.connect(database=db, port=port, autocommit=True, username='monetdb', password='monetdb')
cur1 = conn1.cursor()
running_OS = platform.system()

try:
    cur1.execute("SELECT timestamp_to_str(current_timestamp, '%Q');")
    if running_OS == 'Windows':
        sys.stderr.write("Exception expected")
    elif running_OS == 'Darwin':
        if cur1.fetchall() != [('Q',)]:
            sys.stderr.write("Expected [('Q',)]")
    else:
        if cur1.fetchall() != [('%Q',)]:
            sys.stderr.write("Expected [('%Q',)]")
except pymonetdb.DatabaseError as e:
    if running_OS == 'Windows':
        if "cannot convert timestamp" not in str(e):
            sys.stderr.write('Wrong error %s, expected cannot convert timestamp' % (str(e)))
    else:
        raise e

cur1.close()
conn1.close()
