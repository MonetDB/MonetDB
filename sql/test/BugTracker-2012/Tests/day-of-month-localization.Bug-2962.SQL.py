import time, sys, os, pymonetdb

client1 = pymonetdb.connect(database=os.getenv("TSTDB"), port=int(os.getenv("MAPIPORT")))
cur1 = client1.cursor()
cur1.execute("set time zone interval -'%d:00' hour to minute;\n" % (time.gmtime().tm_hour+1))
cur1.execute("SELECT EXTRACT(DAY FROM CURRENT_TIMESTAMP) <> %d;\n" % (time.gmtime().tm_mday))
if cur1.fetchall()[0][0] != True:
    sys.stderr.write('Expected True')
cur1.close()
client1.close()
