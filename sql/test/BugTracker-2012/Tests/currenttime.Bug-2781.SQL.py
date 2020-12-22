import time, sys, os, pymonetdb

client1 = pymonetdb.connect(database=os.getenv("TSTDB"), port=int(os.getenv("MAPIPORT")))
cur1 = client1.cursor()
currenttime = time.strftime('%F %T', time.localtime(time.time() + time.timezone)) # I needd to add the current timezone to make it match on pymonetdb
#SQL command for checking the localtime
cur1.execute("select localtimestamp() between (timestamp '%s' - interval '20' second) and (timestamp '%s' + interval '20' second);" % (currenttime, currenttime))
if cur1.fetchall()[0][0] != True:
    sys.stderr.write('Expected True')
cur1.close()
client1.close()
