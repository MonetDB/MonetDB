from MonetDBtesting import tpymonetdb as pymonetdb
import sys, threading, os

client1 = pymonetdb.connect(port = int(os.getenv('MAPIPORT', '50000')), database = os.getenv('TSTDB', 'demo'), hostname = os.getenv('MAPIHOST', 'localhost'), autocommit=True)
client2 = pymonetdb.connect(port = int(os.getenv('MAPIPORT', '50000')), database = os.getenv('TSTDB', 'demo'), hostname = os.getenv('MAPIHOST', 'localhost'), autocommit=True)
cursor1 = client1.cursor()
cursor2 = client2.cursor()

MAX_ITERATIONS = 1000
EXPECTED_SUM = sum(range(0, MAX_ITERATIONS))

cursor1.execute("DROP TABLE if exists T;")
cursor1.execute("CREATE TABLE T (k INT PRIMARY KEY, v INT);")

class TestClient(threading.Thread):

    def __init__(self, cursor):
        threading.Thread.__init__ (self)
        self._cursor = cursor

    def run(self):
        for i in range(0, MAX_ITERATIONS):
            try:
                self._cursor.execute("INSERT INTO t VALUES (%d,%d);" % (i, i))
            except pymonetdb.exceptions.DatabaseError:
                pass


thread1 = TestClient(cursor1)
thread2 = TestClient(cursor2)
thread1.start()
thread2.start()
thread1.join()
thread2.join()

cursor1.execute("SELECT COUNT(*), COUNT(DISTINCT v), SUM(k) from T;")
res = cursor1.fetchall()
if res != [(MAX_ITERATIONS, MAX_ITERATIONS, EXPECTED_SUM)]:
    print("[(%d,%d,%d)] expected, [(%d,%d,%d)] received" % (MAX_ITERATIONS, MAX_ITERATIONS, EXPECTED_SUM, res[0][0], res[0][1], res[0][2]), file=sys.stderr)
    cursor1.execute("SELECT * FROM T;")
    res = cursor1.fetchall()
    s = set()
    for t in res:
        if t[0] in s:
            print(f"duplicate entry {t}", file=sys.stderr)
        else:
            s.add(t[0])
    for i in range(MAX_ITERATIONS):
        if i not in s:
            print(f"entry {i} missing", file=sys.stderr)


cursor1.close()
cursor2.close()
client1.close()
client2.close()
