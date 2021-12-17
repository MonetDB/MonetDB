import pymonetdb, sys, threading, os

client1 = pymonetdb.connect(port = int(os.getenv('MAPIPORT', '50000')), database = os.getenv('TSTDB', 'demo'), hostname = os.getenv('MAPIHOST', 'localhost'), autocommit=True)
client2 = pymonetdb.connect(port = int(os.getenv('MAPIPORT', '50000')), database = os.getenv('TSTDB', 'demo'), hostname = os.getenv('MAPIHOST', 'localhost'), autocommit=True)
cursor1 = client1.cursor()
cursor2 = client2.cursor()

MAX_ITERATIONS = 1000
EXPECTED_SUM = sum(range(0, MAX_ITERATIONS))

cursor1.execute("CREATE TABLE T (k INT PRIMARY KEY, v INT);")

class TestClient(threading.Thread):

    def __init__(self, cursor):
        threading.Thread.__init__ (self)
        self._cursor = cursor

    def run(self):
        for i in range(0, MAX_ITERATIONS):
            try:
                self._cursor.execute("INSERT INTO t VALUES (%d,%d);" % (i, i))
            except pymonetdb.exceptions.IntegrityError:
                pass


thread1 = TestClient(cursor1)
thread2 = TestClient(cursor2)
thread1.start()
thread2.start()
thread1.join()
thread2.join()

cursor1.execute("SELECT COUNT(*), COUNT(DISTINCT k), SUM(k) from T;")
if cursor1.fetchall() != [(MAX_ITERATIONS, MAX_ITERATIONS, EXPECTED_SUM)]:
    sys.stderr.write("[(%d,%d,%d)] expected" % (MAX_ITERATIONS, MAX_ITERATIONS, EXPECTED_SUM))
cursor1.execute("DROP TABLE T;")

cursor1.close()
cursor2.close()
client1.close()
client2.close()
