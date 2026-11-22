from MonetDBtesting import tpymonetdb as pymonetdb
import sys
import threading
import os

MAX_ITERATIONS = 1000
# EXPECTED_SUM = sum(range(0, MAX_ITERATIONS))
EXPECTED_SUM = MAX_ITERATIONS * (MAX_ITERATIONS - 1) // 2


class TestClient(threading.Thread):
    def __init__(self, cursor):
        threading.Thread.__init__(self)
        self._cursor = cursor

    def run(self):
        for i in range(0, MAX_ITERATIONS):
            try:
                self._cursor.execute("INSERT INTO t VALUES (%d,%d);" % (i, i))
            except pymonetdb.exceptions.DatabaseError:
                pass


xit = 0                         # exit code

with pymonetdb.connect(port=int(os.getenv('MAPIPORT', '50000')),
                       database=os.getenv('TSTDB', 'demo'),
                       hostname=os.getenv('MAPIHOST', 'localhost'),
                       autocommit=True) as client1, \
     pymonetdb.connect(port=int(os.getenv('MAPIPORT', '50000')),
                       database=os.getenv('TSTDB', 'demo'),
                       hostname=os.getenv('MAPIHOST', 'localhost'),
                       autocommit=True) as client2, \
     client1.cursor() as cursor1, \
     client2.cursor() as cursor2:

    cursor1.execute("DROP TABLE if exists T;")
    cursor1.execute("CREATE TABLE T (k INT PRIMARY KEY, v INT);")

    thread1 = TestClient(cursor1)
    thread2 = TestClient(cursor2)
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()

    cursor1.execute("SELECT COUNT(*), COUNT(DISTINCT v), SUM(k) from T;")
    res = cursor1.fetchall()
    if res != [(MAX_ITERATIONS, MAX_ITERATIONS, EXPECTED_SUM)]:
        xit = 1
        print(f"{[(MAX_ITERATIONS, MAX_ITERATIONS, EXPECTED_SUM)]} expected,"
              f" {res} received", file=sys.stderr)
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


sys.exit(xit)
