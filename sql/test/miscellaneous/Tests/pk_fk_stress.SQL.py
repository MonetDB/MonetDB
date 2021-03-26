import pymonetdb, sys, threading, os

client1 = pymonetdb.connect(port = int(os.getenv('MAPIPORT', '50000')), database = os.getenv('TSTDB', 'demo'), hostname = os.getenv('MAPIHOST', 'localhost'), autocommit=True)
client2 = pymonetdb.connect(port = int(os.getenv('MAPIPORT', '50000')), database = os.getenv('TSTDB', 'demo'), hostname = os.getenv('MAPIHOST', 'localhost'), autocommit=True)
cursor1 = client1.cursor()
cursor2 = client2.cursor()
barrier1 = threading.Barrier(3)
barrier2 = threading.Barrier(3)

MAX_ITERATIONS = 100
vals = [[(1,)],[(3,), (4,)],[(7,)],[(10,)],[(13,)],[(16,)],[(19,)],[(22,)],[(25,)],[(28,)],[(31,)],[(34,)],[(37,)],[(40,)],[(43,)],[(46,)],[(49,)],[(52,)],[(55,)],
        [(58,)],[(61,)],[(64,)],[(67,)], [(70,)],[(73,)]]

cursor1.execute("""
START TRANSACTION;
CREATE TABLE tbl1 (col1 INT AUTO_INCREMENT PRIMARY KEY);
CREATE TABLE tbl2 (col2 INT AUTO_INCREMENT PRIMARY KEY, FOREIGN KEY (col2) REFERENCES tbl1(col1));
COMMIT;
""")

class TestClient1(threading.Thread):

    def __init__(self, cursor, barrier1, barrier2):
        threading.Thread.__init__ (self)
        self._cursor = cursor
        self._barrier1 = barrier1
        self._barrier2 = barrier2

    def run(self):
        i = 0
        vals_i = 0

        while i < MAX_ITERATIONS:
            next_action = i % 4

            if next_action == 0:
                self._cursor.execute('INSERT INTO tbl1;')
            elif next_action == 1:
                self._cursor.execute('SELECT col1 FROM tbl1 ORDER BY col1;')
                a = self._cursor.fetchall()
                if a != vals[vals_i]:
                    sys.stderr.write("Error on TestClient1, expected %s got %s\n" % (str(vals[vals_i]), str(a)))
                vals_i += 1
            elif next_action == 2:
                self._cursor.execute("""
                START TRANSACTION;
                INSERT INTO tbl1;
                INSERT INTO tbl2;
                COMMIT;
                """)
            elif next_action == 3:
                self._cursor.execute("DELETE FROM tbl2 WHERE col2 < %d;" % (i))
            self._barrier1.wait()
            self._barrier2.wait()
            i += 1


class TestClient2(threading.Thread):

    def __init__(self, cursor, barrier1, barrier2):
        threading.Thread.__init__ (self)
        self._cursor = cursor
        self._barrier1 = barrier1
        self._barrier2 = barrier2

    def run(self):
        i = 0
        vals_i = 0

        while i < MAX_ITERATIONS:
            next_action = i % 4

            self._barrier1.wait()
            if next_action == 0:
                self._cursor.execute('INSERT INTO tbl2;')
            elif next_action == 1:
                self._cursor.execute('SELECT col2 FROM tbl2 ORDER BY col2;')
                a = self._cursor.fetchall()
                if a != vals[vals_i]:
                    sys.stderr.write("Error on TestClient2, expected %s got %s\n" % (str(vals[vals_i]), str(a)))
                vals_i += 1
            elif next_action == 2:
                self._cursor.execute("""
                START TRANSACTION;
                INSERT INTO tbl1;
                INSERT INTO tbl2;
                COMMIT;
                """)
            elif next_action == 3:
                self._cursor.execute("DELETE FROM tbl1 WHERE col1 < %d;" % (i))
            self._barrier2.wait()
            i += 1

thread1 = TestClient1(cursor1, barrier1, barrier2)
thread2 = TestClient2(cursor2, barrier1, barrier2)
thread1.start()
thread2.start()

i = 0
while i < MAX_ITERATIONS:
    barrier1.wait()
    barrier1.reset()
    barrier2.wait()
    barrier2.reset()
    i += 1

thread1.join()
thread2.join()

cursor1.execute("""
START TRANSACTION;
DROP TABLE IF EXISTS tbl2;
DROP TABLE IF EXISTS tbl1;
COMMIT;
""")

cursor1.close()
cursor2.close()
client1.close()
client2.close()
