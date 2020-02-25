import os, sys, threading, pymonetdb

"""
Attempt to modify a merge table in a conflicting transaction. After the rollback, the table's contents must be cleaned
"""

class MergeTableWriter(threading.Thread):
    def __init__(self, barrier1, barrier2):
        self._wconn = pymonetdb.connect(port = int(os.getenv('MAPIPORT', '50000')), database = os.getenv('TSTDB', 'demo'), hostname = os.getenv('MAPIHOST', 'localhost'), autocommit=True)
        self._wcursor = self._wconn.cursor()
        self._barrier1 = barrier1
        self._barrier2 = barrier2
        threading.Thread.__init__(self)

    def run(self):
        i = 0
        while i < 3:
            self._wcursor.execute('START TRANSACTION;')
            barrier1.wait()
            self._wcursor.execute('ALTER TABLE "mt" ADD TABLE "part";')
            self._wcursor.execute('ALTER TABLE "mt" DROP TABLE "part";')
            barrier2.wait()
            self._wcursor.execute('ROLLBACK;')
            i += 1

conn = pymonetdb.connect(port = int(os.getenv('MAPIPORT', '50000')), database = os.getenv('TSTDB', 'demo'), hostname = os.getenv('MAPIHOST', 'localhost'), autocommit=True)
cursor = conn.cursor()
barrier1 = threading.Barrier(2)
barrier2 = threading.Barrier(2)

cursor.execute("""
START TRANSACTION;
DROP TABLE IF EXISTS "mt";
DROP TABLE IF EXISTS "part";
DROP TABLE IF EXISTS "dummy";
CREATE MERGE TABLE "mt"("col1" int);
CREATE TABLE "part"("col1" int);
COMMIT;
""")

mtwriter = MergeTableWriter(barrier1, barrier2)
mtwriter.start()

i = 0
while i < 3:
    barrier1.wait()
    cursor.execute('CREATE TABLE "dummy"(col1 int);')
    cursor.execute('DROP TABLE "dummy";')
    barrier2.wait()
    barrier1.reset()
    barrier2.reset()
    i += 1

mtwriter.join()

cursor.execute("""
START TRANSACTION;
DROP TABLE "mt";
DROP TABLE "part";
COMMIT;
""")
