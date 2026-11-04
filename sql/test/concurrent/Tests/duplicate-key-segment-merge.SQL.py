import os
import sys
import threading
from MonetDBtesting import tpymonetdb as pymonetdb

nr_clients = 4
nr_rounds = 10000

host = os.getenv('MAPIHOST')
port = int(os.getenv('MAPIPORT'))
db = os.getenv('TSTDB')

main_conn = pymonetdb.connect(hostname=host, port=port, database=db, autocommit=True)
main_cur = main_conn.cursor()
main_cur.execute("drop table if exists duplicate_key_test;")
main_cur.execute("create table duplicate_key_test (k int primary key, v int);")

conns = [pymonetdb.connect(hostname=host, port=port, database=db, autocommit=True)
         for _ in range(nr_clients)]
cursors = [c.cursor() for c in conns]

barrier = threading.Barrier(nr_clients + 1)
stop = threading.Event()
errors = []
errors_lock = threading.Lock()


def client(idx):
    cur = cursors[idx]
    r = 0
    try:
        while True:
            barrier.wait()
            if stop.is_set():
                return
            r += 1
            try:
                cur.execute("insert into duplicate_key_test values (%d, %d);" % (r, r))
            except pymonetdb.DatabaseError:
                pass
            barrier.wait()
    except threading.BrokenBarrierError:
        return
    except BaseException as e:
        with errors_lock:
            errors.append(e)
        stop.set()
        barrier.abort()


threads = [threading.Thread(target=client, args=(i,)) for i in range(nr_clients)]
for t in threads:
    t.start()

try:
    for r in range(1, nr_rounds + 1):
        barrier.wait()
        barrier.wait()
        main_cur.execute("select count(*), count(distinct v), sum(k) from duplicate_key_test where k=%d;" % r)
        result = main_cur.fetchall()[0]
        if result != (1, 1, r):
            main_cur.execute("select * from duplicate_key_test where k=%d;" % r)
            rows = main_cur.fetchall()
            print("round %d: expected (1, 1, %d), got %s, rows=%s" % (r, r, result, rows),
                  file=sys.stderr)
            sys.exit(1)
except threading.BrokenBarrierError:
    with errors_lock:
        err = errors[0] if errors else None
    print("concurrent client failed: %r" % (err,), file=sys.stderr)
    sys.exit(1)
finally:
    stop.set()
    barrier.abort()
    for t in threads:
        t.join()
    for c in conns:
        c.close()
    main_conn.close()
