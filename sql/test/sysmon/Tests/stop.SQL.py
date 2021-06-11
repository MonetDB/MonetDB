from MonetDBtesting.sqltest import SQLTestCase
import threading
from time import sleep
import sys

ERR = False

def mine_excepthook(args):
    global ERR
    ERR = True

# overide here because it ignores SystemExit
threading.excepthook = mine_excepthook

def run_slow_qry(qry):
    with SQLTestCase() as tc:
        tc.execute(qry)\
            .assertFailed(err_message="prematurely stopped client")

with SQLTestCase() as tc:
    tc.drop()
    tc.execute("create table foo(id bigserial, value int);").assertSucceeded()
    tc.execute("create table bar(id bigserial, value int);").assertSucceeded()
    tc.execute("insert into foo(value) (select * from generate_series(0,10000));").assertSucceeded()
    tc.execute("insert into bar(value) (select * from generate_series(0,10000));").assertSucceeded()
    qry = "select * from foo, bar;"
    t = threading.Thread(target=run_slow_qry, args=(qry,))
    t.start()
    sleep(1)
    # get qry tag
    tag = None
    tc.execute(f"select tag from sys.queue where query='{qry}';").assertSucceeded()
    data = tc.test_results[-1:].pop().data
    if len(data) > 0:
        tag = data.pop()[0]
    tc.execute(f"call sys.stop({tag});").assertSucceeded()
    t.join(timeout=3)
    if ERR:
        raise SystemExit(1)
    if t.is_alive():
        raise SystemExit("stop long running query failed!")

