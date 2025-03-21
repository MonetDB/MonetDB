from MonetDBtesting import tpymonetdb as pymonetdb
import sys, threading, os

query = '''
--select count(*) from tables;
create table t1(i int);
insert into t1 values(1);
insert into t1 values(2);
insert into t1 values(3);
insert into t1 values(4);
insert into t1 values(5);
insert into t1 values(6);
insert into t1 values(7);
insert into t1 values(8);
insert into t1 values(9);
insert into t1 values(0);
select * from t1;
--drop table t1;
'''

class Client(threading.Thread):

    def __init__(self, client):
        threading.Thread.__init__ (self)
        self.client = client
        self.dbh = pymonetdb.connect(port=int(os.getenv('MAPIPORT')),hostname=os.getenv('MAPIHOST'),database=os.getenv('TSTDB'))
        self.error = 0

    def run(self):
        cursor = self.dbh.cursor();
        try:
            cursor.execute(query)
            self.result = cursor.fetchall()
        except:
            self.error = 1
            pass

    def output(self):
        if self.error == 0 and self.result != [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (0,)]:
            print(f'received: {self.result}', file=sys.stderr)
            print('expected: [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (0,)]', file=sys.stderr)

def main():
        C = []
        C.append(Client(0))
        C.append(Client(1))
        C.append(Client(2))
        C.append(Client(3))
        C.append(Client(4))
        for t in C:
               t.start()
        for t in C:
               t.join()
        for t in C:
               t.output()

main()
