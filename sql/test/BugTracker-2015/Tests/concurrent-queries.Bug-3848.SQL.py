import threading, sys

try:
    from MonetDBtesting import process
except ImportError:
    import process

NITER = 1000

c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate('create table test3848 (col1 bigint, col2 varchar(1024));')
sys.stdout.write(out)
sys.stderr.write(err)
c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
c.stdin.write("copy 1000000 records into test3848 from stdin using delimiters ',', '\\n', '\"';")
for i in range(1000000):
    c.stdin.write('%d,"something old"\n' % i)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

class Client(threading.Thread):
    def __init__(self, query):
        self.query = query
        self.out = []
        self.err = []
        threading.Thread.__init__(self)

    def run(self):
        for i in range(NITER):
            c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
            out, err = c.communicate(self.query)
            # self.out.append(out)
            # self.err.append(err)

    def output(self):
        sys.stdout.write(''.join(self.out))
        sys.stderr.write(''.join(self.err))

rdr = Client('select * from test3848 where col1 % 1000 = 0;')
wtr = Client("start transaction; update test3848 set col2 = 'something new' where col1 % 1000 = 0; commit;")

rdr.start()
wtr.start()
rdr.join()
wtr.join()
rdr.output()
wtr.output()

c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate('drop table test3848;')
sys.stdout.write(out)
sys.stderr.write(err)
