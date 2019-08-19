import sys

try:
    from MonetDBtesting import process
except ImportError:
    import process


def client(input):
    c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
    out, err = c.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

def server_stop(s):
    out, err = s.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

s = process.server(args = [], stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
client('''\
create table t (a int, b int, c int);\
alter table t add unique (b);
''')
server_stop(s)

s = process.server(args = [], stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
client('alter table t drop column c;')
server_stop(s)

s = process.server(args = [], stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
client('alter table t drop column b;')
server_stop(s)

s = process.server(args = [], stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
client('alter table t drop column b cascade;')
server_stop(s)

s = process.server(args = [], stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
client('drop table t;')
server_stop(s)
