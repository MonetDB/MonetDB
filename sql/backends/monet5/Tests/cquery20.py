import sys

try:
    from MonetDBtesting import process
except ImportError:
    import process

def server_stop(s):
    out, err = s.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

def client(input):
    c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
    out, err = c.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

script1 = '''\
create temporary stream table sta (a int);\
select count(*) from streams;\
create stream table stb (a int);\
select count(*) from streams;
'''

script2 = '''\
select count(*) from streams;\
'''

script3 = '''\
select count(*) from streams;\
drop table stb;\
select count(*) from streams;
'''

def main():
    s = process.server(args = [], stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
    client(script1)
    client(script2)
    server_stop(s)
    s = process.server(args = [], stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
    client(script3)
    server_stop(s)

if __name__ == '__main__':
    main()
