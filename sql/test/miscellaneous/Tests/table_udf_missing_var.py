import sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def client(input):
    with process.client('sql', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
        out, err = c.communicate(input)
        sys.stdout.write(out)
        sys.stderr.write(err)

client('''
create function myfunc() returns table (x int) begin declare myvar int; return select myvar; end;
create function myfunc2() returns int begin declare myvar int; return myvar; end;
select * from myfunc();
select myfunc2();
''')
client('''
select * from myfunc();
select myfunc2();
drop function myfunc();
drop function myfunc2();
''')
