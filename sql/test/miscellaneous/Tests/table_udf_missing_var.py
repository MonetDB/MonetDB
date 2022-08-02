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
declare myvar int;
create function myfunc() returns table (x int) begin return select myvar; end;
create function myfunc2() returns int begin return myvar; end;
select * from myfunc();
select myfunc2();
''')
client('''
select * from myfunc(); --error, myvar doesn\'t exist
select myfunc2(); --error, myvar doesn\'t exist
drop function myfunc();
drop function myfunc2();
''')
