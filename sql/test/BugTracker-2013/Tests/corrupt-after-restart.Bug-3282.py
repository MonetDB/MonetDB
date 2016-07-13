try:
    from MonetDBtesting import process
except ImportError:
    import process

import sys

s = process.server(stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate('''start transaction;
create table table3282 (i int);
insert into table3282 values (0);
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
select * from table3282 offset 2097140;
commit;
''')
sys.stdout.write(out)
sys.stderr.write(err)
out, err = s.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
s = process.server(stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate('''select * from table3282 offset 2097140;
''')
sys.stdout.write(out)
sys.stderr.write(err)
out, err = s.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
s = process.server(stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate('''select * from table3282 offset 2097140;
drop table table3282;
''')
sys.stdout.write(out)
sys.stderr.write(err)
out, err = s.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
