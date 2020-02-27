import sys
import os
try:
    from MonetDBtesting import process
except ImportError:
    import process

def client(input):
    c = process.client('sql',
                         stdin = process.PIPE,
                         stdout = process.PIPE,
                         stderr = process.PIPE)
    out, err = c.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

script1 = '''\
create table tab1 (group_by_col int, index_col int, f float);
create table tab2 (index_col int, f float);

insert into tab1 values (1,1,1),(1,2,2),(2,1,3),(2,2,4),(3,1,5),(3,2,6);
insert into tab2 values (1,111),(2,222),(3,333),(4,444);

set optimizer='default_pipe';
select optimizer;
select tab1.group_by_col,SUM(fuse(cast (tab1.f as INT),cast (tab2.f as INT))) from tab2 inner join tab1 on tab1.index_col = tab2.index_col group by tab1.group_by_col;

drop table tab1;
drop table tab2;

'''

def main():
    s = process.server(args = ["--set", "gdk_nr_threads=2", "--forcemito"],
                       stdin = process.PIPE,
                       stdout = process.PIPE,
                       stderr = process.PIPE)
    client(script1)
    out, err = s.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

if __name__ == '__main__':
    main()
