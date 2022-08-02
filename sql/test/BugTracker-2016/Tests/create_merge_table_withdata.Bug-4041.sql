
create table other_table (i1 int, i2 int);
insert into other_table values(1,2);

create merge table mt as select * from other_table limit 1;

create merge table mt as select * from other_table limit 1 with no data;
drop table mt;;
drop table other_table;;
