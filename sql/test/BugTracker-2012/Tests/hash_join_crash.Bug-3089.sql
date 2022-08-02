
create table test1 ( id int, seq int, name varchar(20), primary key (id, seq));
create table test2 ( id int not null, seq int not null, name varchar(20));

select * from test1 a, test2 b where a.id = b.id and b.seq = a.seq;

drop table test2;
drop table test1;
