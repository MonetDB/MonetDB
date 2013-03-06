start transaction;

create table tbl_a (id int primary key,
c_id int,
str1 varchar(128),
str2 varchar(128),
str3 varchar(128),
int1 int,
str4 varchar(128));

create table tbl_b (id int primary key,
int1 int);

create table tbl_c (id int primary key,
str1 varchar(128),
str2 varchar(128),
str3 varchar(128));

create table tbl_d (a_id int, str1 varchar(128));

create view view_a as
select a.*,b.int1 as bint1,d.str1 as dstr1
from tbl_a a
inner join tbl_b b on a.id=b.id
inner join tbl_d d on a.id=d.a_id;

create view view_b as
select view_a.*
from view_a
inner join tbl_c on view_a.c_id=tbl_c.id
where 1=1
and view_a.str1 = 'foo'
and view_a.str2 = 'bar'
and view_a.str3 = 'baz'
and tbl_c.str1 <> 'foobar'
and tbl_c.str2 = 'foobarbaz'
and tbl_c.str3 is null;

create view view_c as
select * from view_b
where 1=1
and view_b.int1 = 0;


create view view_d as
select * from view_c
where str4 < 'zzzzz';

select * from view_d order by id;

rollback;
