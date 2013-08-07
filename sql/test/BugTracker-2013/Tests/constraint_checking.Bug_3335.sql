create table ary(x integer check(x >0 and x <2));
insert into ary values(1);
insert into ary values(0);
insert into ary values(2);
insert into ary values(-1);
insert into ary values(3);
drop table ary;
