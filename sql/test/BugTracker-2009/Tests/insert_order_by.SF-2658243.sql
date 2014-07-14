
create table unsorted (n int);
insert into unsorted values (2);
insert into unsorted values (1);
insert into unsorted values (4);
insert into unsorted values (3);
insert into unsorted values (5);

create table sorted (n int);

insert into sorted
select * from unsorted
order by n;

select * from unsorted;
select * from sorted;

drop table sorted;
drop table unsorted;
