-- A table with unsorted values in a and b
create table unsorted (a int,b int);
insert into unsorted values (2, 3);
insert into unsorted values (1, 2);
insert into unsorted values (4, 1);
insert into unsorted values (3, 2);
insert into unsorted values (2, 3);
insert into unsorted values (3, 3);
insert into unsorted values (3, 1);
insert into unsorted values (4, 3);

-- Store it in a new table with tuples sorted on a,b
create table sorted (a int, b int);
insert into sorted
select * from unsorted
order by a,b;

select * from sorted;

-- these tho are semantically equivalent (the group by attributes are swapped)
plan select a,b from sorted group by a,b;
plan select a,b from sorted group by b,a;

drop table unsorted;
drop table sorted;
