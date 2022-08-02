start transaction;

create table table2848 (id char(1));
insert into table2848 values ('a');
insert into table2848 values ('b');
insert into table2848 values ('c');
SELECT CASE WHEN id = 'a' THEN 'x' ELSE 'y' END AS id FROM table2848;
select * from ( SELECT CASE WHEN id = 'a' THEN 'x' ELSE 'y' END AS othercolnamealias FROM table2848) as req;
select * from ( SELECT CASE WHEN id = 'a' THEN 'x' ELSE 'y' END AS id FROM table2848) as req;

rollback;
