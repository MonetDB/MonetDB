create table bug2722 (time timestamp, val int, fk int);
insert into bug2722 values (current_timestamp(), 1, 1);
insert into bug2722 values (current_timestamp(), 2, 2);
insert into bug2722 values (current_timestamp(), 3, 1);
insert into bug2722 values (current_timestamp(), 4, 2);
copy select val from bug2722 where fk=2 order by time into stdout using delimiters ' ' , '\n';
drop table bug2722;
