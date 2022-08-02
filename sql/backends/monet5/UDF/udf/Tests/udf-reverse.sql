set optimizer = 'sequential_pipe';

explain select reverse('MonetDB');
select reverse('MonetDB');

create table udf_reverse ( x string );
insert into udf_reverse values ('MonetDB');
insert into udf_reverse values ('Database Architecture');
insert into udf_reverse values ('Information Systems');
insert into udf_reverse values ('Centrum Wiskunde & Informatica');
select * from udf_reverse;

explain select reverse(x) from udf_reverse;
select reverse(x) from udf_reverse;
