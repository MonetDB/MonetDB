select 'hello world' where 1 in (1);
select 'hello world' where 1 in (0);
select 'hello world' where 1 not in (1);
select 'hello world' where 1 not in (0);

select 'hello world' where NULL in (NULL);
select 'hello world' where NULL in (0);
select 'hello world' where NULL not in (NULL);
select 'hello world' where NULL not in (0);

select 'hello world' where 1 in (1,5);
select 'hello world' where 1 in (0,5);
select 'hello world' where 1 not in (1,5);
select 'hello world' where 1 not in (0,5);

select 'hello world' where NULL in (NULL,5);
select 'hello world' where NULL in (0,5);
select 'hello world' where NULL not in (NULL,5);
select 'hello world' where NULL not in (0,5);

select 'hello world' where 1 in (4,1,5);
select 'hello world' where 1 in (4,0,5);
select 'hello world' where 1 not in (4,1,5);
select 'hello world' where 1 not in (4,0,5);

select 'hello world' where 1 in (select 1);
select 'hello world' where 1 in (select 0);
select 'hello world' where 1 not in (select 1);
select 'hello world' where 1 not in (select 0);

select 'hello world' where (1,1) in (select 1,1);
select 'hello world' where (1,1) in (select 1,5);
select 'hello world' where (1,1) not in (select 1,1);
select 'hello world' where (1,1) not in (select 1,5);

create table in_table( in_col int, colid int, helloworld varchar(20));
insert into in_table values (1, 10, 'hello'), (2, 12,  'world'), (3, 14, '\n');

select helloworld from in_table where 1 in (1);
select helloworld from in_table where 1 in (0);
select helloworld from in_table where 1 not in (1);
select helloworld from in_table where 1 not in (0);

select helloworld from in_table where NULL in (NULL);
select helloworld from in_table where NULL in (0);
select helloworld from in_table where NULL not in (NULL);
select helloworld from in_table where NULL not in (0);

select helloworld from in_table where 1 in (in_col);
select helloworld from in_table where 0 in (in_col);
select helloworld from in_table where 1 not in (in_col);
select helloworld from in_table where 0 not in (in_col);

select helloworld from in_table where 1 in (in_col,1,in_col);
select helloworld from in_table where 0 in (in_col,1,in_col);
select helloworld from in_table where 1 not in (in_col,1,in_col);
select helloworld from in_table where 0 not in (in_col,1,in_col);

select helloworld from in_table where 1 in (in_col,1,in_col,colid);
select helloworld from in_table where 0 in (in_col,1,in_col,colid);
select helloworld from in_table where 1 not in (in_col,1,in_col,colid);
select helloworld from in_table where 0 not in (in_col,1,in_col,colid);

select helloworld from in_table where in_col in (1);
select helloworld from in_table where in_col in (0);
select helloworld from in_table where in_col not in (1);
select helloworld from in_table where in_col not in (0);

select helloworld from in_table where in_col in (1,5);
select helloworld from in_table where in_col in (0,5);
select helloworld from in_table where in_col not in (1,5);
select helloworld from in_table where in_col not in (0,5);

select helloworld from in_table where NULL in (in_col);
select helloworld from in_table where NULL not in (in_col);
select helloworld from in_table where in_col in (NULL);
select helloworld from in_table where in_col not in (NULL);

drop table in_table;
