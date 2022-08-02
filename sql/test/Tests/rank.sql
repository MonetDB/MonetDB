create table ranktest ( id int, k string);
COPY 27 RECORDS INTO ranktest FROM stdin USING DELIMITERS '|',E'\n';
1061|varchar
1061|int
1061|varchar
1061|varchar
1061|int
1061|int
1061|varchar
1061|varchar
1061|boolean
1061|boolean
1061|int
1061|varchar
1061|char
1062|int
1062|char
1062|char
1062|int
1062|smallint
1062|boolean
1062|smallint
1061|int
1061|varchar
1061|int
1061|varchar
1061|int
1061|int
1061|smallint

select count(*) from ranktest;

select ROW_NUMBER() over () as foo from ranktest order by foo;
select ROW_NUMBER() over (PARTITION BY id) as foo, id from ranktest order by foo, id;
select ROW_NUMBER() over (PARTITION BY id ORDER BY id) as foo, id from ranktest order by foo, id;
select ROW_NUMBER() over (ORDER BY id) as foo, id from ranktest order by foo, id;

select RANK() over () as foo from ranktest order by foo;
select RANK() over (PARTITION BY id) as foo, id from ranktest order by foo, id;
select RANK() over (PARTITION BY id ORDER BY id) as foo, id from ranktest order by foo, id;
select RANK() over (ORDER BY id) as foo, id from ranktest order by foo, id;

select RANK() over () as foo, id, k from ranktest order by foo, id, k;
select RANK() over (PARTITION BY id) as foo, id, k from ranktest order by foo, id, k;
select RANK() over (PARTITION BY id ORDER BY id, k) as foo, id, k from ranktest order by foo, id, k;
select RANK() over (ORDER BY id, k) as foo, id, k from ranktest order by foo, id, k;

select DENSE_RANK() over () as foo, id, k from ranktest order by k, foo, id;
select DENSE_RANK() over (PARTITION BY id) as foo, id, k from ranktest order by k, foo, id;
select DENSE_RANK() over (PARTITION BY id ORDER BY id, k) as foo, id, k from ranktest order by k, foo, id;
select DENSE_RANK() over (ORDER BY id, k) as foo, id, k from ranktest order by k, foo, id;
drop table ranktest;
