START TRANSACTION;

create table integers (i int);
insert into integers (select value from generate_series(1,11,1));

CREATE FUNCTION python_sum(v int) returns int
language P
{
    return numpy.sum(v)
};

CREATE FUNCTION python_sum2(v int) returns int
language PYTHON_MAP
{
    return numpy.sum(v)
};

CREATE AGGREGATE python_sum3(v int) returns int
language P
{
    return numpy.sum(v)
};

CREATE AGGREGATE python_sum4(v int) returns int
language PYTHON_MAP
{
    return numpy.sum(v)
};

select python_sum(i) from integers where i = 1 group by i;
select python_sum2(i) from integers where i = 1 group by i;
select python_sum3(i) from integers where i = 1 group by i;
select python_sum4(i) from integers where i = 1 group by i;

ROLLBACK;
