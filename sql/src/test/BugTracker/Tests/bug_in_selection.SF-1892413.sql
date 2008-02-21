
CREATE TABLE test_subquery(date int not null constraint pk_test_subquery primary key);

INSERT into test_subquery values (19251231);
INSERT into test_subquery values (19260102);
INSERT into test_subquery values (19260104);

select * from test_subquery;

-- we don't support limit in the subquery
SELECT date, (SELECT date from test_subquery where date > t1.date limit 1) as dtNext from test_subquery t1;

-- but we need to reduce (ie this fails)
SELECT date, (SELECT date from test_subquery where date > t1.date) as dtNext from test_subquery t1;

-- so we use MAX
SELECT date, (SELECT max(date) from test_subquery where date > t1.date) as dtNext from test_subquery t1;

drop table test_subquery;
