create table t (t1 timestamp, t2 timestamp(0), t3 timestamp(3));
insert into t values (now(), now(), now());
-- to avoid the variable output of 'now()', we check that we got what we
-- want by observing the length of the columns as string.  It is hardly
-- possible to check whether the date really includes a correct
-- fraction.  We can't do this (more reliable) through the headers,
-- because they are ignored by testweb.
select length(cast(t1 as varchar(200))),
	length(cast(t2 as varchar(200))),
	length(cast(t3 as varchar(200)))
from t;
