start transaction;

create table foo (i bigint, t text, f int);
copy into foo from stdin;
1689|00i\047m|2
1690|00i\047v|2
41561|2015‎|1
45804|21π|1
51981|24hours‬|1
171067|ardèch|2
182773|aﬁ|1
607808|poverty‪|1

-- empty line to signal end of input
select * from foo;
rollback;
