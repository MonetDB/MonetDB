
-- Examples 1 & 2[ab] merely set the stage by demonstrating
-- (double-checking) the behaviour of standard COPY INTO and
-- INSERT INTO when skipping columns with defaults.
-- The problem/bug with COPY LOADER INTO is demonstrated in
-- Examples 3[ab].


-- Example 1: COPY INTO & INSERT INTO work fine / correctly:
--start transaction;
create table t (
	a integer auto_increment primary key,
	b integer generated always as identity (start with 2) unique,
	c integer generated always as identity (start with 3) not null,
	d integer generated always as identity (start with 4),
	e integer default 5 not null,
	f integer default 6,
	g integer
);

copy 1 records into t (g) from stdin (g);
-7
copy 1 records into t (g) from stdin (g);
NULL
insert into t (g) values (NULL);
insert into t (g) values (-7);

copy 1 records into t (f) from stdin (f);
-6
copy 1 records into t (f) from stdin (f);
NULL
insert into t (f) values (NULL);
insert into t (f) values (-6);

copy 1 records into t (e) from stdin (e);
-5
copy 1 records into t (e) from stdin (e);
NULL
insert into t (e) values (NULL);
insert into t (e) values (-5);

copy 1 records into t (d) from stdin (d);
-4
copy 1 records into t (d) from stdin (d);
NULL
insert into t (d) values (NULL);
insert into t (d) values (-4);

copy 1 records into t (c) from stdin (c);
-3
copy 1 records into t (c) from stdin (c);
NULL
insert into t (c) values (NULL);
insert into t (c) values (-3);

copy 1 records into t (b) from stdin (b);
-2
copy 1 records into t (b) from stdin (b);
NULL
insert into t (b) values (NULL);
insert into t (b) values (-2);

copy 1 records into t (a) from stdin (a);
-1
copy 1 records into t (a) from stdin (a);
NULL
insert into t (a) values (NULL);
insert into t (a) values (-1);

select * from t;
drop table t;
--rollback;


-- Example 2a: INSERT INTO works fine / correctly:
--start transaction;
create table t (
	a integer auto_increment primary key,
	b integer generated always as identity (start with 2) unique,
	c integer generated always as identity (start with 3) not null,
	d integer generated always as identity (start with 4),
	e integer default 5 not null,
	f integer default 6,
	g integer
);
insert into t (a,b,c,d,e,f,g) values (-11,-12,-13,-14,-15,-16,-17);
insert into t (a,b,c,d,e,f)   values (-21,-22,-23,-24,-25,-26);
insert into t (a,b,c,d,e,g)   values (-31,-32,-33,-34,-35,-37);
insert into t (a,b,c,d,f,g)   values (-41,-42,-43,-44,-46,-47);
insert into t (a,b,c,e,f,g)   values (-51,-52,-53,-55,-56,-57);
insert into t (a,b,d,e,f,g)   values (-61,-62,-64,-65,-66,-67);
insert into t (a,c,d,e,f,g)   values (-71,-73,-74,-75,-76,-77);
insert into t (b,c,d,e,f,g)   values (-82,-83,-84,-85,-86,-87);
select * from t;
drop table t;
--rollback;


-- Example 2b: COPY INTO works fine / correctly:
--start transaction;
create table t (
	a integer auto_increment primary key,
	b integer generated always as identity (start with 2) unique,
	c integer generated always as identity (start with 3) not null,
	d integer generated always as identity (start with 4),
	e integer default 5 not null,
	f integer default 6,
	g integer
);
copy 2 records into t (a,b,c,d,e,f,g) from stdin (a,b,c,d,e,f,g);
-111|-112|-113|-114|-115|-116|-117
-121|-122|-123|-124|-125|-126|-127
copy 2 records into t (a,b,c,d,e,f) from stdin (a,b,c,d,e,f);
-211|-212|-213|-214|-215|-216
-221|-222|-223|-224|-225|-226
copy 2 records into t (a,b,c,d,e,g) from stdin (a,b,c,d,e,g);
-311|-312|-313|-314|-315|-317
-321|-322|-323|-324|-325|-327
copy 2 records into t (a,b,c,d,f,g) from stdin (a,b,c,d,f,g);
-411|-412|-413|-414|-416|-417
-421|-422|-423|-424|-426|-427
copy 2 records into t (a,b,c,e,f,g) from stdin (a,b,c,e,f,g);
-511|-512|-513|-515|-516|-517
-521|-522|-523|-525|-526|-527
copy 2 records into t (a,b,d,e,f,g) from stdin (a,b,d,e,f,g);
-611|-612|-614|-615|-616|-617
-621|-622|-624|-625|-626|-627
copy 2 records into t (a,c,d,e,f,g) from stdin (a,c,d,e,f,g);
-711|-713|-714|-715|-716|-717
-721|-723|-724|-725|-726|-727
copy 2 records into t (b,c,d,e,f,g) from stdin (b,c,d,e,f,g);
-812|-813|-814|-815|-816|-817
-822|-823|-824|-825|-826|-827
select * from t;
drop table t;
--rollback;


-- Example 3a: COPY LOADER INTO works INcorrectly,
--             i.e., replaces missing values/columsn with NULL rather than DEFAULT
--start transaction;
CREATE LOADER myloader(x integer, y string) LANGUAGE PYTHON {
	z={}
	i=0
	for j in ('a','b','c','d','e','f','g'):
		i += 1	
		if j in y:
			z[j] = x - i
	_emit.emit(z)
};
create table t (
	a integer auto_increment primary key,
	b integer generated always as identity (start with 2) unique,
	c integer generated always as identity (start with 3) not null,
	d integer generated always as identity (start with 4),
	e integer default 5 not null,
	f integer default 6,
	g integer
);
COPY LOADER INTO t FROM myloader(-10,'abcdefg');
COPY LOADER INTO t FROM myloader(-20,'abcdef');
COPY LOADER INTO t FROM myloader(-30,'abcdeg');
COPY LOADER INTO t FROM myloader(-40,'abcdfg');
COPY LOADER INTO t FROM myloader(-50,'abcefg');
COPY LOADER INTO t FROM myloader(-60,'abdefg');
COPY LOADER INTO t FROM myloader(-70,'acdefg');
COPY LOADER INTO t FROM myloader(-80,'bcdefg');
select * from t;
drop table t;
drop loader myloader;
--rollback;


-- Example 3b: COPY LOADER INTO works INcorrectly,
--             i.e., replaces missing values/columsn with NULL rather than DEFAULT
--start transaction;
CREATE LOADER myloader(x integer, y string) LANGUAGE PYTHON {
	z={}
	i=0
	for j in ('a','b','c','d','e','f','g'):
		i += 1	
		if j in y:
			z[j] = numpy.array([ x - 10 - i , x - 20 - i ])
	_emit.emit(z)
};
create table t (
	a integer auto_increment primary key,
	b integer generated always as identity (start with 2) unique,
	c integer generated always as identity (start with 3) not null,
	d integer generated always as identity (start with 4),
	e integer default 5 not null,
	f integer default 6,
	g integer
);
COPY LOADER INTO t FROM myloader(-100,'abcdefg');
COPY LOADER INTO t FROM myloader(-200,'abcdef');
COPY LOADER INTO t FROM myloader(-300,'abcdeg');
COPY LOADER INTO t FROM myloader(-400,'abcdfg');
COPY LOADER INTO t FROM myloader(-500,'abcefg');
COPY LOADER INTO t FROM myloader(-600,'abdefg');
COPY LOADER INTO t FROM myloader(-700,'acdefg');
COPY LOADER INTO t FROM myloader(-800,'bcdefg');
select * from t;
drop table t;
drop loader myloader;
--rollback;

