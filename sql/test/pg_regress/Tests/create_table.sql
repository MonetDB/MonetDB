--
-- CREATE_TABLE
--

--
-- CLASS DEFINITIONS
--
CREATE TABLE hobbies_r (
	name		text, 
	person 		text
);

CREATE TABLE equipment_r (
	name 		text,
	hobby		text
);

/* tables onek, tenk1 and tenk2 are already created in loadwisconsin.sql
CREATE TABLE onek (
	unique1		integer,
	unique2		integer,
	two			integer,
	four		integer,
	ten			integer,
	twenty		integer,
	hundred		integer,
	thousand	integer,
	twothousand	integer,
	fivethous	integer,
	tenthous	integer,
	odd			integer,
	even		integer,
	stringu1	string,
	stringu2	string,
	string4		string
);

CREATE TABLE tenk1 (
	unique1		integer,
	unique2		integer,
	two			integer,
	four		integer,
	ten			integer,
	twenty		integer,
	hundred		integer,
	thousand	integer,
	twothousand	integer,
	fivethous	integer,
	tenthous	integer,
	odd			integer,
	even		integer,
	stringu1	string,
	stringu2	string,
	string4		string
);

CREATE TABLE tenk2 (
	unique1 	integer,
	unique2 	integer,
	two 	 	integer,
	four 		integer,
	ten			integer,
	twenty 		integer,
	hundred 	integer,
	thousand 	integer,
	twothousand integer,
	fivethous 	integer,
	tenthous	integer,
	odd			integer,
	even		integer,
	stringu1	string,
	stringu2	string,
	string4		string
);
*/

CREATE TABLE person (
	name 		text,
	age			integer,
	location 	string
);


CREATE TABLE emp (
	name 		text,
	age			integer,
	location 	string,

	salary 		integer,
	manager 	string
); -- INHERITS (person)


CREATE TABLE student (
	name 		text,
	age			integer,
	location 	string,

	gpa 		double
); -- INHERITS (person)


CREATE TABLE stud_emp (
	name 		text,
	age			integer,
	location 	string,

	salary 		integer,
	manager 	string,

	gpa 		double,

	percent 	integer
); -- INHERITS (emp, student)


CREATE TABLE city (
	name		string,
	location 	string,
	budget 		decimal(7,2)
);

CREATE TABLE dept (
	dname		string,
	mgrname 	text
);

CREATE TABLE slow_emp4000 (
	home_base	 string
);

CREATE TABLE fast_emp4000 (
	home_base	 string
);

CREATE TABLE road (
	name		text,
	thepath 	string
);

CREATE TABLE ihighway (
	name		text,
	thepath 	string
); -- INHERITS (road)

CREATE TABLE shighway (
	name		text,
	thepath 	string,

	surface		text
); -- INHERITS (road)

CREATE TABLE real_city (
	pop			integer,
	cname		text,
	outline 	string
);

--
-- test the "star" operators a bit more thoroughly -- this time,
-- throw in lots of NULL fields...
--
-- a is the type root
-- b and c inherit from a (one-level single inheritance)
-- d inherits from b and c (two-level multiple inheritance)
-- e inherits from c (two-level single inheritance)
-- f inherits from e (three-level single inheritance)
--
CREATE TABLE a_star (
	class		char, 
	a 			integer
);

CREATE TABLE b_star (
	class		char, 
	a 			integer,

	b 			text
); -- INHERITS (a_star)

CREATE TABLE c_star (
	class		char, 
	a 			integer,

	c 			string
); -- INHERITS (a_star)

CREATE TABLE d_star (
	class		char, 
	a 			integer,
	b 			text,

	c 			string,

	d 			double
); -- INHERITS (b_star, c_star)

CREATE TABLE e_star (
	class		char, 
	a 			integer,
	c 			string,

	e 			smallint
); -- INHERITS (c_star)

CREATE TABLE f_star (
	class		char, 
	a 			integer,
	c 			string,
	e 			smallint,

	f 			string
); -- INHERITS (e_star)

CREATE TABLE aggtest (
	a 			smallint,
	b			float
);

CREATE TABLE hash_i4_heap (
	seqno 		integer,
	random 		integer
);

CREATE TABLE hash_name_heap (
	seqno 		integer,
	random 		string
);

CREATE TABLE hash_txt_heap (
	seqno 		integer,
	random 		text
);

CREATE TABLE hash_f8_heap (
	seqno		integer,
	random 		double
);

-- don't include the hash_ovfl_heap stuff in the distribution
-- the data set is too large for what it's worth
-- 
-- CREATE TABLE hash_ovfl_heap (
--	x			integer,
--	y			integer
-- );

CREATE TABLE bt_i4_heap (
	seqno 		integer,
	random 		integer
);

CREATE TABLE bt_name_heap (
	seqno 		string,
	random 		integer
);

CREATE TABLE bt_txt_heap (
	seqno 		text,
	random 		integer
);

CREATE TABLE bt_f8_heap (
	seqno 		double, 
	random 		integer
);


-- in drop.sql all these tables are dropped
