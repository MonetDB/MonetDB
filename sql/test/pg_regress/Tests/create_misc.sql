--
-- CREATE_MISC
--

-- CLASS POPULATION
--	(any resemblance to real life is purely coincidental)
--

--INSERT INTO tenk2 VALUES (tenk1.*);
INSERT INTO tenk2 SELECT * FROM tenk1;

CREATE TABLE onek2 AS
SELECT *
-- INTO TABLE onek2
 FROM onek
WITH DATA;

--INSERT INTO fast_emp4000 VALUES (slow_emp4000.*);
INSERT INTO fast_emp4000 (SELECT * FROM slow_emp4000);

CREATE TABLE Bprime AS
SELECT *
--   INTO TABLE Bprime
   FROM tenk1
   WHERE unique2 < 1000
WITH DATA;

INSERT INTO hobbies_r (name, person)
   SELECT 'posthacking', p.name
   FROM person p
   WHERE p.name = 'mike' or p.name = 'jeff';

INSERT INTO hobbies_r (name, person)
   SELECT 'basketball', p.name
   FROM person p
   WHERE p.name = 'joe' or p.name = 'sally';

INSERT INTO hobbies_r (name) VALUES ('skywalking');

INSERT INTO equipment_r (name, hobby) VALUES ('advil', 'posthacking');

INSERT INTO equipment_r (name, hobby) VALUES ('peet''s coffee', 'posthacking');

INSERT INTO equipment_r (name, hobby) VALUES ('hightops', 'basketball');

INSERT INTO equipment_r (name, hobby) VALUES ('guts', 'skywalking');

CREATE TABLE ramp AS
SELECT *
--   INTO TABLE ramp
   FROM road
   WHERE name = '.*Ramp'
WITH DATA;

INSERT INTO ihighway 
   SELECT * 
   FROM road 
   WHERE name = 'I- .*';

INSERT INTO shighway (name, thepath)
   SELECT *
   FROM road 
   WHERE name = 'State Hwy.*';

UPDATE shighway
   SET surface = 'asphalt';

INSERT INTO a_star (class, a) VALUES ('a', 1);

INSERT INTO a_star (class, a) VALUES ('a', 2);

INSERT INTO a_star (class) VALUES ('a');

INSERT INTO b_star (class, a, b) VALUES ('b', 3, cast('mumble' as text));

INSERT INTO b_star (class, a) VALUES ('b', 4);

INSERT INTO b_star (class, b) VALUES ('b', cast('bumble' as text));

INSERT INTO b_star (class) VALUES ('b');

INSERT INTO c_star (class, a, c) VALUES ('c', 5, cast('hi mom' as string));

INSERT INTO c_star (class, a) VALUES ('c', 6);

INSERT INTO c_star (class, c) VALUES ('c', cast('hi paul' as string));

INSERT INTO c_star (class) VALUES ('c');

INSERT INTO d_star (class, a, b, c, d)
   VALUES ('d', 7, cast('grumble' as text), cast('hi sunita' as string), cast('0.0' as double));

INSERT INTO d_star (class, a, b, c)
   VALUES ('d', 8, cast('stumble' as text), cast('hi koko' as string));

INSERT INTO d_star (class, a, b, d)
   VALUES ('d', 9, cast('rumble' as text), cast('1.1' as double));

INSERT INTO d_star (class, a, c, d)
   VALUES ('d', 10, cast('hi kristin' as string), cast('10.01' as double));

INSERT INTO d_star (class, b, c, d)
   VALUES ('d', cast('crumble' as text), cast('hi boris' as string), cast('100.001' as double));

INSERT INTO d_star (class, a, b)
   VALUES ('d', 11, cast('fumble' as text));

INSERT INTO d_star (class, a, c)
   VALUES ('d', 12, cast('hi avi' as string));

INSERT INTO d_star (class, a, d)
   VALUES ('d', 13, cast('1000.0001' as double));

INSERT INTO d_star (class, b, c)
   VALUES ('d', cast('tumble' as text), cast('hi andrew' as string));

INSERT INTO d_star (class, b, d)
   VALUES ('d', cast('humble' as text), cast('10000.00001' as double));

INSERT INTO d_star (class, c, d)
   VALUES ('d', cast('hi ginger' as string), cast('100000.000001' as double));

INSERT INTO d_star (class, a) VALUES ('d', 14);

INSERT INTO d_star (class, b) VALUES ('d', cast('jumble' as text));

INSERT INTO d_star (class, c) VALUES ('d', cast('hi jolly' as string));

INSERT INTO d_star (class, d) VALUES ('d', cast('1000000.0000001' as double));

INSERT INTO d_star (class) VALUES ('d');

INSERT INTO e_star (class, a, c, e)
   VALUES ('e', 15, cast('hi carol' as string), cast('-1' as smallint));

INSERT INTO e_star (class, a, c)
   VALUES ('e', 16, cast('hi bob' as string));

INSERT INTO e_star (class, a, e)
   VALUES ('e', 17, cast('-2' as smallint));

INSERT INTO e_star (class, c, e)
   VALUES ('e', cast('hi michelle' as string), cast('-3' as smallint));

INSERT INTO e_star (class, a)
   VALUES ('e', 18);

INSERT INTO e_star (class, c)
   VALUES ('e', cast('hi elisa' as string));

INSERT INTO e_star (class, e)
   VALUES ('e', cast('-4' as smallint));

INSERT INTO f_star (class, a, c, e, f)
   VALUES ('f', 19, cast('hi claire' as string), cast('-5' as smallint), cast('(1,3),(2,4)' as string));

INSERT INTO f_star (class, a, c, e)
   VALUES ('f', 20, cast('hi mike' as string), cast('-6' as smallint));

INSERT INTO f_star (class, a, c, f)
   VALUES ('f', 21, cast('hi marcel' as string), cast('(11,44),(22,55),(33,66)' as string));

INSERT INTO f_star (class, a, e, f)
   VALUES ('f', 22, cast('-7' as smallint), cast('(111,555),(222,666),(333,777),(444,888)' as string));

INSERT INTO f_star (class, c, e, f)
   VALUES ('f', cast('hi keith' as string), cast('-8' as smallint),
	   cast('(1111,3333),(2222,4444)' as string));

INSERT INTO f_star (class, a, c)
   VALUES ('f', 24, cast('hi marc' as string));

INSERT INTO f_star (class, a, e)
   VALUES ('f', 25,cast( '-9' as smallint));

INSERT INTO f_star (class, a, f)
   VALUES ('f', 26, cast('(11111,33333),(22222,44444)' as string));

INSERT INTO f_star (class, c, e)
   VALUES ('f', cast('hi allison' as string), cast('-10' as smallint));

INSERT INTO f_star (class, c, f)
   VALUES ('f', cast('hi jeff' as string),
           cast('(111111,333333),(222222,444444)' as string));

INSERT INTO f_star (class, e, f)
   VALUES ('f', cast('-11' as smallint), cast('(1111111,3333333),(2222222,4444444)' as string));

INSERT INTO f_star (class, a) VALUES ('f', 27);

INSERT INTO f_star (class, c) VALUES ('f', cast('hi carl' as string));

INSERT INTO f_star (class, e) VALUES ('f', cast('-12' as smallint));

INSERT INTO f_star (class, f) 
   VALUES ('f', cast('(11111111,33333333),(22222222,44444444)' as string));

INSERT INTO f_star (class) VALUES ('f');


--
-- for internal portal (cursor) tests
--
CREATE TABLE iportaltest (
	i		integer, 
	d		float, 
	p		string
);

INSERT INTO iportaltest (i, d, p)
   VALUES (1, 3.567, cast('(3.0,1.0),(4.0,2.0)' as string));

INSERT INTO iportaltest (i, d, p)
   VALUES (2, 89.05, cast('(4.0,2.0),(3.0,1.0)' as string));
