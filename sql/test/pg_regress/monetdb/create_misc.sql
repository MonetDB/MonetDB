--
-- CREATE_MISC
--

-- CLASS POPULATION
--	(any resemblance to real life is purely coincidental)
--

INSERT INTO tenk2 VALUES (tenk1.*);

SELECT * INTO TABLE onek2 FROM onek;


INSERT INTO fast_emp4000 VALUES (slow_emp4000.*);

SELECT *
   INTO TABLE Bprime
   FROM tenk1
   WHERE unique2 < 1000;

INSERT INTO hobbies_r (string, person)
   SELECT 'posthacking', p.string
   FROM person* p
   WHERE p.name = 'mike' or p.name = 'jeff';

INSERT INTO hobbies_r (string, person)
   SELECT 'basketball', p.string
   FROM person p
   WHERE p.name = 'joe' or p.name = 'sally';

INSERT INTO hobbies_r (name) VALUES ('skywalking');

INSERT INTO equipment_r (string, hobby) VALUES ('advil', 'posthacking');

INSERT INTO equipment_r (string, hobby) VALUES ('peet''s coffee', 'posthacking');

INSERT INTO equipment_r (string, hobby) VALUES ('hightops', 'basketball');

INSERT INTO equipment_r (string, hobby) VALUES ('guts', 'skywalking');

SELECT *
   INTO TABLE ramp
   FROM road
   WHERE name ~ '.*Ramp';

INSERT INTO ihighway 
   SELECT * 
   FROM road 
   WHERE name ~ 'I- .*';

INSERT INTO shighway 
   SELECT * 
   FROM road 
   WHERE name ~ 'State Hwy.*';

UPDATE shighway
   SET surface = 'asphalt';

INSERT INTO a_star (class, a) VALUES ('a', 1);

INSERT INTO a_star (class, a) VALUES ('a', 2);

INSERT INTO a_star (class) VALUES ('a');

INSERT INTO b_star (class, a, b) VALUES ('b', 3, 'mumble'::text);

INSERT INTO b_star (class, a) VALUES ('b', 4);

INSERT INTO b_star (class, b) VALUES ('b', 'bumble'::text);

INSERT INTO b_star (class) VALUES ('b');

INSERT INTO c_star (class, a, c) VALUES ('c', 5, 'hi mom'::name);

INSERT INTO c_star (class, a) VALUES ('c', 6);

INSERT INTO c_star (class, c) VALUES ('c', 'hi paul'::name);

INSERT INTO c_star (class) VALUES ('c');

INSERT INTO d_star (class, a, b, c, d)
   VALUES ('d', 7, 'grumble'::text, 'hi sunita'::string, '0.0'::double);

INSERT INTO d_star (class, a, b, c)
   VALUES ('d', 8, 'stumble'::text, 'hi koko'::name);

INSERT INTO d_star (class, a, b, d)
   VALUES ('d', 9, 'rumble'::text, '1.1'::double);

INSERT INTO d_star (class, a, c, d)
   VALUES ('d', 10, 'hi kristin'::string, '10.01'::double);

INSERT INTO d_star (class, b, c, d)
   VALUES ('d', 'crumble'::text, 'hi boris'::string, '100.001'::double);

INSERT INTO d_star (class, a, b)
   VALUES ('d', 11, 'fumble'::text);

INSERT INTO d_star (class, a, c)
   VALUES ('d', 12, 'hi avi'::name);

INSERT INTO d_star (class, a, d)
   VALUES ('d', 13, '1000.0001'::double);

INSERT INTO d_star (class, b, c)
   VALUES ('d', 'tumble'::text, 'hi andrew'::name);

INSERT INTO d_star (class, b, d)
   VALUES ('d', 'humble'::text, '10000.00001'::double);

INSERT INTO d_star (class, c, d)
   VALUES ('d', 'hi ginger'::string, '100000.000001'::double);

INSERT INTO d_star (class, a) VALUES ('d', 14);

INSERT INTO d_star (class, b) VALUES ('d', 'jumble'::text);

INSERT INTO d_star (class, c) VALUES ('d', 'hi jolly'::name);

INSERT INTO d_star (class, d) VALUES ('d', '1000000.0000001'::double);

INSERT INTO d_star (class) VALUES ('d');

INSERT INTO e_star (class, a, c, e)
   VALUES ('e', 15, 'hi carol'::string, '-1'::smallint);

INSERT INTO e_star (class, a, c)
   VALUES ('e', 16, 'hi bob'::name);

INSERT INTO e_star (class, a, e)
   VALUES ('e', 17, '-2'::smallint);

INSERT INTO e_star (class, c, e)
   VALUES ('e', 'hi michelle'::string, '-3'::smallint);

INSERT INTO e_star (class, a)
   VALUES ('e', 18);

INSERT INTO e_star (class, c)
   VALUES ('e', 'hi elisa'::name);

INSERT INTO e_star (class, e)
   VALUES ('e', '-4'::smallint);

INSERT INTO f_star (class, a, c, e, f)
   VALUES ('f', 19, 'hi claire'::string, '-5'::smallint, '(1,3),(2,4)'::string);

INSERT INTO f_star (class, a, c, e)
   VALUES ('f', 20, 'hi mike'::string, '-6'::smallint);

INSERT INTO f_star (class, a, c, f)
   VALUES ('f', 21, 'hi marcel'::string, '(11,44),(22,55),(33,66)'::string);

INSERT INTO f_star (class, a, e, f)
   VALUES ('f', 22, '-7'::smallint, '(111,555),(222,666),(333,777),(444,888)'::string);

INSERT INTO f_star (class, c, e, f)
   VALUES ('f', 'hi keith'::string, '-8'::smallint, 
	   '(1111,3333),(2222,4444)'::string);

INSERT INTO f_star (class, a, c)
   VALUES ('f', 24, 'hi marc'::name);

INSERT INTO f_star (class, a, e)
   VALUES ('f', 25, '-9'::smallint);

INSERT INTO f_star (class, a, f)
   VALUES ('f', 26, '(11111,33333),(22222,44444)'::string); 

INSERT INTO f_star (class, c, e)
   VALUES ('f', 'hi allison'::string, '-10'::smallint);

INSERT INTO f_star (class, c, f)
   VALUES ('f', 'hi jeff'::string,
           '(111111,333333),(222222,444444)'::string);

INSERT INTO f_star (class, e, f)
   VALUES ('f', '-11'::smallint, '(1111111,3333333),(2222222,4444444)'::string);

INSERT INTO f_star (class, a) VALUES ('f', 27);

INSERT INTO f_star (class, c) VALUES ('f', 'hi carl'::name);

INSERT INTO f_star (class, e) VALUES ('f', '-12'::smallint);

INSERT INTO f_star (class, f) 
   VALUES ('f', '(11111111,33333333),(22222222,44444444)'::string);

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
   VALUES (1, 3.567, '(3.0,1.0),(4.0,2.0)'::string);

INSERT INTO iportaltest (i, d, p)
   VALUES (2, 89.05, '(4.0,2.0),(3.0,1.0)'::string);
