CREATE TABLE fract_only (id int, val numeric(4,4));
--Note that the precision is equal to the scale, so the allowed number of significant digits before the decimal dot should not exceed 0 (4 - 4 = 0).

--the following SQL are NOT accepted but should be accepted:
INSERT INTO fract_only VALUES (1, '-0.9999');
INSERT INTO fract_only VALUES (2, '+0.9999');

--the following SQL are accepted but incorrect data is stored:
INSERT INTO fract_only VALUES (3, '+.9999');
SELECT * FROM fract_only;
-- returns value 2.5535 !!

--the following SQL are accepted but should error:
INSERT INTO fract_only VALUES (4, '0.99995');  -- should fail but is invalidly accepted
INSERT INTO fract_only VALUES (5, '0.99999');  -- should fail but is invalidly accepted
SELECT * FROM fract_only;
-- both show 1.0000 which out of the allowed value range of numeric(4,4)

INSERT INTO fract_only VALUES (6, '+0.99995'); -- correctly fails
INSERT INTO fract_only VALUES (6, '+.99995');  -- should fail but is invalidly accepted
SELECT * FROM fract_only;
-- returns value 2.5536 for id 6 !!

INSERT INTO fract_only VALUES (7, '-0.99995'); -- correctly fails
INSERT INTO fract_only VALUES (7, '-.999998');  -- should fail but is invalidly accepted
SELECT * FROM fract_only;
-- returns value -0.9999 for id 7 !!

drop table fract_only;
