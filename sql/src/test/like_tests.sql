START TRANSACTION;

-- create our table to test with
CREATE TABLE like_test (
	str varchar
);

-- insert some dull values
INSERT INTO like_test VALUES ('');
INSERT INTO like_test VALUES ('t');
INSERT INTO like_test VALUES ('ts');
INSERT INTO like_test VALUES ('tsz');
-- insert some interesting values
INSERT INTO like_test VALUES ('a math assignment');
INSERT INTO like_test VALUES ('pathfinder is fun!');
-- insert some non-normal life values
INSERT INTO like_test VALUES ('123123123');
INSERT INTO like_test VALUES ('123123456123');
INSERT INTO like_test VALUES ('199993456123');
INSERT INTO like_test VALUES ('123456123456');
INSERT INTO like_test VALUES ('123454321');

-- see if everything is in the table
SELECT * FROM like_test;

-- check for a string that starts with a 't' and is two chars long
SELECT * FROM like_test WHERE str LIKE 't_';
-- simple, this should only match the first interesting value
SELECT * FROM like_test WHERE str LIKE '%math%';
-- more complicated; find values that match the given patterns
SELECT * FROM like_test WHERE str LIKE 'a%math%';
SELECT * FROM like_test WHERE str LIKE 'a_math%';
SELECT * FROM like_test WHERE str LIKE '%m_th_a%t';
SELECT * FROM like_test WHERE str LIKE '%at%_!';
-- exhaustive?
  -- head match
SELECT * FROM like_test WHERE str LIKE '1%';
SELECT * FROM like_test WHERE str LIKE '3%';
  -- tail match
SELECT * FROM like_test WHERE str LIKE '%1';
SELECT * FROM like_test WHERE str LIKE '%3';
  -- head/tail match
SELECT * FROM like_test WHERE str LIKE '1%1';	
SELECT * FROM like_test WHERE str LIKE '1%3';
SELECT * FROM like_test WHERE str LIKE '3%1';
SELECT * FROM like_test WHERE str LIKE '3%3';
  -- body match
SELECT * FROM like_test WHERE str LIKE '%1%';
SELECT * FROM like_test WHERE str LIKE '%12%';
SELECT * FROM like_test WHERE str LIKE '%13%';
SELECT * FROM like_test WHERE str LIKE '%454%';
  -- float match
SELECT * FROM like_test WHERE str LIKE '%2%2%';
  -- deeper insights, fairly specific matches
SELECT * FROM like_test WHERE str LIKE '_2_3%123';
SELECT * FROM like_test WHERE str LIKE '_123%3';
    -- attention: this one should match on 123123456123, it is tricky because
	--            if you match the first 1, it won't match...
SELECT * FROM like_test WHERE str LIKE '%1_3456%';
    -- attention: this one shows why the above like expression returns the
	--            correct answer... the _ is treated as {1,*} not {1}
SELECT * FROM like_test WHERE str LIKE '_3456%';

-- clean up mess we made
ROLLBACK;
