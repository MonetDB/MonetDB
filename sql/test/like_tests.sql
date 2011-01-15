-- create our table to test with
CREATE TABLE liketable (
	str varchar(20)
);

-- insert some dull values
INSERT INTO liketable VALUES ('');
INSERT INTO liketable VALUES ('t');
INSERT INTO liketable VALUES ('ts');
INSERT INTO liketable VALUES ('tsz');
-- insert some interesting values
INSERT INTO liketable VALUES ('a math assignment');
INSERT INTO liketable VALUES ('pathfinder is fun!');
-- insert some non-normal life values
INSERT INTO liketable VALUES ('123123123');
INSERT INTO liketable VALUES ('123123456123');
INSERT INTO liketable VALUES ('199993456123');
INSERT INTO liketable VALUES ('123456123456');
INSERT INTO liketable VALUES ('123454321');

-- see if everything is in the table
SELECT * FROM liketable;

-- check for a string that starts with a 't' and is two chars long
SELECT * FROM liketable WHERE str LIKE 't_';
-- simple, this should only match the first interesting value
SELECT * FROM liketable WHERE str LIKE '%math%';
-- more complicated; find values that match the given patterns
SELECT * FROM liketable WHERE str LIKE 'a%math%';
SELECT * FROM liketable WHERE str LIKE 'a_math%';
SELECT * FROM liketable WHERE str LIKE '%m_th_a%t';
SELECT * FROM liketable WHERE str LIKE '%at%_!';
-- exhaustive?
  -- head match
SELECT * FROM liketable WHERE str LIKE '1%';
SELECT * FROM liketable WHERE str LIKE '3%';
  -- tail match
SELECT * FROM liketable WHERE str LIKE '%1';
SELECT * FROM liketable WHERE str LIKE '%3';
  -- head/tail match
SELECT * FROM liketable WHERE str LIKE '1%1';	
SELECT * FROM liketable WHERE str LIKE '1%3';
SELECT * FROM liketable WHERE str LIKE '3%1';
SELECT * FROM liketable WHERE str LIKE '3%3';
  -- body match
SELECT * FROM liketable WHERE str LIKE '%1%';
SELECT * FROM liketable WHERE str LIKE '%12%';
SELECT * FROM liketable WHERE str LIKE '%13%';
SELECT * FROM liketable WHERE str LIKE '%454%';
  -- float match
SELECT * FROM liketable WHERE str LIKE '%2%2%';
  -- deeper insights, fairly specific matches
SELECT * FROM liketable WHERE str LIKE '_2_3%123';
SELECT * FROM liketable WHERE str LIKE '_123%3';
    -- attention: this one should match on 123123456123, it is tricky because
	--            if you match the first 1, it won't match...
SELECT * FROM liketable WHERE str LIKE '%1_3456%';
    -- attention: this one shows why the above like expression returns the
	--            correct answer... the _ is treated as {1,*} not {1}
SELECT * FROM liketable WHERE str LIKE '_3456%';
    -- another simple way to give the like processor a hard time
SELECT * FROM liketable WHERE str LIKE '%23';
-- test the ESCAPE statement
SELECT * FROM liketable WHERE str LIKE '%' ESCAPE '?';
SELECT * FROM liketable WHERE str LIKE '?%' ESCAPE '?';
-- test with an ESCAPE character that has a special meaning in a reg exp
SELECT * FROM liketable WHERE str LIKE '.%' ESCAPE '.';
SELECT * FROM liketable WHERE str LIKE '..' ESCAPE '.';
-- test whether escaping the ESCAPE character actually works
SELECT * FROM liketable WHERE str LIKE 'tt' ESCAPE 't';

-- clean up mess we made
drop table liketable;
