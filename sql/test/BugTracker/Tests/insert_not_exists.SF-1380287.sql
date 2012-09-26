START TRANSACTION;
SELECT 'creating tables';
CREATE TABLE s4_kwm(urlid int,kwid int);
CREATE TABLE s4_keywords(id int, kw string);
CREATE TABLE kwTemp (id int, kw text);
CREATE TABLE kwmTemp (urlid int, kwid int);
COMMIT;

-- INSERT SOME VALUES IN kwTemp
START TRANSACTION;
SELECT 'filling kwTemp';
INSERT INTO kwtemp VALUES(1,'references');
INSERT INTO kwtemp VALUES(2,'Socialsc');
INSERT INTO kwtemp VALUES(3,'geog');
INSERT INTO kwtemp VALUES(4,'nd');
INSERT INTO kwtemp VALUES(5,'wz');
INSERT INTO kwtemp VALUES(6,'twtrst');
INSERT INTO kwtemp VALUES(7,'webquest');
INSERT INTO kwtemp VALUES(8,'Resources');
INSERT INTO kwtemp VALUES(9,'weather');
INSERT INTO kwtemp VALUES(10,'huricane');
INSERT INTO kwtemp VALUES(11,'whhistry');
INSERT INTO kwtemp VALUES(12,'wfrancat');
INSERT INTO kwtemp VALUES(13,'kugle');
INSERT INTO kwtemp VALUES(14,'hko');
INSERT INTO kwtemp VALUES(15,'informtc');
INSERT INTO kwtemp VALUES(16,'informcc');
INSERT INTO kwtemp VALUES(17,'data');
INSERT INTO kwtemp VALUES(18,'hrd');
INSERT INTO kwtemp VALUES(19,'tcfaq');
INSERT INTO kwtemp VALUES(20,'tcfaqF');
COMMIT;

-- INSERT SOME VALUES IN kwmTemp
START TRANSACTION;
SELECT 'filling kwmTemp';
INSERT INTO kwmtemp VALUES(1,1);
INSERT INTO kwmtemp VALUES(1,2);
INSERT INTO kwmtemp VALUES(1,3);
INSERT INTO kwmtemp VALUES(1,4);
INSERT INTO kwmtemp VALUES(2,5);
INSERT INTO kwmtemp VALUES(2,6);
INSERT INTO kwmtemp VALUES(3,7);
INSERT INTO kwmtemp VALUES(3,8);
INSERT INTO kwmtemp VALUES(4,9);
INSERT INTO kwmtemp VALUES(4,10);
INSERT INTO kwmtemp VALUES(4,11);
INSERT INTO kwmtemp VALUES(5,9);
INSERT INTO kwmtemp VALUES(5,12);
INSERT INTO kwmtemp VALUES(6,13);
INSERT INTO kwmtemp VALUES(7,14);
INSERT INTO kwmtemp VALUES(7,15);
INSERT INTO kwmtemp VALUES(7,16);
INSERT INTO kwmtemp VALUES(8,17);
INSERT INTO kwmtemp VALUES(9,18);
INSERT INTO kwmtemp VALUES(9,19);
COMMIT;

-- #### REAL BUG QUERIES START HERE ####


-- QUERY 1 ALL FROM kwTEMP -> s4_keywords
-- But without duplicates on the second column (keyword column)

START TRANSACTION;
SELECT count(*) FROM kwTemp;
SELECT 'filling s4_keywords from kwTemp';
INSERT INTO s4_keywords SELECT * FROM kwTemp WHERE NOT EXISTS (SELECT * FROM s4_keywords AS kt WHERE kt.kw = kwTemp.kw);
SELECT 'dropping kwTemp';
DROP TABLE kwTemp;
COMMIT;

-- QUERY 2 ALL FROM kwmTemp -> s4_kwm
-- BUT without duplicates, on both columns

START TRANSACTION;
SELECT count(*) FROM kwmtemp;
SELECT 'filling s4_kwm from kwmTemp';
INSERT INTO s4_kwm SELECT kwmTemp.urlId,kwmTemp.kwid FROM kwmTemp WHERE NOT EXISTS (SELECT * FROM s4_kwm AS kwm WHERE kwm.kwid = kwmTemp.kwid AND kwm.urlid = kwmTemp.urlid);
SELECT 'dropping kwmTemp';
DROP TABLE kwmTemp;
COMMIT;

START TRANSACTION;
-- OUTCOME is 20, exactly as espected
SELECT 'expect 20', COUNT(*) FROM s4_keywords;
-- OUTCOME is 0, 20 was expected
SELECT 'expect 20', COUNT(*) FROM s4_kwm;
COMMIT;

START TRANSACTION;
SELECT 'dropping s4_kwm and s4_keywords';
DROP TABLE s4_kwm;
DROP TABLE s4_keywords;
COMMIT;
