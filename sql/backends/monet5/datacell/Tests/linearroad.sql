-- The base line implementation
-- Table structures.
create schema lrbm;
set optimizer ='datacell_pipe';

create table lrbm.input (
    "type" int,
    "time" int,
    "carid" int,
    "speed" int,
    "xway" int,
    "lane" int,
    "dir" int,
    "seg" int,
    "pos" int,
    "qid" int,
    "m_init" int,
    "m_end" int,
    "dow" int,
    "tod" int,
    "day2" int);

call datacell.receptor('lrbm.input','localhost',50599);

CREATE TABLE lrbm.preAccident1(time integer, carid integer, lane integer, dir integer, seg integer, pos integer);
call datacell.basket('lrbm.preAccident1');

CREATE TABLE lrbm.preAccident2 (carid1 integer,carid2 integer,time integer,dir integer,seg integer, pos integer);
call datacell.basket('lrbm.preAccident2');

CREATE TABLE lrbm.accident(carid1 integer, carid2 integer, firstMinute integer, lastMinute integer, dir integer, seg integer, pos integer);
call datacell.basket('lrbm.accident');

CREATE TABLE lrbm.statisticsinput ( dir int, seg int, "time" int);
call datacell.basket('lrbm.statisticsinput');
CREATE TABLE lrbm.statistics(dir  int,seg  int,time_minute int,numvehicles  int,lav   float,toll  int,accident int,accidentSeg int);
call datacell.basket('lrbm.statistics');

CREATE TABLE lrbm.preVehicle (dir  int,seg  int,time_minute2 int,numvehicles  int);
call datacell.basket('lrbm.preVehicle');

CREATE TABLE lrbm.preLav( dir  int,seg  int,time_minute3 int,lav  float);
call datacell.basket('lrbm.preLav');

CREATE TABLE lrbm.TollAccAlerts(
                  time   INTEGER,
                  carid  INTEGER,
                  dir    INTEGER,
                  seg    INTEGER,
                  lav    INTEGER,
                  toll   INTEGER,
                  accidentSeg INTEGER);

CREATE TABLE lrbm.dailyExpenditureRequests ( "type" int, "time" int, "carid" int, "xway" int, "qid" int, "day2" int);
CREATE TABLE lrbm.dailyExpenditureanswer ( "carid" int, "day2" int, "bal" int, "qid" int );
CREATE TABLE lrbm.completehistory ( "carid" int, "day2" int, "xway" int, "toll" int);
CREATE TABLE lrbm.accountBalanceRequests ( "type" int, "time" int, "carid" int, "xway" int, "qid" int, "day2" int);

-- split the incoming stream over the 5 different groups first.
CREATE PROCEDURE lrb.splitter()
BEGIN
	INSERT INTO lrbm.preAccident1 (time, carid, lane, dir, seg, pos) 
	SELECT  time, carid, lane, dir, seg, pos 
	FROM lrbm.input 
	WHERE   speed = 0 AND type = 0;

	-- collect all unique triples
	INSERT INTO lrbm.statisticsinput
	SELECT dir, seg, "time" FROM lrbm.input WHERE type = 0
	GROUP BY dir, seg, "time";
END;

CREATE PROCEDURE lrbm.accidents()
BEGIN
	INSERT INTO lrbm.preAccident2 (carid1 ,carid2 , time , dir , seg , pos) 
	SELECT  in1.carid, in2.carid, in2.time, in1.dir, in1.seg, in1.pos 
	FROM    lrbm.preAccident1 AS in1, 
		lrbm.preAccident1 AS in2, 
		lrbm.preAccident1 AS in11, 
		lrbm.preAccident1 AS in22 
	WHERE   in2.carid <> in1.carid AND 
		in2.pos = in1.pos AND 
		in2.lane = in1.lane AND 
		in2.dir = in1.dir AND 
		in2.time >= in1.time AND 
		in2.time <= in1.time + 30 AND 
		in11.carid = in1.carid AND 
		in11.pos = in1.pos AND 
		in11.lane = in1.lane AND 
		in11.dir = in1.dir AND 
		in11.time = in1.time + 120 AND 
		in22.carid = in2.carid AND 
		in22.pos = in1.pos AND 
		in22.lane = in1.lane AND 
		in22.dir = in1.dir AND 
		in22.time = in2.time + 120;

	INSERT INTO lrbm.accident 
	SELECT   min(carid1),
		max(carid2),
		((min(time) + 90)/60) + 1, 
		((max(time) + 120)/60) + 1,
		dir, seg, pos 
	FROM    lrbm.preAccident2 
	GROUP BY dir,seg,pos;
END;


CREATE PROCEDURE lrbm.statistics()
BEGIN
	-- how to ensure you only have unique dir/seq/tme triples, use the time window
	INSERT INTO  lrbm.statistics (dir,seg,time_minute,numvehicles,lav,toll,accident,accidentSeg) 
	SELECT dir, seg, tme+1, 0,null,null,null,null 
	FROM 
		(SELECT dir AS dir , seg AS seg, (time/60) AS tme FROM lrbm.statisticsinput  and 
			datacell.window('lrbm.statisticsinput',interval '1' second, interval '1' second) AS tmpT 
	GROUP BY dir,seg,tme;
END;

CREATE PROCEDURE lrbm.extractNumVehicles()
BEGIN
	INSERT INTO  lrbm.preVehicle (dir,seg,time_minute2,numvehicles)   
	SELECT dir, seg, tme+2, count (distinct cnt) 
	FROM (select    dir AS dir , 
			seg AS seg, 
			(time/60) AS tme, 
			carid AS cnt 
	      FROM 	lrbm.input 
	      WHERE 	type = 0) AS tmpT 
	GROUP BY dir,seg,tme;


	UPDATE lrbm.statistics SET numVehicles = (
	SELECT numVehicles 
	FROM   lrbm.preVehicle 
	WHERE  statistics.dir = preVehicle.dir AND
	       statistics.seg = preVehicle.seg AND 
		statistics.time_minute = preVehicle.time_minute2);

	UPDATE lrbm.statistics SET    numVehicles = 0 WHERE  numVehicles IS NULL;
END;

CREATE PROCEDURE lrbm.extractLav()
BEGIN
	INSERT INTO lrbm.prelav(dir, seg, time_minute3, lav ) 
	SELECT dir, seg,  tme, speed 
	FROM  ( SELECT dir, seg,  tme, avg(speed) AS speed
		FROM (  SELECT dir,seg,carid,tme,avg(speed) AS speed 
			FROM	(SELECT  dir AS dir, seg AS seg, carid as carid, (time/60)+1 AS tme,  speed AS speed  
				 FROM lrbm.input  
				 WHERE type=0) AS temp_A  
			GROUP BY dir,seg,carid,tme ) AS temp_B 
		GROUP BY dir,seg,tme) AS temp_C;



	UPDATE lrbm.statistics SET lav = (
	SELECT floor(avg(prelav.lav)) 
	FROM   lrbm.prelav 
	WHERE  statistics.dir = prelav.dir AND 
		statistics.seg = prelav.seg AND 
		prelav.time_minute3 <= statistics.time_minute - 1 AND 
		prelav.time_minute3  >= statistics.time_minute - 5);



	UPDATE lrbm.statistics SET    lav = -2 WHERE  lav IS NULL;

	UPDATE lrbm.statistics SET    toll = -3 WHERE  toll IS NULL;
	UPDATE lrbm.statistics SET    accident = -3 WHERE  accident IS NULL;
	UPDATE lrbm.statistics SET    accidentseg = -3 WHERE  accidentseg IS NULL;
END;

CREATE PROCEDURE lrbm.calculateToll()
BEGIN
-- 5a
	UPDATE lrbm.statistics SET    toll = 0 WHERE  lav >= 40 OR numVehicles <= 50;

-- 5b
	## the following query does not generate the correct mal query plan 
	# check 'where exists'
-- IT CRASHES THE SYSTEM
	UPDATE lrbm.statistics 
	SET    toll = 0, accident = 1 
	WHERE EXISTS( 
		SELECT acc.seg 
		FROM   lrbm.accident AS acc 
		WHERE  acc.dir = statistics.dir AND 
			acc.firstMinute + 1 <= statistics.time_minute AND 
			acc.lastMinute + 1 >= statistics.time_minute AND 
			( 
				( 
					(acc.dir = 0) AND 
					(acc.seg >= statistics.seg) AND 
					(acc.seg <= statistics.seg + 4)  
				) 
				OR 
				( 
					(acc.dir <> 0) AND 
					(acc.seg <= statistics.seg) AND 
					(acc.seg >= statistics.seg - 4)
				)
			)
	);

--5c
	UPDATE lrbm.statistics SET    toll = (2 * (numVehicles - 50) * (numVehicles - 50)) WHERE  toll IS NULL;

-- 5d
-- toll = 0, accident = 1, 
	UPDATE lrbm.statistics 
	SET    
		accidentSeg = 
		(SELECT acc.seg 
		FROM    lrbm.accident AS acc 
		WHERE   acc.dir = statistics.dir AND 
			acc.firstMinute + 1 <= statistics.time_minute AND 
			acc.lastMinute + 1 >= statistics.time_minute AND 
			( 
				( 
					(acc.dir = 0) AND 
					(acc.seg >= statistics.seg) AND 
					(acc.seg <= statistics.seg + 4) 
				) 
				OR 
				( 
					(acc.dir <> 0) AND 
					(acc.seg <= statistics.seg) AND
					(acc.seg >= statistics.seg - 1)
				)
			)
		)
		WHERE statistics.accident = 1;

--5e
	UPDATE lrbm.statistics SET    accident = 0, accidentSeg = -1 WHERE  accident=-3;

-- Note: 5b and 5d contain the same subquery

	SELECT acc.seg 
	FROM   lrbm.accident AS acc 
	WHERE  acc.dir = statistics.dir AND 
		acc.firstMinute + 1 <= statistics.time_minute AND 
		acc.lastMinute + 1 >= statistics.time_minute AND 
		( 
			( 
				(acc.dir = 0) AND 
				(acc.seg >= statistics.seg) AND 
				(acc.seg <= statistics.seg + 4)  
			) 
			OR 
			( 
				(acc.dir <> 0) AND 
				(acc.seg <= statistics.seg) AND 
				(acc.seg >= statistics.seg - 4)
			)
		)

-- We have implemented the nested selection just once
--  and then we have added the appropriate code to also support 5d

END;

CREATE PROCEDURE createAlerts()
BEGIN
	INSERT INTO lrbm.tollAccAlerts(time, carid, dir, seg)
           SELECT  min(time),
                   carid,
                   dir,
                   seg
           FROM    input
           WHERE   type = 0 AND
                   lane <> 4
           GROUP BY dir, seg, carid;

	UPDATE lrbm.tollAccAlerts 
	SET    lav = 
		(SELECT CAST(statistics.lav AS integer) AS lav
		FROM   lrbm.statistics 
		WHERE  statistics.dir = tollAccAlerts.dir AND 
			statistics.seg = tollAccAlerts.seg AND 
			statistics.time_minute = (tollAccAlerts.time/60) + 1);

	UPDATE lrbm.tollAccAlerts 
	SET    toll = 
		(SELECT statistics.toll 
		FROM    lrbm.statistics 
		WHERE   statistics.dir = tollAccAlerts.dir AND 
			statistics.seg = tollAccAlerts.seg AND 
			statistics.time_minute = tollAccAlerts.time/60 + 1);

	UPDATE lrbm.tollAccAlerts 
	SET    accidentSeg = 
		(SELECT statistics.accidentSeg 
		FROM    lrbm.statistics 
		WHERE   statistics.dir = tollAccAlerts.dir AND 
			statistics.seg = tollAccAlerts.seg AND 
			statistics.time_minute = tollAccAlerts.time/60 + 1);
END;

CREATE PROCEDURE splitByType()
BEGIN
	INSERT INTO  lrbm.dailyExpenditureRequests (type,time,carid,xway, qid, day2) 
		SELECT type, time, carid, xway, qid, day2 
		FROM lrbm.input 
		WHERE type=3;

	INSERT INTO  lrbm.accountBalanceRequests (type,time,carid,xway, qid, day2) 
		SELECT type, time, carid, xway, qid, day2 
		FROM lrbm.input 
		WHERE type=2;
END;

CREATE PROCEDURE dailyExpenditureAnswer()
BEGIN
	INSERT INTO  lrbm.dailyExpenditureAnswer (carid, day2, bal, qid) 
	SELECT completehistory.carid AS carid, 
		completehistory.day2 AS day2, 
		completehistory.toll AS bal, 
		dailyExpenditurerequests.qid 
	FROM lrbm.dailyExpenditurerequests 
	INNER JOIN lrbm.completehistory ON 
		(dailyExpenditurerequests.day2 = completehistory.day2) AND 
		(dailyExpenditurerequests.carid = completehistory.carid) AND 
		(dailyExpenditurerequests.xway = completehistory.xway);
END;

CREATE PROCEDURE accountBalanceAnswer()
BEGIN

	DROP TABLE lrbm.accountBalancetimeEq;

	CREATE TABLE lrbm.accountBalancetimeEq ( "carid" int, "qid" int, "querytime" int, "resulttime" int);

	INSERT INTO  lrbm.accountBalancetimeEq (carid, qid, querytime, resulttime) 
		SELECT   t2.carid, t2.qid,  t2.time AS querytime, t2.time AS resulttime
		FROM     lrbm.accountBalancerequests AS t2, lrbm.tollAccAlerts
		WHERE    t2.carid=tollAccAlerts.carid AND
			 t2.time = tollAccAlerts.time
		GROUP BY t2.carid, t2.time, t2.qid
		ORDER BY t2.carid, t2.time;

	DROP TABLE lrbm.accountBalancenow;

	CREATE TABLE lrbm.accountBalancenow ( "carid" int, "querytime" int, "resulttime" int, "qid" int, "toll" int);

	INSERT INTO lrbm.accountBalancenow (carid,querytime,resulttime,qid,toll)
		SELECT  t2.carid, t2.querytime, t2.resulttime, t2.qid, sum(tollAccAlerts.toll) AS toll 
		FROM    lrbm.accountBalancetimeEq AS t2, lrbm.tollAccAlerts 
		WHERE   t2.carid=tollAccAlerts.carid AND 
			t2.resulttime > tollAccAlerts.time 
		GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid 
		ORDER BY t2.carid, t2.querytime;

	DROP TABLE lrbm.accountBalancetime0;

	CREATE TABLE lrbm.accountBalancetime0 ( "carid" int, "qid" int, "querytime" int, "resulttime" int);

	INSERT INTO lrbm.accountBalancetime0 (carid,qid,querytime,resulttime) 
		SELECT t2.carid, t2.qid,  t2.time AS querytime, max (tollAccAlerts.time) AS resulttime 
		FROM   lrbm.accountBalancerequests AS t2, lrbm.tollAccAlerts 
		WHERE   t2.carid=tollAccAlerts.carid AND 
			t2.time >=tollAccAlerts.time AND
			t2.time-30<tollAccAlerts.time 
		GROUP BY t2.carid, t2.time, t2.qid 
		ORDER BY t2.carid, t2.time;

	DROP TABLE lrbm.accountBalancetime1;

	CREATE TABLE lrbm.accountBalancetime1 ( "carid" int, "qid" int, "querytime" int, "resulttime" int);

	INSERT INTO lrbm.accountBalancetime1 (carid, qid, querytime, resulttime) 
		SELECT  t2.carid, t2.qid,  t2.querytime, max(tollAccAlerts.time) AS resulttime 
		FROM    lrbm.accountBalancetime0 AS t2, lrbm.tollAccAlerts 
		WHERE   t2.carid=tollAccAlerts.carid AND 
			 t2.resulttime > tollAccAlerts.time 
		GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid 
		ORDER BY t2.carid, t2.resulttime;
--E
	DROP TABLE lrbm.accountBalancemiddle;
	CREATE TABLE lrbm.accountBalancemiddle ( "carid" int, "querytime" int, "resulttime" int, "qid" int, "toll" int);

	INSERT INTO    lrbm.accountBalancemiddle ( carid, querytime,  resulttime, qid, toll)
	SELECT  t2.carid, t2.querytime as querytime, t2.resulttime as resulttime, t2.qid, sum(tollAccAlerts.toll) AS toll
	FROM    lrbm.accountBalancetime1 AS t2, lrbm.tollAccAlerts
	WHERE   t2.carid=tollAccAlerts.carid AND
			t2.resulttime > tollAccAlerts.time
	GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid;

--F 
	DROP TABLE lrbm.accountBalancetime10;
	CREATE TABLE lrbm.accountBalancetime10 ( "carid" int, "qid" int, "querytime" int, "resulttime" int);

	INSERT INTO lrbm.accountBalancetime10 (carid, qid, querytime, resulttime)
	SELECT   t2.carid, t2.qid,  t2.time as querytime, min (tollAccAlerts.time) as resulttime
	FROM     lrbm.accountBalancerequests AS t2, lrbm.tollAccAlerts
	WHERE    t2.carid=tollAccAlerts.carid AND
		 t2.time >=tollAccAlerts.time AND
		 t2.time-60<tollAccAlerts.time
	GROUP BY t2.carid, t2.time, t2.qid
	ORDER BY t2.carid, t2.time;

--G
    DROP TABLE lrbm.accountBalancetime2_tmp;


    CREATE TABLE lrbm.accountBalancetime2_tmp ( "carid" int, "querytime" int, "resulttime" int, "qid" int, "toll" int, "rest" int);

    INSERT INTO lrbm.accountBalancetime2_tmp (carid,  querytime, resulttime,qid, rest)
        SELECT   t2.carid, t2.querytime, max(tollAccAlerts.time) AS resulttime, t2.qid, t2.resulttime
        FROM     lrbm.accountBalancetime10 AS t2, lrbm.tollAccAlerts
        WHERE    t2.carid=tollAccAlerts.carid AND
             t2.resulttime >= tollAccAlerts.time
        GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid
        ORDER BY t2.carid, t2.resulttime;

    UPDATE lrbm.accountBalancetime2_tmp
    SET toll =0 WHERE toll is NULL;

    DROP TABLE lrbm.accountBalancetime2;

    CREATE TABLE lrbm.accountBalancetime2 ( "carid" int, "querytime" int, "resulttime" int, "qid" int, "toll" int);

    INSERT INTO lrbm.accountBalancetime2 (carid, querytime, resulttime, qid, toll)
        SELECT  t2.carid, t2.querytime, t2.resulttime, t2.qid, t2.toll
        FROM    lrbm.accountBalancetime2_tmp AS t2;

--H
	DROP TABLE lrbm.accountBalancelast;

	CREATE TABLE lrbm.accountBalancelast ( "carid" int, "querytime" int, "resulttime" int, "qid" int, "toll" int);

	INSERT INTO lrbm.accountBalancelast (carid, querytime, resulttime, qid, toll)
	SELECT   t2.carid, t2.querytime AS querytime, t2.resulttime AS resulttime, t2.qid, sum(tollAccAlerts.toll) AS toll
	FROM     lrbm.accountBalancetime2 AS t2, tollAccAlerts
	WHERE    t2.carid=tollAccAlerts.carid AND
			 t2.resulttime > tollAccAlerts.time
	GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid;

--I
	DROP TABLE lrbm.accountBalancetime60;

	CREATE TABLE lrbm.accountBalancetime60 ( "carid" int, "querytime" int, "resulttime" int, "qid" int, "toll" int);

	INSERT INTO  lrbm.accountBalancetime60 ( carid , querytime, resulttime,  qid)
	SELECT t2.carid, t2.time AS querytime, max(tollAccAlerts.time) AS resulttime,  t2.qid
	FROM  lrbm.accountBalancerequests AS t2, lrbm.tollAccAlerts
	WHERE    t2.carid=tollAccAlerts.carid AND
		 t2.time-60 >= tollAccAlerts.time
	GROUP BY t2.carid, t2.time, t2.qid
	ORDER BY t2.carid, t2.time;


	UPDATE lrbm.accountBalancetime60 SET toll =0 WHERE toll IS NULL;

--J
	DELETE FROM lrbm.accountBalancetime60 WHERE qid IN (SELECT qid FROM lrbm.accountBalancetime0);

--K
	DELETE FROM lrbm.accountBalancetime60 WHERE qid IN (SELECT qid FROM lrbm.accountBalancetime10);

--M
	DELETE FROM lrbm.accountBalancetime60 WHERE qid IN (SELECT qid FROM lrbm.accountBalancetime2);

--N 
	DROP TABLE lrbm.accountBalanceancient;

	CREATE TABLE lrbm.accountBalanceancient ( "carid" int, "querytime" int, "resulttime" int, "qid" int, "toll" int);

	INSERT INTO  lrbm.accountBalanceancient ( carid , querytime, resulttime,  qid, toll)
	SELECT t2.carid, t2.querytime AS querytime, t2.resulttime AS resulttime, t2.qid, sum(tollAccAlerts.toll) AS toll 
	FROM  lrbm.accountBalancetime60 AS t2, lrbm.tollAccAlerts
	WHERE  t2.carid=tollAccAlerts.carid AND
			t2.resulttime > tollAccAlerts.time
	GROUP BY t2.carid, t2.querytime, t2.resulttime, t2.qid;

--O
	DELETE FROM lrbm.accountBalancetime60 WHERE qid IN (SELECT qid FROM lrbm.accountBalanceancient);

-- P
	DROP TABLE lrbm.accountBalancequeryatenterance;

	CREATE TABLE lrbm.accountBalancequeryatenterance ( "carid" int, "querytime" int, "resulttime" int, "qid" int, "toll" int);

	INSERT INTO  lrbm.accountBalancequeryatenterance ( carid , querytime, resulttime,  qid)
	SELECT t2.carid, t2.time AS querytime, min (tollAccAlerts.time) AS resulttime,  t2.qid
	FROM  lrbm.accountBalancerequests AS t2, lrbm.tollAccAlerts
	WHERE  t2.carid=tollAccAlerts.carid
	GROUP BY t2.carid, t2.time, t2.qid
	ORDER BY t2.carid, t2.time;

	UPDATE lrbm.accountBalancequeryatenterance SET toll =0 WHERE toll is NULL;

--Q
	DELETE FROM lrbm.accountBalancequeryatenterance WHERE querytime<>resulttime;

--R
	DELETE FROM lrbm.accountBalancetime2 WHERE qid IN (SELECT qid FROM lrbm.accountBalancelast);

--S
	DROP TABLE lrbm.accountBalanceanswer;

	CREATE TABLE lrbm.accountBalanceanswer ( "carid" int, "querytime" int, "resulttime" int, "qid" int, "toll" int);

INSERT INTO  lrbm.accountBalanceanswer ( carid , querytime, resulttime,  qid, toll)
SELECT * FROM lrbm.accountBalancenow UNION
SELECT * FROM lrbm.accountBalancemiddle UNION
SELECT * FROM lrbm.accountBalancelast UNION
SELECT * FROM lrbm.accountBalanceancient UNION
SELECT * FROM lrbm.accountBalancequeryatenterance UNION
SELECT * FROM lrbm.accountBalancetime2 UNION
SELECT * FROM lrbm.accountBalancetime60;

END;

DROP TABLE lrbm.input;
DROP TABLE lrbm.preAccident1;
DROP TABLE lrbm.preAccident2 ;
DROP TABLE lrbm.accident;
DROP TABLE lrbm.statistics;
DROP TABLE lrbm.preVehicle ;
DROP TABLE lrbm.preLav;
DROP TABLE lrbm.TollAccAlerts;
DROP TABLE lrbm.dailyExpenditureAnswer ;
DROP TABLE lrbm.dailyExpenditureRequests ;
DROP TABLE lrbm.completehistory;
DROP TABLE lrbm.accountBalanceRequests ;
DROP TABLE lrbm.statisticsinput;
