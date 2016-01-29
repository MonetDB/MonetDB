CREATE TABLE sampleData ( groupID int, numValue int );
INSERT INTO sampleData VALUES ( 1,   1 );
INSERT INTO sampleData VALUES ( 1,   2 );
INSERT INTO sampleData VALUES ( 1,   6 );
INSERT INTO sampleData VALUES ( 1,  16 );
INSERT INTO sampleData VALUES ( 1,   7 );
INSERT INTO sampleData VALUES ( 2,   5 );
INSERT INTO sampleData VALUES ( 2,   5 );
INSERT INTO sampleData VALUES ( 2,   5 );
INSERT INTO sampleData VALUES ( 2,  11 );
INSERT INTO sampleData VALUES ( 3,  10 );
INSERT INTO sampleData VALUES ( 3,  17 );
INSERT INTO sampleData VALUES ( 3,  52 );
INSERT INTO sampleData VALUES ( 3,  66 );
INSERT INTO sampleData VALUES ( 4,  18 );
INSERT INTO sampleData VALUES ( 5,   0 );
INSERT INTO sampleData VALUES ( 5,   0 );
INSERT INTO sampleData VALUES ( 5,   0 );

SELECT count(*) from sampleData;

-- Median tests
SELECT median(numValue) FROM sampleData;  -- should return 6
SELECT median(groupID) FROM sampleData;  -- should return 2
SELECT groupID, median(numValue) FROM sampleData GROUP BY groupID ORDER BY groupId;  -- should return (6, 5, 17, 18,  0)


SELECT R.groupID, AVG(1.0*R.numValue) AS medianValue
FROM
(    SELECT GroupID, numValue, ROW_NUMBER() OVER(PARTITION BY groupID ORDER BY NumValue) AS rowno
    FROM sampleData
) R
INNER JOIN
(    SELECT GroupID, 1+count(*) as N
    FROM sampleData
    GROUP BY GroupID
) G
ON R.GroupID = G.GroupID AND R.rowNo BETWEEN N/2 AND N/2+N%2
GROUP BY R.groupID ORDER by R.groupID;

SELECT R.groupID, sqrt(SUM((R.n-G.a)*(R.n-G.a))/count(*)) AS stdev
FROM
(    SELECT GroupID, NumValue as n
    FROM sampleData
) R
INNER JOIN
(    SELECT GroupID, AVG(numValue) as a
    FROM sampleData
    GROUP BY GroupID
) G
ON R.GroupID = G.GroupID 
GROUP BY R.groupID ORDER BY R.groupID;

drop table sampleData;
