CREATE TABLE test (ID BIGINT, UUID STRING, sec BIGINT, data VARCHAR(1000));
INSERT INTO test VALUES
  (1000000, 'uuid0000', 1621539934, 'a0' )
, (1000003, 'uuid0003', 1621539934, 'a3' );

CREATE TABLE extra (ID BIGINT, UUID STRING, sec BIGINT, extra VARCHAR(1000));
INSERT INTO extra VALUES
  (1000009, 'uuid0009', 1621539934, 'a9' )
, (1000009, 'uuid0009', 1621539934, 'a9' )
, (1000009, 'uuid0009', 1621539934, 'a9' )
, (1000009, 'uuid0009', 1621539934, 'a9' )

, (1000000, 'uuid0000', 1621539934, 'a0' )
, (1000000, 'uuid0000', 1621539934, 'a0' )
, (1000000, 'uuid0000', 1621539934, 'a0' )
, (1000000, 'uuid0000', 1621539934, 'a0' )

, (1000003, 'uuid0003', 1621539934, 'a3' )
, (1000003, 'uuid0003', 1621539934, 'a3' )
, (1000003, 'uuid0003', 1621539934, 'a3' )
, (1000003, 'uuid0003', 1621539934, 'a3' );

-- validate 4x of each id.
SELECT id, COUNT(*) as cnt FROM extra GROUP BY id ORDER BY id;

WITH ca AS
(
        SELECT e.ID as id
             , e.UUID as uuid
             , e.sec as sec
          FROM extra AS e
LEFT OUTER JOIN test AS t
            ON t.ID = e.ID
           AND t.UUID = e.UUID
           AND t.sec = e.sec
         WHERE t.ID IS NULL
           AND t.UUID IS NULL
           AND t.sec IS NULL
)
DELETE
FROM extra AS i
WHERE i.id = ca.id
  AND i.sec = ca.sec
  AND i.uuid = ca.uuid
; -- 16 affected rows!

-- Count is -4!
SELECT COUNT(*) FROM extra;
-- Count is 12!
select table, column, type, count from sys.storage() where table = 'extra';

-- clean up
DROP TABLE IF EXISTS test;
DROP TABLE IF EXISTS extra;

