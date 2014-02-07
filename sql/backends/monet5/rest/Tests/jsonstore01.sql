CREATE TABLE json_first (
  _id uuid, _rev VARCHAR(34),
  deleted BOOLEAN,
  js json);

CREATE FUNCTION first_update_doc
( doc_id VARCHAR(36),
  doc json )
  RETURNS TABLE ( OK BOOLEAN )
BEGIN
 DECLARE ISNEW INTEGER;
 DECLARE VERSION INT;
 DECLARE NEWVER VARCHAR(6);
 SET ISNEW = (SELECT
  COUNT(*) FROM json_first
  WHERE _id = doc_id);
   IF (ISNEW = 0) THEN
    SET NEWVER = '1';
   ELSE
    SET VERSION = (
     SELECT MAX(
      CAST(
       SUBSTRING(_rev,
        1,POSITION('-'
         IN _rev) - 1)
       AS INT) + 1)
     FROM json_first
     WHERE _id =
      doc_id);
     SET NEWVER =
      CAST(VERSION AS
       VARCHAR(6));
   END IF;
  INSERT INTO json_first (
   _id, _rev, deleted, js )
  VALUES ( doc_id,
   CONCAT(NEWVER,
    CONCAT('-',
     md5(doc))),
   FALSE,
   doc );
  RETURN
   SELECT TRUE;
END;

INSERT INTO json_first (_id, _rev, deleted, js)
VALUES ('4b5b0c91-61f3-46db-b279-1535c8e2bd41', concat('1-', md5('{}')), FALSE, '{}');

SELECT * FROM json_first;

WITH curr_first(maxrev, _id) AS (
  SELECT MAX(CAST(SUBSTRING(_rev,1,POSITION('-' IN _rev) - 1) AS INT)), 
         _id
  FROM json_first
  GROUP BY _id)
SELECT json_first._id,
       json_first._rev,
       json_first.js
FROM curr_first,
     json_first
WHERE curr_first._id = json_first._id
AND json_first.deleted = FALSE
AND curr_first.maxrev = CAST(SUBSTRING(_rev,1,POSITION('-' IN _rev) - 1) AS INT);

SELECT * FROM json_first WHERE _id = '4b5b0c91-61f3-46db-b279-1535c8e2bd41';

WITH curr_first(maxrev, _id) AS (
  SELECT MAX(CAST(SUBSTRING(_rev,1,POSITION('-' IN _rev) - 1) AS INT)), 
         _id
  FROM json_first
  WHERE _id = '4b5b0c91-61f3-46db-b279-1535c8e2bd41'
  GROUP BY _id)
SELECT json_first._id,
       json_first._rev,
       json_first.js
FROM curr_first,
     json_first
WHERE curr_first._id = json_first._id
AND json_first.deleted = FALSE
AND curr_first.maxrev = CAST(SUBSTRING(_rev,1,POSITION('-' IN _rev) - 1) AS INT);

SELECT * FROM first_update_doc ('4b5b0c91-61f3-46db-b279-1535c8e2bd41', '{"message":"hello world"}');

INSERT INTO json_first (_id, _rev, deleted, js) 
VALUES ('4b5b0c91-61f3-46db-b279-1535c8e2bd42', concat('1-', md5('{}')), FALSE, '{}');

UPDATE json_first SET deleted = TRUE WHERE _id = '4b5b0c91-61f3-46db-b279-1535c8e2bd41';

SELECT * FROM json_first;

WITH curr_first(maxrev, _id) AS (
  SELECT MAX(CAST(SUBSTRING(_rev,1,POSITION('-' IN _rev) - 1) AS INT)), 
         _id
  FROM json_first
  GROUP BY _id) 
SELECT json_first._id, 
       json_first._rev, 
       json_first.js 
FROM curr_first, 
     json_first
WHERE curr_first._id = json_first._id
AND json_first.deleted = FALSE
AND curr_first.maxrev = CAST(SUBSTRING(_rev,1,POSITION('-' IN _rev) - 1) AS INT);

SELECT * FROM json_first WHERE _id = '4b5b0c91-61f3-46db-b279-1535c8e2bd41';

WITH curr_first(maxrev, _id) AS (
  SELECT MAX(CAST(SUBSTRING(_rev,1,POSITION('-' IN _rev) - 1) AS INT)), 
         _id
  FROM json_first
  WHERE _id = '4b5b0c91-61f3-46db-b279-1535c8e2bd41'
  GROUP BY _id) 
SELECT json_first._id, 
       json_first._rev, 
       json_first.js 
FROM curr_first, 
     json_first
WHERE curr_first._id = json_first._id
AND json_first.deleted = FALSE
AND curr_first.maxrev = CAST(SUBSTRING(_rev,1,POSITION('-' IN _rev) - 1) AS INT);

DROP FUNCTION first_update_doc;
DROP TABLE json_first;
