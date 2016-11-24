START TRANSACTION;

CREATE TABLE places(
    id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    url VARCHAR(2000) NOT NULL,
    "type" VARCHAR(40) NOT NULL, 
    isPartOf BIGINT
);

CREATE TABLE friends(
    src BIGINT NOT NULL,
    dst BIGINT NOT NULL,
    creationDate TIMESTAMP(3) WITH TIME ZONE NOT NULL,
    PRIMARY KEY(src, dst)
);

CREATE TABLE persons(
    id BIGINT NOT NULL PRIMARY KEY,
    firstName VARCHAR(40) NOT NULL ,
    lastName VARCHAR(40) NOT NULL,
    gender VARCHAR(40) NOT NULL,
    birthDay DATE NOT NULL,
    creationDate TIMESTAMP(3) WITH TIME ZONE NOT NULL,
    locationIP VARCHAR(40) NOT NULL,
    browserUsed VARCHAR(40) NOT NULL,
    place_id BIGINT NOT NULL
);

ALTER TABLE friends ADD FOREIGN KEY(src) REFERENCES persons(id);
ALTER TABLE friends ADD FOREIGN KEY(dst) REFERENCES persons(id);
ALTER TABLE places ADD FOREIGN KEY(isPartOf) REFERENCES places(id);
ALTER TABLE persons ADD FOREIGN KEY(place_id) REFERENCES places(id);

WITH
params AS (
  SELECT id, countryX, countryY, startDate, startDate + CAST (duration AS INTERVAL DAY) AS endDate FROM (
    SELECT
      4398046512167 AS id,
      'United_Kingdom' AS countryX,
      'United_States' AS countryY,
      CAST('2011-11-05' AS TIMESTAMP(3)) AS startDate,
      365 AS duration
  ) AS tmp
),
countries AS (
    SELECT id FROM places WHERE type = 'country' AND name = (SELECT countryX from params)
  UNION
    SELECT id FROM places WHERE type = 'country' AND name = (SELECT countryY from params)
),
friends_id1 AS (
  SELECT f.dst AS id FROM friends f WHERE f.src = (SELECT id FROM params)
),
friends_id2 AS (
    SELECT f.dst AS id FROM friends f WHERE f.src IN (SELECT id FROM friends_id1) AND f.dst <> (SELECT id FROM params)
   UNION
    SELECT * FROM friends_id1
),
candidates AS (
    SELECT p.id
    FROM friends_id2 f, persons p, places city, places country
    WHERE
      f.id = p.id AND p.place_id = city.id AND city.ispartof NOT IN (SELECT * FROM countries)
)
SELECT * FROM candidates;

ROLLBACK;

-- comment 4 reproduction example
START TRANSACTION;
CREATE TABLE subt (A1 BIGINT);
CREATE TABLE t (B BIGINT);

SELECT (B IN (SELECT A1 FROM subt)) FROM t;

ROLLBACK;

