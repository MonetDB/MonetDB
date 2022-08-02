
CREATE TABLE repro_friends(src BIGINT NOT NULL, dst BIGINT NOT NULL, PRIMARY KEY(src, dst));
CREATE TABLE repro_persons(id BIGINT NOT NULL, firstName VARCHAR(40) NOT NULL);

WITH
    params AS ( /* input parameters */
        SELECT 
           2199023260527 AS id,
           CAST('Lin' AS VARCHAR(40)) AS name
    ),
    friends_1 AS (
        SELECT 1 as "distance", p1."id", p1.firstName
        FROM repro_persons p1, repro_friends f, params p
        WHERE p.id = f."src" AND f."dst" = p1."id"
    ),
    friends_2 AS (
        SELECT * FROM friends_1
      UNION
        SELECT 2 as "distance", p2."id", p2.firstName
        FROM repro_persons p2, repro_friends f
        WHERE f."src" IN ( SELECT "id" FROM friends_1 ) AND f."dst" = p2."id" AND p2.id NOT IN ( SELECT "id" FROM friends_1 )
    ),
    friends_3 AS (
        SELECT * FROM friends_2 f WHERE f.firstName = (SELECT name FROM params)
      UNION
        SELECT 3 as "distance", p3."id", p3.firstName
        FROM repro_persons p3, repro_friends f
        WHERE f."src" IN ( SELECT "id" FROM friends_2 ) AND f."dst" = p3."id" AND p3.id NOT IN ( SELECT "id" FROM friends_2 ) AND p3.firstName = (SELECT name FROM params) 
    ),
    filter AS (
            SELECT * FROM friends_3 f WHERE f.firstName = (SELECT name FROM params)
    )
SELECT * FROM filter ORDER BY "distance";

DROP TABLE repro_friends;
DROP TABLE repro_persons;

