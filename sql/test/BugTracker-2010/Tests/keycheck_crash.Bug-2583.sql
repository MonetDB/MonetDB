CREATE SCHEMA "test";
CREATE TABLE "test"."tbl1" (
         "id"          int           NOT NULL,
         "id1"         int           NOT NULL,
         "id2"         int           NOT NULL,
         "name"        varchar(300)  NOT NULL,
         CONSTRAINT "test_id_pkey" PRIMARY KEY ("id"),
         CONSTRAINT "test_id1_name_unique" UNIQUE ("id1", "name")
);


SELECT
        (1) AS "a"
FROM
        "test"."tbl1"
WHERE
        "tbl1"."name" =  'some text'  AND
        "tbl1"."id1"   = 1  AND NOT
        ("tbl1"."id2" =  295)
;

DROP TABLE "test"."tbl1";
DROP SCHEMA "test";
