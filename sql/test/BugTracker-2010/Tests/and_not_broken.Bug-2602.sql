CREATE TABLE "tbl1" (
         "id"          int           NOT NULL,
         "id1"         int           NOT NULL,
         "id2"         int           NOT NULL,
         "name"        varchar(300)  NOT NULL,
         CONSTRAINT "test_id_pkey" PRIMARY KEY ("id"),
         CONSTRAINT "test_id1_name_unique" UNIQUE ("id1", "name")
);


INSERT INTO tbl1 VALUES (1, 1, 1, 'one');

SELECT (1) as "a" FROM tbl1 WHERE id1 = 2;
SELECT (1) as "a" FROM tbl1 WHERE ( id1 = 1 AND name = 'one' AND NOT id = 1);

DROP TABLE "tbl1";
