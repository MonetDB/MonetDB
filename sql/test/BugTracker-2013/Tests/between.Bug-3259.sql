set optimizer = 'sequential_pipe';

START TRANSACTION;

CREATE TABLE treeitems (
	    "tree"    CHARACTER LARGE OBJECT,
	    "subject" INTEGER,
	    "pre"     BIGINT,
	    "post"    BIGINT,
	    "size"    BIGINT,
	    "level"   TINYINT,
	    "prob"    DOUBLE        DEFAULT 1.0,
	    CONSTRAINT "treeitems_tree_pre_unique" UNIQUE ("tree", "pre"),
	    CONSTRAINT "treeitems_tree_post_unique" UNIQUE ("tree", "post")
);

insert into treeitems values('sequoia',1,2,2,2,2,2.0);

--explain
--SELECT t1.subject as id1, t2.subject as id2
--FROM  treeitems t1, treeitems t2
--WHERE t2.pre between t1.pre and t1.pre + t1.size;

SELECT t1.subject as id1, t2.subject as id2
FROM  treeitems t1, treeitems t2
WHERE t2.pre between t1.pre and t1.pre + t1.size;
ROLLBACK;
