start transaction;

set optimizer='inline,remap,evaluate,costModel,coercions,emptySet,aliases,mergetable,deadcode,constants,commonTerms,reorder,joinPath,deadcode,recycle,reduce,dataflow,history,multiplex,garbageCollector';

CREATE TABLE y (x int);
INSERT INTO y VALUES (10);
SELECT * FROM y;

rollback;


start transaction;

CREATE TABLE y (x int);
INSERT INTO y VALUES (13);
SELECT * FROM y;

rollback;
