
CREATE TABLE "treeitems" (
        "tree" CHARACTER LARGE OBJECT,
        "pre"  BIGINT
);
INSERT INTO treeitems VALUES('documentStructure', 0);
INSERT INTO treeitems VALUES('documentStructure', 1);


-- Query 1
SELECT *
FROM  treeitems AS a,
      treeitems AS b
WHERE a.pre > b.pre
AND   a.tree = 'documentStructure'
AND   b.tree = 'documentStructure';

-- Query 2
SELECT * 
FROM  treeitems AS a,
      treeitems AS b
WHERE a.pre > b.pre
AND   a.tree = 'documentStructure'
AND   b.tree = a.tree;

DROP TABLE treeitems;
