CREATE TABLE obale (nm VARCHAR(99) );
INSERT INTO obale VALUES ('a'), ('b'), ('b'), (NULL);
SELECT nm FROM obale ORDER BY 1 desc;
SELECT nm FROM obale ORDER BY upper(nm);
SELECT nm FROM obale ORDER BY nm || nm;
SELECT nm as alias1 FROM obale ORDER BY alias1, nm;    -- no problemo

SELECT nm as alias1 FROM obale ORDER BY upper(alias1);
-- retuns error: SELECT: identifier 'alias1' unknown

SELECT nm as alias1 FROM obale ORDER BY nm || alias1;
-- retuns error: SELECT: identifier 'alias1' unknown

SELECT nm, upper(nm) as alias1 FROM obale ORDER BY alias1;    -- no problemo
SELECT nm, nm||nm as alias1 FROM obale ORDER BY alias1;    -- no problemo

SELECT nm, COUNT(nm) countnm, COUNT(DISTINCT nm) countdnm FROM obale GROUP BY nm ORDER BY countnm desc, countdnm;    -- no problemo

SELECT nm, COUNT(nm) countnm, COUNT(DISTINCT nm) countdnm FROM obale GROUP BY nm ORDER BY countdnm - countnm;
-- retuns error: SELECT: identifier 'countdnm' unknown

DROP TABLE obale;

