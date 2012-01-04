CREATE TABLE _rank2933 (pre INTEGER, post INTEGER);
INSERT INTO _rank2933 VALUES (22, 37);
INSERT INTO _rank2933 VALUES (23, 24);
INSERT INTO _rank2933 VALUES (33, 34);

SELECT
  count(*)
FROM
  _rank2933 AS _rank1,
  _rank2933 AS _rank2
WHERE
  NOT _rank1.pre = _rank2.pre AND
  EXISTS (SELECT pre FROM _rank2933 AS ancestor WHERE
    ancestor.pre < _rank1.pre AND _rank1.pre < ancestor.post AND
    ancestor.pre < _rank2.pre AND _rank2.pre < ancestor.post);

SELECT
  count(*)
FROM
  _rank2933 AS _rank1,
  _rank2933 AS _rank2
WHERE
  NOT _rank1.pre = _rank2.pre AND
  EXISTS (SELECT pre FROM _rank2933 AS ancestor WHERE
    ancestor.pre < _rank1.pre AND _rank1.pre < ancestor.post 
  INTERSECT SELECT pre FROM _rank2933 AS ancestor WHERE
    ancestor.pre < _rank2.pre AND _rank2.pre < ancestor.post);

DROP TABLE _rank2933;


SELECT 123 AS dummy1 FROM tables n WHERE EXISTS ( SELECT 456 AS dummy2 FROM
	(SELECT * FROM columns) nnn WHERE n.id > 0 );
