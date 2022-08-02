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

create table tbls (id int);
insert into tbls values (42);
insert into tbls select * from tbls;
insert into tbls select * from tbls;
insert into tbls select * from tbls;
create table clmns (id int);
insert into clmns values (42);
insert into clmns select * from clmns;
insert into clmns select * from clmns;
insert into clmns select * from clmns;
SELECT 123 AS dummy1 FROM tbls n WHERE EXISTS ( SELECT 456 AS dummy2 FROM
	(SELECT * FROM clmns) nnn WHERE n.id > 0 );

drop table clmns;
drop table tbls;
