CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab0 VALUES(97,1,99), (15,81,47), (87,21,10);

prepare select col0 from tab0 where (?) in (select col0 from tab0);
prepare select col0 from tab0 where (?,?) in (select col0,col1 from tab0);
prepare select col0 from tab0 where (col1,col1) in (select col0,? from tab0);
prepare select col0 from tab0 where (col1,col1) in (select ?,? from tab0);
prepare select col0 from tab0 where (col0) in (?);
prepare select col0 from tab0 where (col0) in (?,?);

prepare select ? < ANY (select max(col0) from tab0) from tab0 t1;
prepare select col0 = ALL (select ? from tab0) from tab0 t1;

prepare select 1 from tab0 where 1 between ? and ?;
prepare select 1 from tab0 where ? between 1 and ?;
prepare select 1 from tab0 where ? between ? and 1;

prepare select EXISTS (SELECT ? FROM tab0) from tab0;
prepare select EXISTS (SELECT ?,? FROM tab0) from tab0;

prepare select col0 from tab0 where (?) in (?); --error
prepare select ? = ALL (select ? from tab0) from tab0 t1; --error
prepare select 1 from tab0 where ? between ? and ?; --error

prepare select case when col0 = 0 then ? else 1 end from tab0;
prepare select case when col0 = 0 then 1 else ? end from tab0;
prepare select case when col0 = 0 then ? else ? end from tab0; --error

prepare select case when col0 = 0 then ? when col0 = 1 then ? else 1 end from tab0;
prepare select case when col0 = 0 then ? when col0 = 1 then ? else ? end from tab0; --error

prepare select ? is null from tab0; --error
prepare select max(?); --error
prepare select max(?) over (); --error

CREATE TABLE tab1(col0 INTEGER, col1 STRING);
prepare select 1 from tab1 where (col0,col1) in (select ?,? from tab1);

drop table tab0;
drop table tab1;
