CREATE TABLE CASE_TBL (
i integer,
f double precision
);

CREATE TABLE CASE2_TBL (
i integer,
j integer
);

SELECT *
FROM CASE_TBL a, CASE2_TBL b
WHERE COALESCE(a.f, b.i, b.j) = 2;

drop table case_tbl;
drop table case2_tbl;

