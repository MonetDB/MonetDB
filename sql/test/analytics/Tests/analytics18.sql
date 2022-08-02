START TRANSACTION;
CREATE TABLE tab1 (
    col1 varchar(100) NOT NULL,
    col2 varchar(100) NOT NULL,
    col3 int NOT NULL,
    col4 int,
    col5 int,
    col6 varchar(20),
    col11 decimal(13,4)
);

CREATE SEQUENCE "myseq";
CREATE TABLE tab2 (
    col4 int DEFAULT next value for "myseq" NOT NULL,
    col1 varchar(100) NOT NULL,
    col7 varchar(100),
    col8 int,
    col9 int,
    col10 int,
    CONSTRAINT myconstraint PRIMARY KEY (col4)
);

SELECT
tab2.col10,
tab2.col7,
tab2.col1,
tab2.col8,
col2,
col3,
tab2.col4 as col4,
case when col11 like '%-%' and col3 < 20190101 then -1
when col5=6 and tab2.col8 in (2,4,6,8,10,12,14,16,18,20) and col3 >= 20190101 then 1
when col5=6 and tab2.col8 in (1,3,5,7,9,11,13,15,17,21) and col3 >= 20190101 then -1
else 1 END as something,
RANK() OVER mywindow as rnk1
FROM tab2
JOIN tab1
ON tab2.col4 = tab1.col4
WINDOW mywindow as (
PARTITION BY
tab2.col10,
tab2.col7,
tab2.col1,
col2,
col3
ORDER BY
tab2.col9 DESC,
case when col11 like '%-%' and col3 < 20190101 then -1
when col5=6 and tab2.col8 in (2,4,6,8,10,12,14,16,18,20) and col3 >= 20190101 then 1
when col5=6 and tab2.col8 in (1,3,5,7,9,11,13,15,17,21) and col3 >= 20190101 then -1
else 1 END DESC,
tab2.col4 DESC
)
where
((col5<>0 and col3 >= 20190101) or col3 < 20190101)
and tab2.col10 = 5
and tab1.col6 is null
and col2 <> 'HEHEHEHE';
ROLLBACK;
