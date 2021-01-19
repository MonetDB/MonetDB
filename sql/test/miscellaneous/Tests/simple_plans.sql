set optimizer='sequential_pipe';

start transaction;

create table myy (col1 int, col2 int);
insert into myy values (1, 1), (2, 0), (3,3), (4,2);
select distinct col1 + col2 from myy order by col1 + col2;
plan select distinct col1 + col2 from myy order by col1 + col2;
plan select col2 from myy order by col1 ASC, col1 DESC;
plan select col2 from myy order by col1 DESC, col1 DESC;

create table myx (x uuid, y uuid);
plan select * from myx where x in ('1aea00e5db6e0810b554fde31d961965') or y = '1aea00e5db6e0810b554fde31d961965';
plan select * from myx where x in ('1aea00e5db6e0810b554fde31d961965') or y is null;

plan select * from myx where x in ('1aea00e5db6e0810b554fde31d961965', '1aea00e5db6e0810b554fde31d961966') or y = '1aea00e5db6e0810b554fde31d961967';

insert into myx values ('1aea00e5db6e0810b554fde31d961965', '1aea00e5db6e0810b554fde31d961967');
select * from myx where x in ('1aea00e5db6e0810b554fde31d961966') or y is not null;
select * from myx where x in ('1aea00e5db6e0810b554fde31d961966') or y is null;

CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);
INSERT INTO tab0 VALUES (97,1,99), (15,81,47),(87,21,10);

/* x = 1 or x = 2 => x in (1, 2)*/

plan select 1 from tab0 where col1 = 1 or col1 = 81;

plan select 1 from tab0 where col1 = 1 or col1 = 81 or col1 = 100;

plan select 1 from tab0 where (col1 = 1 or col1 = 81) or (col2 < 0); 

plan select 1 from tab0 where ((col1 = 1 or col1 = 81) or col1 = 100) or (col2 > 10);

plan select 1 from tab0 where ((col1 = 1 or col1 = 81) or col2 = 100) or (col1 = 100); --the rightmost comparison to col1 cannot be merged to the other 2

/* x <> 1 and x <> 2 => x not in (1, 2)*/

plan select 1 from tab0 where col1 <> 1 and col1 <> 81;

plan select 1 from tab0 where col1 <> 1 and col1 <> 81 and (col2 < 0); 

plan select 1 from tab0 where (col1 <> 1 and col1 <> 81) or (col2 < 0); 

plan select 1 from tab0 where ((col1 <> 1 and col1 <> 81) and col1 <> 100) or (col2 > 10);

plan select 1 from tab0 where ((col1 <> 1 and col1 <> 81) or col2 = 100) and (col1 <> 100); --the rightmost comparison to col1 cannot be merged to the other 2

CREATE TABLE tbl_ProductSales (ColID int, Product_Category  varchar(64), Product_Name  varchar(64), TotalSales int); 
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);

-- make sure the following explains don't show crossproducts!
EXPLAIN SELECT 1 FROM another_t t1 INNER JOIN another_t t2 ON t1.col1 BETWEEN t2.col1 AND t2.col1;
EXPLAIN SELECT 1 FROM another_t t1 INNER JOIN another_t t2 ON t1.col1 BETWEEN t2.col1 - 1 AND t2.col1 + 1;
EXPLAIN SELECT 1 FROM another_t t1 INNER JOIN another_t t2 ON t1.col1 BETWEEN t2.col1 AND 2;
EXPLAIN SELECT 1 FROM tbl_productsales t1 INNER JOIN tbl_productsales t2 ON t1.product_category LIKE t2.product_category;

EXPLAIN SELECT 1 FROM another_t t1 INNER JOIN another_t t2 ON t1.col1 > t2.col1;

/* Make sure the range comparisons are optimized into BETWEEN */
PLAN SELECT 1 FROM another_t WHERE (col1 >= 1 AND col1 <= 2) OR col2 IS NULL;
PLAN SELECT (col1 >= 1 AND col1 <= 2) OR col2 IS NULL FROM another_t;

/* Make sure no cross-products are generated */
CREATE TABLE tabel1 (id_nr INT, dt_sur STRING, edg INT, ede DATE, pc_nml_hur STRING, srt_ukr STRING);
INSERT INTO tabel1 VALUES (10, 'Koning', NULL, current_date - INTERVAL '1' MONTH, '50', '01'), (20, 'Nes', NULL, NULL, '50', '01');
CREATE TABLE tabel2 (id_nr INT, dt_sur STRING, edg INT, ede DATE, pc_nml_hur STRING);
INSERT INTO tabel2 VALUES (10, 'Koning', NULL, current_date - INTERVAL '1' MONTH, '50'), (10, 'Koning', NULL, NULL, '50'), (20, 'Nes', NULL, NULL, '50');
CREATE TABLE tabel3 (id_nr INT, my_date DATE);
INSERT INTO tabel3 VALUES (10, current_date - INTERVAL '1' MONTH), (10, NULL), (20, NULL);
create view view1 as SELECT * FROM tabel1 as a;
create view view2 as SELECT * FROM tabel2 as a;
create view view3 as SELECT * FROM tabel3 as a;
PLAN SELECT 1 FROM view1 s INNER JOIN view2 h ON s.id_nr = h.id_nr LEFT JOIN view2 h2 ON h.id_nr = h2.id_nr INNER JOIN view3 a ON a.id_nr = s.id_nr;

-- optimize (a = b) or (a is null and b is null) -> a = b with null semantics
CREATE TABLE integers(i INTEGER, j INTEGER);
INSERT INTO integers VALUES (1,4), (2,5), (3,6), (NULL,NULL);

plan select i1.i, i2.i from integers i1 inner join integers i2 on i1.i = i2.i or (i1.i is null and i2.i is null);
select i1.i, i2.i from integers i1 inner join integers i2 on i1.i = i2.i or (i1.i is null and i2.i is null);

plan select i1.i, i2.i from integers i1 full outer join integers i2 on (i1.i is null and i2.i is null) or i1.i = i2.i;
select i1.i, i2.i from integers i1 full outer join integers i2 on (i1.i is null and i2.i is null) or i1.i = i2.i;

plan select i, j from integers where i = j or (j is null and i is null);
select i, j from integers where i = j or (j is null and i is null);
rollback;

set optimizer='default_pipe';
