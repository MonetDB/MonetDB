CREATE TABLE tbl_ProductSales (ColID int, Product_Category  varchar(64), Product_Name  varchar(64), TotalSales int); 
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);
CREATE TABLE integers(i INTEGER);
INSERT INTO integers VALUES (1), (2), (3), (NULL);

START TRANSACTION;
-- Bug 6883
select 1 from integers where (((0.7161494983624688) in (integers.i)) = true) = false;
	-- 1
	-- 1
	-- 1

delete from integers where (((0.7161494983624688) in (integers.i)) = true) = false;
	-- 3 rows affected
select i from integers;
	-- NULL

CREATE TABLE t0 (a1 INT, a2 int, a3 int); --Bug 6884
UPDATE t0 SET a2 = (- (- (t0.a1))), a3 = (ascii(CAST(ascii('}悂Y8K*韖5<c>^n8_X1X|p(''bX') AS STRING(920)))) WHERE NOT (((0.27023923567918073) NOT IN (0.9149447665258329)) = FALSE);
	--0 rows affected

insert into integers(i) values(((length(reverse('8 \rcr੧[bp1eMY쫺4j5s뮯!<Rn4*}')))>>(((- (- (528640982)))&(ascii('528640982')))))); --Bug 6885
SELECT i from integers order by i;
	-- NULL
	-- 0
select reverse('8 \rcr੧[bp1eMY쫺4j5s뮯!<Rn4*}');

-- Bug 6886
INSERT INTO another_t(col4) VALUES(-589206676), (-1557408577);
DELETE FROM another_t WHERE ((another_t.col8)<=(+ (another_t.col8)));
ALTER TABLE another_t ADD UNIQUE(col8, col1, col6, col3);
ROLLBACK;

DROP TABLE tbl_ProductSales;
DROP TABLE another_T;
DROP TABLE integers;
