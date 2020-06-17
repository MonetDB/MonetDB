start transaction;

create table myy (col1 int, col2 int);
insert into myy values (1, 1), (2, 0), (3,3), (4,2);
select distinct col1 + col2 from myy order by col1 + col2;
plan select distinct col1 + col2 from myy order by col1 + col2;

create table myx (x uuid, y uuid);
plan select * from myx where x in ('1aea00e5db6e0810b554fde31d961965') or y = '1aea00e5db6e0810b554fde31d961965';
plan select * from myx where x in ('1aea00e5db6e0810b554fde31d961965') or y is null;

plan select * from myx where x in ('1aea00e5db6e0810b554fde31d961965', '1aea00e5db6e0810b554fde31d961966') or y = '1aea00e5db6e0810b554fde31d961967';

insert into myx values ('1aea00e5db6e0810b554fde31d961965', '1aea00e5db6e0810b554fde31d961967');
select * from myx where x in ('1aea00e5db6e0810b554fde31d961966') or y is not null;
select * from myx where x in ('1aea00e5db6e0810b554fde31d961966') or y is null;

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
rollback;
