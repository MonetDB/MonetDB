CREATE TABLE tbl_ProductSales (ColID int, Product_Category  varchar(64), Product_Name  varchar(64), TotalSales int); 
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);

SELECT 1
FROM tbl_ProductSales
GROUP BY Product_Category;

SELECT 1
FROM tbl_ProductSales
GROUP BY ();

SELECT
    GROUPING()
FROM tbl_ProductSales
GROUP BY Product_Category; --error, "grouping" requires arguments

SELECT
    GROUPING(Product_Name)
FROM tbl_ProductSales GROUP BY (); --error, Product_Name it's not a grouping column

DROP TABLE tbl_ProductSales;
