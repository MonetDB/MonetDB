START TRANSACTION;
CREATE TABLE tbl_ProductSales (ColID int, Product_Category clob, Product_Name clob, TotalSales int); 
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);

SELECT 
    CAST(SUM(TotalSales) as BIGINT) AS TotalSales
FROM tbl_ProductSales
GROUP BY ();

/*SELECT
    CAST(SUM(TotalSales) as BIGINT) AS TotalSales
FROM tbl_ProductSales
GROUP BY Product_Category, Product_Name;

SELECT
    CAST(SUM(TotalSales) as BIGINT) AS TotalSales
FROM tbl_ProductSales
GROUP BY Product_Category;

SELECT
    CAST(SUM(TotalSales) as BIGINT) AS TotalSales
FROM tbl_ProductSales;

SELECT
    CAST(SUM(TotalSales) as BIGINT) AS TotalSales
FROM tbl_ProductSales
GROUP BY ROLLUP (Product_Category, Product_Name);

SELECT
    CAST(SUM(TotalSales) as BIGINT) AS TotalSales
FROM tbl_ProductSales
GROUP BY Product_Category, ROLLUP (Product_Category, Product_Name);*/
ROLLBACK;
