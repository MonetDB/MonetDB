CREATE TABLE tbl_ProductSales (ColID int, Product_Category clob, Product_Name clob, TotalSales int); 
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);

SELECT 
    CAST(SUM(TotalSales) as BIGINT) AS TotalSales
FROM tbl_ProductSales
GROUP BY (); --global aggregate

-- ROLLUP

SELECT 
    CAST(SUM(TotalSales) as BIGINT) AS TotalSales
FROM tbl_ProductSales
GROUP BY ROLLUP (); --error, rollup must have at least one column

SELECT
    Product_Category, CAST(SUM(TotalSales) as BIGINT) AS TotalSales
FROM tbl_ProductSales
GROUP BY ROLLUP(Product_Category);

SELECT
    Product_Name, CAST(SUM(TotalSales) as BIGINT) AS TotalSales
FROM tbl_ProductSales
GROUP BY ROLLUP(Product_Name);

SELECT
    Product_Category, Product_Name, CAST(SUM(TotalSales) as BIGINT) AS TotalSales
FROM tbl_ProductSales
GROUP BY ROLLUP(Product_Category, Product_Name);

SELECT
    Product_Category, Product_Name, CAST(SUM(TotalSales) as BIGINT) AS TotalSales
FROM tbl_ProductSales
GROUP BY ROLLUP(Product_Category, Product_Name) HAVING SUM(TotalSales) > 400;

-- CUBE

SELECT 
    CAST(SUM(TotalSales) as BIGINT) AS TotalSales
FROM tbl_ProductSales
GROUP BY CUBE(); --error, cube must have at least one column

SELECT 
    Product_Category, CAST(SUM(TotalSales) as BIGINT) AS TotalSales FROM tbl_ProductSales
GROUP BY CUBE(Product_Category);

SELECT
    Product_Name, CAST(SUM(TotalSales) as BIGINT) AS TotalSales
FROM tbl_ProductSales
GROUP BY CUBE(Product_Name);

SELECT
    Product_Category, Product_Name, CAST(SUM(TotalSales) as BIGINT) AS TotalSales
FROM tbl_ProductSales
GROUP BY CUBE(Product_Category, Product_Name);

SELECT
    Product_Category, Product_Name, CAST(SUM(TotalSales) as BIGINT) AS TotalSales
FROM tbl_ProductSales
GROUP BY CUBE(Product_Category, Product_Name) ORDER BY Product_Category, Product_Name;

DROP TABLE tbl_ProductSales;
