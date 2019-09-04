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

SELECT
    GROUPING(Product_Name)
FROM tbl_ProductSales; --error, same as upper one

SELECT
    1
FROM tbl_ProductSales
GROUP BY GROUPING(Product_Name); --error, "grouping" not allowed inside GROUP BY

SELECT
    1
FROM tbl_ProductSales
WHERE GROUPING(Product_Category) > 1
GROUP BY GROUPING SETS((Product_Category)); --error, "grouping" not allowed in where clause

SELECT 
    AVG(GROUPING(Product_Category))
FROM tbl_ProductSales
GROUP BY GROUPING SETS((Product_Category)) --error, "grouping" not allowed inside aggregation functions

-- GROUPING calls

SELECT
    GROUPING(Product_Category) AS myalias
FROM tbl_ProductSales
GROUP BY Product_Category;

SELECT
    GROUPING(Product_Category) AS myalias
FROM tbl_ProductSales
GROUP BY Product_Category, Product_Name;

SELECT
    GROUPING(Product_Name, Product_Category)
FROM tbl_ProductSales
GROUP BY Product_Category, Product_Name;

-- With ROLLUP, CUBE and GROUPING SETS, the "grouping" outputs non-zero values

SELECT
    GROUPING(Product_Category) AS myalias
FROM tbl_ProductSales
GROUP BY ROLLUP(Product_Category);

SELECT
    GROUPING(Product_Category) AS myalias
FROM tbl_ProductSales
GROUP BY Product_Category, ROLLUP(Product_Category);

SELECT
    GROUPING(Product_Category, Product_Name, ColID)
FROM tbl_ProductSales
GROUP BY ROLLUP(Product_Category, Product_Name, ColID);

SELECT
    GROUPING(Product_Category, Product_Name, ColID)
FROM tbl_ProductSales
GROUP BY ROLLUP((Product_Category, Product_Name, ColID));

SELECT
    GROUPING(Product_Category)
FROM tbl_ProductSales
GROUP BY GROUPING SETS((Product_Category), (Product_Name), (Product_Category, Product_Name), ())
ORDER BY GROUPING(Product_Category);

SELECT
    GROUPING(Product_Category)
FROM tbl_ProductSales
GROUP BY GROUPING SETS((Product_Category), (Product_Name), (Product_Category, Product_Name), ())
HAVING GROUPING(Product_Category) = 0;

DROP TABLE tbl_ProductSales;
