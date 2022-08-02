CREATE TABLE tbl_ProductSales (ColID int, Product_Category  varchar(64), Product_Name  varchar(64), TotalSales int); 
CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);

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
GROUP BY GROUPING SETS((Product_Category)); --error, "grouping" not allowed inside aggregation functions

SELECT
    GROUPING(1)
FROM tbl_ProductSales
GROUP BY Product_Category; --error, "grouping" requires group columns as input

SELECT
    col1 IN (SELECT ColID + col2 FROM tbl_ProductSales)
FROM another_T
GROUP BY ROLLUP(col1); --error, col2 is not a grouping column

SELECT
    (SELECT GROUPING(t1.col1) FROM tbl_ProductSales)
FROM another_T t1; --error, PostgreSQL gives: arguments to GROUPING must be grouping expressions of the associated query level

-- Valid GROUPING calls

SELECT
    GROUPING(Product_Name)
FROM tbl_ProductSales;

SELECT
    GROUPING(Product_Name)
FROM tbl_ProductSales GROUP BY ();

SELECT
    GROUPING(Product_Category) AS myalias
FROM tbl_ProductSales
GROUP BY Product_Category;

SELECT
    GROUPING(Product_Category) myalias
FROM tbl_ProductSales
GROUP BY Product_Category, Product_Name;

SELECT
    GROUPING(Product_Name, Product_Category)
FROM tbl_ProductSales
GROUP BY Product_Category, Product_Name;

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
    GROUPING(Product_Category, ColID)
FROM tbl_ProductSales
GROUP BY ROLLUP((Product_Category, Product_Name, ColID));

SELECT
    GROUPING(Product_Category, ColID)
FROM tbl_ProductSales
GROUP BY CUBE((Product_Category, Product_Name, ColID));

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

SELECT
    GROUPING(Product_Category, Product_Name, ColID), GROUPING(Product_Name, ColID)
FROM tbl_ProductSales
GROUP BY CUBE((Product_Category, Product_Name, ColID))
ORDER BY GROUPING(Product_Category, ColID);

SELECT
    GROUPING(Product_Category, Product_Name, ColID) + 1
FROM tbl_ProductSales
GROUP BY ROLLUP(Product_Category, Product_Name, ColID)
HAVING GROUPING(Product_Category, Product_Name, ColID) <> 3
ORDER BY GROUPING(Product_Category, Product_Name, ColID) DESC;

SELECT
    GROUPING(Product_Category), AVG(SUM(TotalSales)) OVER (ROWS UNBOUNDED PRECEDING)
FROM tbl_ProductSales
GROUP BY GROUPING SETS((Product_Category), (Product_Name), (Product_Category, Product_Name), ());

SELECT
    GROUPING(Product_Category), RANK() OVER (PARTITION BY SUM(TotalSales))
FROM tbl_ProductSales
GROUP BY GROUPING SETS((Product_Category), (Product_Name), (Product_Category, Product_Name), ());

SELECT
    CASE WHEN GROUPING(Product_Category, Product_Name, ColID) * 10 = 30 THEN 2 ELSE NULL END
FROM tbl_ProductSales
GROUP BY ROLLUP(Product_Category, Product_Name, ColID);

SELECT
    GROUPING(Product_Category), AVG(SUM(TotalSales)) OVER (ROWS UNBOUNDED PRECEDING), RANK() OVER (PARTITION BY SUM(TotalSales))
FROM tbl_ProductSales
GROUP BY GROUPING SETS((Product_Category), (Product_Name), (Product_Category, Product_Name), ());

SELECT
    GROUPING(Product_Category),
    CAST(SUM(SUM(TotalSales)) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS BIGINT),
    CAST(SUM(GROUPING(Product_Category, Product_Name)) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS BIGINT),
    RANK() OVER (PARTITION BY SUM(ColID))
FROM tbl_ProductSales
GROUP BY GROUPING SETS((Product_Category), (Product_Name), (Product_Category, Product_Name), ());

CREATE TABLE tbl_X (ColID int, NItems int); 
INSERT INTO tbl_X VALUES (1,1000),(2,500),(3,323),(4,0);

SELECT myalias, COUNT(*) FROM
(
    SELECT
        GROUPING(tbl_ProductSales.ColID, tbl_X.ColID) AS myalias
    FROM tbl_ProductSales
    INNER JOIN tbl_X
    ON tbl_ProductSales.ColID = tbl_X.ColID
    WHERE tbl_X.NItems < 1000
    GROUP BY CUBE(tbl_ProductSales.Product_Category, tbl_ProductSales.Product_Name, tbl_ProductSales.ColID), ROLLUP(tbl_X.ColID, tbl_X.NItems)
) AS SubTables GROUP BY myalias ORDER BY myalias;

SELECT
    GROUPING(ColID, ColID)
FROM tbl_ProductSales
INNER JOIN tbl_X
ON tbl_ProductSales.ColID = tbl_X.ColID
GROUP BY CUBE(tbl_ProductSales.Product_Category); --error, ambiguous identifier

SELECT
    GROUPING(tbl_ProductSales.ColID, tbl_X.ColID) AS myalias
FROM tbl_ProductSales
INNER JOIN tbl_X
ON tbl_ProductSales.ColID = tbl_X.ColID
WHERE tbl_X.NItems < 1000
GROUP BY CUBE(Product_Category, Product_Name, tbl_ProductSales.ColID), ROLLUP(tbl_X.ColID, tbl_X.NItems)
ORDER BY SUM(TotalSales) DESC
LIMIT 1;

DROP TABLE tbl_ProductSales;
DROP TABLE tbl_X;
DROP TABLE another_T;
