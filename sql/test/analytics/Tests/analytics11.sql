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
GROUP BY GROUPING SETS((Product_Category)); --error, "grouping" not allowed inside aggregation functions

SELECT
    GROUPING(1)
FROM tbl_ProductSales
GROUP BY Product_Category; --error, "grouping" requires group columns as input

SELECT
    GROUPING(Product_Category) AS myalias
FROM tbl_ProductSales
GROUP BY Product_Category; --error, requires ROLLUP, CUBE or GROUPING SETS

SELECT
    GROUPING(Product_Category) myalias
FROM tbl_ProductSales
GROUP BY Product_Category, Product_Name; --error, requires ROLLUP, CUBE or GROUPING SETS

SELECT
    GROUPING(Product_Name, Product_Category)
FROM tbl_ProductSales
GROUP BY Product_Category, Product_Name; --error, requires ROLLUP, CUBE or GROUPING SETS

-- Valid GROUPING calls

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
    SUM(SUM(TotalSales)) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 
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

CREATE TABLE another_T (col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT);
INSERT INTO another_T VALUES (1,2,3,4,5,6,7,8), (11,22,33,44,55,66,77,88), (111,222,333,444,555,666,777,888), (1111,2222,3333,4444,5555,6666,7777,8888);

SELECT
    col1 IN (SELECT ColID FROM tbl_ProductSales)
FROM another_T
GROUP BY ROLLUP(col1);

SELECT
    col1 IN (SELECT ColID + col1 FROM tbl_ProductSales)
FROM another_T
GROUP BY ROLLUP(col1);

SELECT
    col1 IN (SELECT SUM(ColID + col1) FROM tbl_ProductSales)
FROM another_T
GROUP BY ROLLUP(col1);

SELECT
    col3 > ALL (SELECT 1 FROM tbl_ProductSales HAVING MIN(col4) > 30)
FROM another_T
GROUP BY ROLLUP(col3, col4);

SELECT
    col1 = ALL (SELECT 1 FROM tbl_ProductSales HAVING MIN(col2) IS NULL)
FROM another_T
GROUP BY CUBE(col1, col2);

SELECT
    SUM(col1) IN (SELECT DISTINCT col2 FROM another_T GROUP BY col2)
FROM another_T
GROUP BY CUBE(col4);

SELECT
    CASE WHEN NOT col1 NOT IN (SELECT (SELECT MAX(col7)) UNION (SELECT MIN(ColID) FROM tbl_ProductSales LEFT JOIN another_T t2 ON t2.col5 = t2.col1)) THEN 1 ELSE 2 END
FROM another_T t1
GROUP BY col1;

SELECT
    t1.col1 IN (SELECT ColID FROM tbl_ProductSales GROUP BY CUBE(t1.col1, tbl_ProductSales.ColID))
FROM another_T t1
GROUP BY CUBE(col1, col2);

SELECT
    CASE WHEN NOT t1.col2 NOT IN (SELECT (SELECT MAX(t1.col7)) UNION (SELECT MIN(ColID) FROM tbl_ProductSales LEFT JOIN another_T t2 ON t2.col5 = t1.col1)) THEN 1 ELSE 2 END,
    CASE WHEN NOT t1.col2 NOT IN (SELECT (SELECT MAX(t1.col7)) UNION (SELECT MIN(ColID) FROM tbl_ProductSales LEFT JOIN another_T t2 ON MIN(t1.col5) = t1.col1)) THEN 1 ELSE 2 END,
    CASE WHEN NOT t1.col2 NOT IN (SELECT (SELECT MAX(t1.col7)) UNION (SELECT MIN(ColID) FROM tbl_ProductSales tp LEFT JOIN another_T t2 ON tp.ColID = t1.col1 AND tp.ColID = t2.col2)) THEN 1 ELSE 2 END
FROM another_T t1
GROUP BY CUBE(t1.col1, t1.col2);

SELECT
    GROUPING(t1.col6, t1.col7) IN (SELECT SUM(t2.col2) FROM another_T t2 GROUP BY t2.col5),
    NOT 32 * GROUPING(t1.col7, t1.col6) IN (SELECT MAX(t2.col2) FROM another_T t2),
    GROUPING(t1.col6, t1.col7) NOT IN (SELECT MIN(t2.col2) FROM another_T t2 GROUP BY t1.col6),
    NOT SUM(t1.col2) * GROUPING(t1.col6, t1.col6, t1.col6, t1.col6) NOT IN (SELECT MAX(t2.col6) FROM another_T t2 GROUP BY t1.col6 HAVING t1.col7 + MIN(t2.col8) < MAX(t2.col7 - t1.col6)),
    GROUPING(t1.col6) <> ANY (SELECT t1.col7 INTERSECT SELECT t1.col6),
    GROUPING(t1.col7) = ALL (SELECT GROUPING(t1.col6) UNION ALL SELECT 10 * MIN(t1.col8))
FROM another_T t1
GROUP BY CUBE(t1.col7, t1.col6);

SELECT
    NOT col1 * col5 = ALL (SELECT 1 FROM tbl_ProductSales HAVING MAX(col2) > 2),
    NOT AVG(col2) * col1 <> ANY (SELECT 20 FROM tbl_ProductSales HAVING MAX(col1) IS NULL),
    NOT EXISTS (SELECT ColID - 12 FROM tbl_ProductSales GROUP BY ColID HAVING MAX(col1) IS NOT NULL OR MIN(col1) < MIN(col2)),
    CAST (NOT col1 IN (SELECT col2 FROM another_T GROUP BY col2) AS INTEGER) | CAST (col2 IN (SELECT col2 FROM another_T GROUP BY col2) AS INTEGER),
    CAST (EXISTS (SELECT MAX(col5) * MAX(col4) FROM another_T GROUP BY col5, col4) AS INTEGER) & CAST (GROUPING(col1, col5) IN (SELECT DISTINCT col2 FROM another_T GROUP BY col2) AS INTEGER)
FROM another_T
GROUP BY CUBE(col1, col2, col5);

SELECT
    DISTINCT
    NOT col1 * col5 = ALL (SELECT 1 FROM tbl_ProductSales HAVING MAX(col2) > 2),
    NOT AVG(col2) * col1 <> ANY (SELECT 20 FROM tbl_ProductSales HAVING MAX(col1) IS NULL),
    NOT EXISTS (SELECT ColID - 12 FROM tbl_ProductSales GROUP BY ColID HAVING MAX(col1) IS NOT NULL OR MIN(col1) < MIN(col2)),
    CAST (NOT col1 IN (SELECT col2 FROM another_T GROUP BY col2) AS INTEGER) | CAST (col2 IN (SELECT col2 FROM another_T GROUP BY col2) AS INTEGER),
    CAST (EXISTS (SELECT MAX(col5) * MAX(col4) FROM another_T GROUP BY col5, col4) AS INTEGER) & CAST (GROUPING(col1, col5) IN (SELECT DISTINCT col2 FROM another_T GROUP BY col2) AS INTEGER)
FROM another_T
GROUP BY CUBE(col1, col2, col5);

SELECT
    GROUPING(col1, col2, col5, col8),
    col1 IN (SELECT ColID + col2 FROM tbl_ProductSales),
    col1 < ANY (SELECT MAX(ColID + col2) FROM tbl_ProductSales),
    col5 = ALL (SELECT 1 FROM tbl_ProductSales HAVING MIN(col8) IS NULL),
    EXISTS (SELECT col2 FROM tbl_ProductSales WHERE tbl_ProductSales.ColID = another_T.col1),
    col1 + col5 = (SELECT MIN(ColID) FROM tbl_ProductSales),
    CAST(SUM(DISTINCT CASE WHEN col5 - col8 = (SELECT MIN(ColID / col2) FROM tbl_ProductSales) THEN col2 - 5 ELSE ABS(col1) END) AS BIGINT),
    (SELECT MAX(ColID + col2) FROM tbl_ProductSales) * DENSE_RANK() OVER (PARTITION BY AVG(DISTINCT col5)),
    GROUPING(col1, col5, col8) * MIN(col8) OVER (PARTITION BY col5 ORDER BY col1 ROWS UNBOUNDED PRECEDING) evil,
    MAX(col3) / 10 + GROUPING(col1, col5, col2) * 10,
    GROUP_CONCAT(CAST(col4 AS VARCHAR(32)), '-sep-') || ' plus ' || GROUPING(col1),
    NTILE(col1) OVER (ORDER BY col8 DESC),
    col2 * NULL
FROM another_T
GROUP BY CUBE(col1, col2, col5, col8), GROUPING SETS (());

SELECT
    col1 IN (SELECT ColID + col2 FROM tbl_ProductSales)
FROM another_T
GROUP BY ROLLUP(col1); --error, col2 is not a grouping column

SELECT
    GROUPING(col1, col2, col3, col4, col5, col6, col7, col8), AVG(col1), CAST(SUM(col2) * 3 AS BIGINT), col3 + col4,
    CAST(MAX(col5) * MIN(col6) AS BIGINT), col7, col1 IN (SELECT ColID FROM tbl_ProductSales), col2 IN (SELECT ColID + col3 FROM tbl_ProductSales)
FROM another_T
GROUP BY ROLLUP(col1, col2, col3, col4, col5, col6, col7, col8) --with 8 columns, a smallint is necessary for grouping's output
ORDER BY GROUPING(col1, col2, col3, col4, col5, col6, col7, col8);

DROP TABLE tbl_ProductSales;
DROP TABLE tbl_X;
DROP TABLE another_T;
