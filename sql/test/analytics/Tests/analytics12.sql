START TRANSACTION;

CREATE TABLE tbl_ProductSales (ColID int, Product_Category  varchar(64), Product_Name  varchar(64), TotalSales int); 
INSERT INTO tbl_ProductSales VALUES (1,'Game','Mobo Game',200),(2,'Game','PKO Game',400),(3,'Fashion','Shirt',500),(4,'Fashion','Shorts',100);

SELECT
    GROUPING(Product_Category, Product_Name, ColID), GROUPING(ColID, Product_Category, Product_Name),
    GROUPING(Product_Category, Product_Name), GROUPING(Product_Name, Product_Category)
FROM tbl_ProductSales
GROUP BY ROLLUP(Product_Category, Product_Name, ColID);

SELECT
    GROUPING(Product_Name, Product_Name),
    GROUPING(Product_Category, ColID),
    GROUPING(ColID, Product_Category),
    GROUPING(Product_Category) + GROUPING(Product_Category, Product_Name) + GROUPING(Product_Category, Product_Name, ColID),
    CAST(SUM(ColID) AS BIGINT)
FROM tbl_ProductSales
GROUP BY ROLLUP(Product_Category, Product_Name, ColID);

WITH "groupings" AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY ColID) as "rows",
        GROUPING(Product_Category, Product_Name, ColID) AS col1, GROUPING(ColID, Product_Category, Product_Name) AS col2,
        GROUPING(Product_Category, Product_Name) col3, GROUPING(Product_Name, Product_Category) "col4"
    FROM tbl_ProductSales
    GROUP BY GROUPING SETS ( Product_Category, (Product_Name), ColID,
                            ROLLUP (Product_Category, ColID),
                            CUBE (Product_Name, Product_Category),
                            () )
) SELECT "rows", col1, col2, col3, col4 FROM "groupings" ORDER BY col1, col2, col3, col4;

ROLLBACK;
