START TRANSACTION;
CREATE TABLE categories (id INTEGER, "description" TEXT);
CREATE TABLE sections (id INTEGER, "description" TEXT);
CREATE TABLE products (id INTEGER, categoryid int, sectionid INTEGER, "description" TEXT, price DECIMAL(6,2));
CREATE TABLE sales (productid INTEGER, sale_day DATE, units INTEGER);

INSERT INTO categories VALUES (1, 'fresh food'), (2, 'dry food'), (3, 'drinks');
INSERT INTO sections VALUES (1, 'front'), (2, 'side'), (3, 'back');
INSERT INTO products VALUES (1, 1, 1, 'apples', 1.5), (2, 1, 2, 'melons', 4.0), (3, 2, 2, 'peanuts', 2.0), (4, 3, 1, 'water', 1.0), (5, 3, 3, 'wine', 5.0), (6, 2, 3, 'walnuts', 1.5);
INSERT INTO sales VALUES (1, date '2020-03-01', 10), (2, date '2020-03-01', 3), (4, date '2020-03-01', 4), (1, date '2020-03-02', 6), (4, date '2020-03-02', 5), (5, date '2020-03-02', 2), (1, date '2020-03-03', 7), (3, date '2020-03-03', 4), (2, date '2020-03-03', 3), (5, date '2020-03-03', 1), (6, date '2020-03-03', 1);

SELECT COALESCE(products."description", 'all_products') AS "description", 
       COALESCE(sale_day, 'all_days') AS sale_day, 
       CAST(totals.total AS DECIMAL (12,4))
FROM (
    SELECT productid, sale_day, SUM(units * price) AS total 
    FROM products
    LEFT JOIN sales ON sales.productid = products.id
    GROUP BY ROLLUP(productid, sale_day)
) AS totals
LEFT JOIN products ON products.id = totals.productid
ORDER BY sale_day NULLS LAST, productid NULLS LAST;

SELECT COALESCE(sections."description", 'all_sections') AS section,
       COALESCE(categories."description", 'all_categories') AS category,
       CAST(totals.total AS DECIMAL (12,4))
FROM (
    SELECT categoryid, sectionid, SUM(units * price) AS total 
    FROM products
    LEFT JOIN sales ON sales.productid = products.id
    GROUP BY GROUPING SETS ((categoryid), (sectionid), ())
) AS totals
LEFT JOIN categories ON totals.categoryid = categories.id
LEFT JOIN sections ON totals.sectionid = sections.id;

CREATE VIEW sales_totals AS
    SELECT GROUPING(categoryid) AS category_aggregates, 
           GROUPING(sectionid) AS section_aggregates, 
           categoryid, sectionid, SUM(units * price) AS total 
    FROM products
    LEFT JOIN sales ON sales.productid = products.id
    GROUP BY GROUPING SETS ((categoryid), (sectionid), ());

SELECT "description", CAST(total AS DECIMAL (12,4))
FROM sales_totals
LEFT JOIN categories ON sales_totals.categoryid = categories.id
WHERE category_aggregates = 0;

SELECT * FROM categories GROUP BY (); --error

ROLLBACK;
