--
-- LIMIT
-- Check the LIMIT/OFFSET feature of SELECT
-- using the wisconsin test set

SELECT '' AS two, unique1, unique2, stringu1 
		FROM onek WHERE unique1 > 50 
		ORDER BY unique1 LIMIT 2;
SELECT '' AS five, unique1, unique2, stringu1 
		FROM onek WHERE unique1 > 60 
		ORDER BY unique1 LIMIT 5;
SELECT '' AS two, unique1, unique2, stringu1 
		FROM onek WHERE unique1 > 60 AND unique1 < 63
		ORDER BY unique1 LIMIT 5;
SELECT '' AS three, unique1, unique2, stringu1 
		FROM onek WHERE unique1 > 100 
		ORDER BY unique1 LIMIT 3 OFFSET 20;
SELECT '' AS zero, unique1, unique2, stringu1 
		FROM onek WHERE unique1 < 50 
		ORDER BY unique1 DESC LIMIT 8 OFFSET 99;
SELECT '' AS eleven, unique1, unique2, stringu1 
		FROM onek WHERE unique1 < 50 
		ORDER BY unique1 DESC LIMIT 20 OFFSET 39;
SELECT '' AS ten, unique1, unique2, stringu1 
		FROM onek
		ORDER BY unique1 OFFSET 990;
SELECT '' AS five, unique1, unique2, stringu1 
		FROM onek
		ORDER BY unique1 OFFSET 990 LIMIT 5;
SELECT '' AS five, unique1, unique2, stringu1 
		FROM onek
		ORDER BY unique1 LIMIT 5 OFFSET 900;
