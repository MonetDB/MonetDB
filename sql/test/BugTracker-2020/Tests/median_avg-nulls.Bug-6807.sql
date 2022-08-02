SELECT median_avg(c1) FROM (VALUES (1), (10)) t1(c1);
SELECT median_avg(c1) FROM (VALUES (1), (NULL)) t1(c1);
SELECT quantile_avg(c1, 0.2) FROM (VALUES (1), (NULL)) t1(c1);
SELECT quantile_avg(c1, 0.1) FROM (VALUES (1), (10)) t1(c1);

SELECT c2, median_avg(c1) FROM (VALUES (1, 1), (NULL, 1), (1, 2), (2, 2), (NULL, 3), (NULL, 3)) t1(c1,c2) GROUP BY c2;
SELECT c2, quantile_avg(c1, 0.7) FROM (VALUES (1, 1), (NULL, 1), (1, 2), (2, 2), (NULL, 3), (NULL, 3)) t1(c1,c2) GROUP BY c2;
