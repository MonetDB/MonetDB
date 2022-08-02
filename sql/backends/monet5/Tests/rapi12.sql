START TRANSACTION;

CREATE TABLE observations (subject int, age int, height int, weight int);
insert into observations values (1, 30, 180, 75), (2, 60, 190, 85), (3, 7, 100, 40), (4, 48, 196, 597);

SELECT * FROM observations;

CREATE FUNCTION widetolong(subject int, age int, height int, weight int) RETURNS TABLE (subject int, key string, value int) LANGUAGE R {
	dd <- data.frame(subject, age, height, weight)
	if (length(subject) < 2) stop("What do we want? Vectorization! When do we want it? Now!")
	do.call(rbind, 
		lapply(split(dd, dd$subject), 
			function(split) data.frame(
				subject=rep(split$subject, 3), 
				key=c("age", "height", "weight"), 
				value=c(split$age, split$height, split$weight))))
};

SELECT * FROM widetolong( (SELECT * FROM observations AS o) );
--SELECT * FROM widetolong(observations);

-- result should look like this (without the row number)
--    subject    key value
-- 1        1    age    30
-- 2        1 height   180
-- 3        1 weight    75
-- 4        2    age    60
-- 5        2 height   190
-- 6        2 weight    85
-- 7        3    age     7
-- 8        3 height   100
-- 9        3 weight    40
-- 10       4    age    48
-- 11       4 height   196
-- 12       4 weight   597

DROP FUNCTION widetolong;
DROP TABLE observations;
ROLLBACK;
