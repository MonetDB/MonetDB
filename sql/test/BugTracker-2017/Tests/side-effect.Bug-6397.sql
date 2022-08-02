-- this query gives inconsistent answers when run multiple times.
CREATE FUNCTION my_generate_series(start INT, finish INT)
RETURNS TABLE (value INT)
BEGIN
    DECLARE TABLE tmp_generate_series(value INT);
    DECLARE i INT;
    SET i = start;
    WHILE (i <= finish) DO
        INSERT INTO tmp_generate_series VALUES(i);
        SET i = i + 1;
    END WHILE;
    RETURN tmp_generate_series;
END;

explain select count(*) from my_generate_series(1,5) as t1,
my_generate_series(1,100) as t2;

select count(*) from my_generate_series(1,5) as t1,
my_generate_series(1,100) as t2;
select count(*) from my_generate_series(1,5) as t1,
my_generate_series(1,100) as t2;
select count(*) from my_generate_series(1,5) as t1,
my_generate_series(1,100) as t2;
