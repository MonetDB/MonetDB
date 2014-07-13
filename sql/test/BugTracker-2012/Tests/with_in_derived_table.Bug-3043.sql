SELECT * FROM (
    WITH x AS (SELECT 1)
        SELECT * FROM x
) y;
