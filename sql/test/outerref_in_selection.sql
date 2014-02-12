create table capediff (
        cape_date date,
        num_at_cape integer
);

SELECT cape_date,(
    SELECT sum(a.num_at_cape)
        FROM capediff AS a
        WHERE a.cape_date <= b.cape_date) AS numppl
    FROM capediff AS b;
