select 'avg(sqrt(n8)) == 1.1', avg(cast(1.1 as real)) from generate_series(cast(0 as integer), 100000000, 1);
