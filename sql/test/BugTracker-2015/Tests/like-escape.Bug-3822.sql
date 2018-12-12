select '_' like E'\\_';
select '_' like E'\\_' escape E'\\';
select * from (select '_' as foo) AS t0 WHERE foo LIKE E'\\_';
select * from (select '_' as foo) AS t0 WHERE foo LIKE E'\\_' escape E'\\';
