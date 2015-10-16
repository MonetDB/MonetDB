select '_' like '\\_';
select '_' like '\\_' escape '\\';
select * from (select '_' as foo) AS t0 WHERE foo LIKE '\\_';
select * from (select '_' as foo) AS t0 WHERE foo LIKE '\\_' escape '\\';
