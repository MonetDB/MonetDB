start transaction;
create table analytics (aa int, bb int, cc bigint);
insert into analytics values (15, 3, 15), (3, 1, 3), (2, 1, 2), (5, 3, 5), (NULL, 2, NULL), (3, 2, 3), (4, 1, 4), (6, 3, 6), (8, 2, 8), (NULL, 4, NULL);
create table stressme (aa varchar(64), bb int);
insert into stressme values ('one', 1), ('another', 1), ('stress', 1), (NULL, 2), ('ok', 2), ('check', 3), ('me', 3), ('please', 3), (NULL, 4);

prepare select count(*) over (rows ? preceding) from analytics;
exec **(2);

prepare select max(aa) over (rows between 5 preceding and ? following) from stressme;
exec **(2);

prepare select max(aa) over (order by bb range between ? preceding and 10 following) from stressme;
exec **(2);

rollback;
