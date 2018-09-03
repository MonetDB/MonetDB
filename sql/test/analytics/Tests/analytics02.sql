start transaction;

create table analytics (aa int, bb int, cc bigint);
insert into analytics values (15, 3, 15), (3, 1, 3), (2, 1, 2), (5, 3, 5), (NULL, 2, NULL), (3, 2, 3), (4, 1, 4), (6, 3, 6), (8, 2, 8), (NULL, 4, NULL);

select cast (sum(aa) over (rows between 5 preceding and 0 following) as bigint) from analytics;
select cast (sum(aa) over (rows between 5 preceding and 2 following) as bigint) from analytics;
select cast (sum(aa) over (partition by bb order by bb rows between 5 preceding and 0 following) as bigint) from analytics;
select cast (sum(aa) over (partition by bb order by bb rows between 5 preceding and 2 following) as bigint) from analytics;

create table debugme (aa real, bb int);
insert into debugme values (15, 3), (3, 1), (2, 1), (5, 3), (NULL, 2), (3, 2), (4, 1), (6, 3), (8, 2), (NULL, 4);

select cast (sum(aa) over (rows between 2 preceding and 0 following) as bigint) from debugme;
select cast (sum(aa) over (rows between 2 preceding and 2 following) as bigint) from debugme;
select cast (sum(aa) over (partition by bb order by bb rows between 2 preceding and 0 following) as bigint) from debugme;
select cast (sum(aa) over (partition by bb order by bb rows between 2 preceding and 2 following) as bigint) from debugme;

rollback;
