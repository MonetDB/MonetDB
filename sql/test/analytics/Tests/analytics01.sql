start transaction;
create table analytics (aa int, bb int, cc bigint);
insert into analytics values (15, 3, 15), (3, 1, 3), (2, 1, 2), (5, 3, 5), (NULL, 2, NULL), (3, 2, 3), (4, 1, 4), (6, 3, 6), (8, 2, 8), (NULL, 4, NULL);

select percent_rank() over (partition by aa) from analytics;
select percent_rank() over (partition by aa order by aa asc) from analytics;
select percent_rank() over (partition by aa order by aa desc) from analytics;
select percent_rank() over (order by aa) from analytics;
select percent_rank() over (order by aa desc) from analytics;

select percent_rank() over (partition by bb) from analytics;
select percent_rank() over (partition by bb order by bb asc) from analytics;
select percent_rank() over (partition by bb order by bb desc) from analytics;
select percent_rank() over (order by bb) from analytics;
select percent_rank() over (order by bb desc) from analytics;

rollback;
