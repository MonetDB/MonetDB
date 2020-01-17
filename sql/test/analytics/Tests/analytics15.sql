start transaction;
create table analytics (aa int, bb int);
insert into analytics values (15, 3), (3, 1), (2, 1), (5, 3), (NULL, 2), (3, 2), (4, 1), (6, 3), (8, 2), (NULL, 4);

select covar_samp(aa, aa) over (partition by bb) from analytics;
select covar_samp(aa, aa) over (partition by bb order by bb asc) from analytics;
select covar_samp(aa, aa) over (partition by bb order by bb desc) from analytics;
select covar_samp(aa, aa) over (order by bb desc) from analytics;

select covar_samp(bb, bb) over (partition by bb) from analytics;
select covar_samp(bb, bb) over (partition by bb order by bb asc) from analytics;
select covar_samp(bb, bb) over (partition by bb order by bb desc) from analytics;
select covar_samp(bb, bb) over (order by bb desc) from analytics;


select covar_pop(aa, aa) over (partition by bb) from analytics;
select covar_pop(aa, aa) over (partition by bb order by bb asc) from analytics;
select covar_pop(aa, aa) over (partition by bb order by bb desc) from analytics;
select covar_pop(aa, aa) over (order by bb desc) from analytics;

select covar_pop(bb, bb) over (partition by bb) from analytics;
select covar_pop(bb, bb) over (partition by bb order by bb asc) from analytics;
select covar_pop(bb, bb) over (partition by bb order by bb desc) from analytics;
select covar_pop(bb, bb) over (order by bb desc) from analytics;


select covar_samp(aa, bb) over (partition by bb) from analytics;
select covar_samp(aa, bb) over (partition by bb order by bb asc) from analytics;
select covar_samp(aa, bb) over (partition by bb order by bb desc) from analytics;
select covar_samp(aa, bb) over (order by bb desc) from analytics;

select covar_samp(bb, aa) over (partition by bb) from analytics;
select covar_samp(bb, aa) over (partition by bb order by bb asc) from analytics;
select covar_samp(bb, aa) over (partition by bb order by bb desc) from analytics;
select covar_samp(bb, aa) over (order by bb desc) from analytics;


select covar_pop(aa, bb) over (partition by bb) from analytics;
select covar_pop(aa, bb) over (partition by bb order by bb asc) from analytics;
select covar_pop(aa, bb) over (partition by bb order by bb desc) from analytics;
select covar_pop(aa, bb) over (order by bb desc) from analytics;

select covar_pop(bb, aa) over (partition by bb) from analytics;
select covar_pop(bb, aa) over (partition by bb order by bb asc) from analytics;
select covar_pop(bb, aa) over (partition by bb order by bb desc) from analytics;
select covar_pop(bb, aa) over (order by bb desc) from analytics;


select covar_pop(aa, 1) over (partition by bb) from analytics;
select covar_pop(aa, 1) over (partition by bb order by bb asc) from analytics;
select covar_pop(aa, 1) over (partition by bb order by bb desc) from analytics;
select covar_pop(aa, 1) over (order by bb desc) from analytics;

select covar_pop(bb, -100) over (partition by bb) from analytics;
select covar_pop(bb, -100) over (partition by bb order by bb asc) from analytics;
select covar_pop(bb, -100) over (partition by bb order by bb desc) from analytics;
select covar_pop(bb, -100) over (order by bb desc) from analytics;


select covar_samp(aa, 1) over (partition by bb) from analytics;
select covar_samp(aa, 1) over (partition by bb order by bb asc) from analytics;
select covar_samp(aa, 1) over (partition by bb order by bb desc) from analytics;
select covar_samp(aa, 1) over (order by bb desc) from analytics;

select covar_samp(bb, -100) over (partition by bb) from analytics;
select covar_samp(bb, -100) over (partition by bb order by bb asc) from analytics;
select covar_samp(bb, -100) over (partition by bb order by bb desc) from analytics;
select covar_samp(bb, -100) over (order by bb desc) from analytics;


select covar_pop(aa, aa) over () from analytics;
select covar_pop(bb, bb) over () from analytics;
select covar_pop(aa, bb) over () from analytics;
select covar_pop(bb, aa) over () from analytics;
select covar_pop(aa, 1) over () from analytics;
select covar_pop(aa, 1) over () from analytics;

select covar_samp(aa, aa) over () from analytics;
select covar_samp(bb, bb) over () from analytics;
select covar_samp(aa, bb) over () from analytics;
select covar_samp(bb, aa) over () from analytics;
select covar_samp(aa, 1) over () from analytics;
select covar_samp(bb, -100) over () from analytics;


select covar_samp(NULL, 2) over () from analytics;
select covar_samp(2, NULL) over () from analytics;
select covar_samp(aa, NULL) over () from analytics;
select covar_samp(NULL, aa) over () from analytics;
select covar_samp(NULL, NULL) over () from analytics;

select covar_pop(NULL, 2) over () from analytics;
select covar_pop(2, NULL) over () from analytics;
select covar_pop(aa, NULL) over () from analytics;
select covar_pop(NULL, aa) over () from analytics;
select covar_pop(NULL, NULL) over () from analytics;

rollback;
