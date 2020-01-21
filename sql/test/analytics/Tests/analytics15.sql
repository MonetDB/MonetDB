start transaction;
create table analytics (aa int, bb int);
insert into analytics values (15, 3), (3, 1), (2, 1), (5, 3), (NULL, 2), (3, 2), (4, 1), (6, 3), (8, 2), (NULL, 4);

select covar_samp(aa, aa) over (partition by bb),
       covar_samp(aa, aa) over (partition by bb order by bb asc),
       covar_samp(aa, aa) over (partition by bb order by bb desc),
       covar_samp(aa, aa) over (order by bb desc) from analytics;

select covar_samp(bb, bb) over (partition by bb),
       covar_samp(bb, bb) over (partition by bb order by bb asc),
       covar_samp(bb, bb) over (partition by bb order by bb desc),
       covar_samp(bb, bb) over (order by bb desc) from analytics;

select corr(bb, bb) over (partition by bb),
       corr(bb, bb) over (partition by bb order by bb asc),
       corr(bb, bb) over (partition by bb order by bb desc),
       corr(bb, bb) over (order by bb desc) from analytics;


select covar_pop(aa, aa) over (partition by bb),
       covar_pop(aa, aa) over (partition by bb order by bb asc),
       covar_pop(aa, aa) over (partition by bb order by bb desc),
       covar_pop(aa, aa) over (order by bb desc) from analytics;

select covar_pop(bb, bb) over (partition by bb),
       covar_pop(bb, bb) over (partition by bb order by bb asc),
       covar_pop(bb, bb) over (partition by bb order by bb desc),
       covar_pop(bb, bb) over (order by bb desc) from analytics;

select corr(bb, bb) over (partition by bb),
       corr(bb, bb) over (partition by bb order by bb asc),
       corr(bb, bb) over (partition by bb order by bb desc),
       corr(bb, bb) over (order by bb desc) from analytics;


select covar_samp(aa, bb) over (partition by bb),
       covar_samp(aa, bb) over (partition by bb order by bb asc),
       covar_samp(aa, bb) over (partition by bb order by bb desc),
       covar_samp(aa, bb) over (order by bb desc) from analytics;

select covar_samp(bb, aa) over (partition by bb),
       covar_samp(bb, aa) over (partition by bb order by bb asc),
       covar_samp(bb, aa) over (partition by bb order by bb desc),
       covar_samp(bb, aa) over (order by bb desc) from analytics;

select corr(bb, aa) over (partition by bb),
       corr(bb, aa) over (partition by bb order by bb asc),
       corr(bb, aa) over (partition by bb order by bb desc),
       corr(bb, aa) over (order by bb desc) from analytics;


select covar_pop(aa, bb) over (partition by bb),
       covar_pop(aa, bb) over (partition by bb order by bb asc),
       covar_pop(aa, bb) over (partition by bb order by bb desc),
       covar_pop(aa, bb) over (order by bb desc) from analytics;

select covar_pop(bb, aa) over (partition by bb),
       covar_pop(bb, aa) over (partition by bb order by bb asc),
       covar_pop(bb, aa) over (partition by bb order by bb desc),
       covar_pop(bb, aa) over (order by bb desc) from analytics;

select corr(bb, aa) over (partition by bb),
       corr(bb, aa) over (partition by bb order by bb asc),
       corr(bb, aa) over (partition by bb order by bb desc),
       corr(bb, aa) over (order by bb desc) from analytics;


select covar_pop(aa, 1) over (partition by bb),
       covar_pop(aa, 1) over (partition by bb order by bb asc),
       covar_pop(aa, 1) over (partition by bb order by bb desc),
       covar_pop(aa, 1) over (order by bb desc) from analytics;

select covar_pop(bb, -100) over (partition by bb),
       covar_pop(bb, -100) over (partition by bb order by bb asc),
       covar_pop(bb, -100) over (partition by bb order by bb desc),
       covar_pop(bb, -100) over (order by bb desc) from analytics;

select corr(bb, -100) over (partition by bb),
       corr(bb, -100) over (partition by bb order by bb asc),
       corr(bb, -100) over (partition by bb order by bb desc),
       corr(bb, -100) over (order by bb desc) from analytics;


select covar_samp(aa, 1) over (partition by bb),
       covar_samp(aa, 1) over (partition by bb order by bb asc),
       covar_samp(aa, 1) over (partition by bb order by bb desc),
       covar_samp(aa, 1) over (order by bb desc) from analytics;

select covar_samp(bb, -100) over (partition by bb),
       covar_samp(bb, -100) over (partition by bb order by bb asc),
       covar_samp(bb, -100) over (partition by bb order by bb desc),
       covar_samp(bb, -100) over (order by bb desc) from analytics;

select corr(bb, -100) over (partition by bb),
       corr(bb, -100) over (partition by bb order by bb asc),
       corr(bb, -100) over (partition by bb order by bb desc),
       corr(bb, -100) over (order by bb desc) from analytics;


select covar_pop(aa, aa) over (),
       covar_pop(bb, bb) over (),
       covar_pop(aa, bb) over (),
       covar_pop(bb, aa) over (),
       covar_pop(aa, 1) over (),
       covar_pop(aa, 1) over () from analytics;

select covar_samp(aa, aa) over (),
       covar_samp(bb, bb) over (),
       covar_samp(aa, bb) over (),
       covar_samp(bb, aa) over (),
       covar_samp(aa, 1) over (),
       covar_samp(bb, -100) over () from analytics;

select corr(aa, aa) over (),
       corr(bb, bb) over (),
       corr(aa, bb) over (),
       corr(bb, aa) over (),
       corr(aa, 1) over (),
       corr(bb, -100) over () from analytics;


select covar_samp(NULL, 2) over (),
       covar_samp(2, NULL) over (),
       covar_samp(aa, NULL) over (),
       covar_samp(NULL, aa) over (),
       covar_samp(NULL, NULL) over () from analytics;

select covar_pop(NULL, 2) over (),
       covar_pop(2, NULL) over (),
       covar_pop(aa, NULL) over (),
       covar_pop(NULL, aa) over (),
       covar_pop(NULL, NULL) over () from analytics;

select corr(NULL, 2) over (),
       corr(2, NULL) over (),
       corr(aa, NULL) over (),
       corr(NULL, aa) over (),
       corr(NULL, NULL) over () from analytics;

rollback;
