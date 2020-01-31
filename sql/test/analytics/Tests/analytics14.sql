start transaction;
create table analytics (aa int, bb int, cc bigint);
insert into analytics values (15, 3, 15), (3, 1, 3), (2, 1, 2), (5, 3, 5), (NULL, 2, NULL), (3, 2, 3), (4, 1, 4), (6, 3, 6), (8, 2, 8), (NULL, 4, NULL);

select stddev_samp(aa) over (partition by bb),
       stddev_samp(aa) over (partition by bb order by bb asc),
       stddev_samp(aa) over (partition by bb order by bb desc),
       stddev_samp(aa) over (order by bb desc) from analytics;

select stddev_samp(cc) over (partition by bb),
       stddev_samp(cc) over (partition by bb order by bb asc),
       stddev_samp(cc) over (partition by bb order by bb desc),
       stddev_samp(cc) over (order by bb desc) from analytics;


select stddev_pop(aa) over (partition by bb),
       stddev_pop(aa) over (partition by bb order by bb asc),
       stddev_pop(aa) over (partition by bb order by bb desc),
       stddev_pop(aa) over (order by bb desc) from analytics;

select stddev_pop(cc) over (partition by bb),
       stddev_pop(cc) over (partition by bb order by bb asc),
       stddev_pop(cc) over (partition by bb order by bb desc),
       stddev_pop(cc) over (order by bb desc) from analytics;


select var_samp(aa) over (partition by bb),
       var_samp(aa) over (partition by bb order by bb asc),
       var_samp(aa) over (partition by bb order by bb desc),
       var_samp(aa) over (order by bb desc) from analytics;

select var_samp(cc) over (partition by bb),
       var_samp(cc) over (partition by bb order by bb asc),
       var_samp(cc) over (partition by bb order by bb desc),
       var_samp(cc) over (order by bb desc) from analytics;


select var_pop(aa) over (partition by bb),
       var_pop(aa) over (partition by bb order by bb asc),
       var_pop(aa) over (partition by bb order by bb desc),
       var_pop(aa) over (order by bb desc) from analytics;

select var_pop(cc) over (partition by bb),
       var_pop(cc) over (partition by bb order by bb asc),
       var_pop(cc) over (partition by bb order by bb desc),
       var_pop(cc) over (order by bb desc) from analytics;


select stddev_samp(aa) over (),
       stddev_pop(aa) over (),
       var_samp(aa) over (),
       var_pop(aa) over () from analytics;


select stddev_samp(NULL) over (),
       stddev_pop(NULL) over (),
       var_samp(NULL) over (),
       var_pop(NULL) over () from analytics;

rollback;

create table stressme (aa varchar(64), bb int);
insert into stressme values ('one', 1), ('another', 1), ('stress', 1), (NULL, 2), ('ok', 2), ('check', 3), ('me', 3), ('please', 3), (NULL, 4);

select stddev_samp(aa) over (partition by bb) from stressme; --error, stddev_samp not available for string type
select stddev_samp() over (partition by bb) from stressme; --error, stddev_samp without parameters not available
select stddev_samp(*) over (partition by bb) from stressme; --error, stddev_samp(*) not available

drop table stressme;
