start transaction;
create table analytics (aa int, bb int);
insert into analytics values (15, 3), (3, 1), (2, 1), (5, 3), (NULL, 2), (3, 2), (4, 1), (6, 3), (8, 2), (NULL, 4);

select group_concat(aa, aa) over (partition by bb),
       group_concat(aa, aa) over (partition by bb order by bb asc),
       group_concat(aa, aa) over (partition by bb order by bb desc),
       group_concat(aa, aa) over (order by bb desc) from analytics;

select group_concat(bb, bb) over (partition by bb),
       group_concat(bb, bb) over (partition by bb order by bb asc),
       group_concat(bb, bb) over (partition by bb order by bb desc),
       group_concat(bb, bb) over (order by bb desc) from analytics;

select group_concat(aa, bb) over (partition by bb),
       group_concat(aa, bb) over (partition by bb order by bb asc),
       group_concat(aa, bb) over (partition by bb order by bb desc),
       group_concat(aa, bb) over (order by bb desc) from analytics;

select group_concat(bb, aa) over (partition by bb),
       group_concat(bb, aa) over (partition by bb order by bb asc),
       group_concat(bb, aa) over (partition by bb order by bb desc),
       group_concat(bb, aa) over (order by bb desc) from analytics;

select group_concat(aa, 1) over (partition by bb),
       group_concat(aa, 1) over (partition by bb order by bb asc),
       group_concat(aa, 1) over (partition by bb order by bb desc),
       group_concat(aa, 1) over (order by bb desc) from analytics;

select group_concat(bb, -100) over (partition by bb),
       group_concat(bb, -100) over (partition by bb order by bb asc),
       group_concat(bb, -100) over (partition by bb order by bb desc),
       group_concat(bb, -100) over (order by bb desc) from analytics;

select group_concat(aa, aa) over (),
       group_concat(bb, bb) over (),
       group_concat(aa, bb) over (),
       group_concat(bb, aa) over (),
       group_concat(aa, 1) over (),
       group_concat(aa, 1) over () from analytics;

select group_concat(NULL, 2) over (),
       group_concat(2, NULL) over (),
       group_concat(aa, NULL) over (),
       group_concat(NULL, aa) over (),
       group_concat(NULL, NULL) over () from analytics;


create table testmore (a int, b clob);
insert into testmore values (1, 'another'), (1, 'testing'), (1, 'todo'), (2, 'lets'), (3, 'get'), (2, 'harder'), (3, 'even'), (2, 'more'), (1, ''), (3, 'even'), (2, NULL), (1, '');

select a, listagg(a) over (),
       listagg(a) over (order by a),
       listagg(a, min(b)) over (), 
       listagg(a, min(b)) over (partition by max(b)) from testmore group by a;

rollback;
