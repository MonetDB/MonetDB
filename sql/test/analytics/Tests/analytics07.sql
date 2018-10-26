create table testintervals (aa date, bb int);
insert into testintervals values (date '2018-01-02', -1), (date '2018-02-04', 3), (date '2018-04-19', 2),
    (date '2018-05-03', 10), (date '2018-06-06', -12), (date '2018-07-12', 1), (date '2018-08-29', 1131);

create table testintervals2 (aa timestamp, bb int);
insert into testintervals2 values (timestamp '2018-01-02 08:00:10', -1), (timestamp '2018-02-04 19:02:01', 3),
    (timestamp '2018-04-19 15:49:45', 2), (timestamp '2018-05-03 05:12:04', 10), (timestamp '2018-06-06 02:45:03', -12),
    (timestamp '2018-07-12 18:26:01', 1), (timestamp '2018-08-29 14:56:33', 1131);

start transaction;

select count(*) over (order by aa range between interval '0' month preceding and interval '0' month following),
       count(*) over (order by aa range between interval '1' month preceding and interval '1' month following),
       count(*) over (order by aa range between interval '2' month preceding and interval '1' month following),
       count(*) over (order by aa range between interval '0' second preceding and interval '0' second following),
       count(*) over (order by aa range between interval '2629800' second preceding and interval '2629800' second following) from testintervals;

select count(*) over (order by aa desc range between interval '0' month preceding and interval '0' month following),
       count(*) over (order by aa desc range between interval '1' month preceding and interval '1' month following),
       count(*) over (order by aa desc range between interval '2' month preceding and interval '1' month following),
       count(*) over (order by aa desc range between interval '0' second preceding and interval '0' second following),
       count(*) over (order by aa desc range between interval '2629800' second preceding and interval '2629800' second following) from testintervals;

select count(*) over (order by aa range between interval '0' month preceding and interval '0' month following),
       count(*) over (order by aa range between interval '1' month preceding and interval '1' month following),
       count(*) over (order by aa range between interval '2' month preceding and interval '1' month following),
       count(*) over (order by aa range between interval '0' second preceding and interval '0' second following),
       count(*) over (order by aa range between interval '2629800' second preceding and interval '2629800' second following) from testintervals2;

select count(*) over (order by aa desc range between interval '0' month preceding and interval '0' month following),
       count(*) over (order by aa desc range between interval '1' month preceding and interval '1' month following),
       count(*) over (order by aa desc range between interval '2' month preceding and interval '1' month following),
       count(*) over (order by aa desc range between interval '0' second preceding and interval '0' second following),
       count(*) over (order by aa desc range between interval '2629800' second preceding and interval '2629800' second following) from testintervals2;

rollback;

select count(*) over (order by aa range between interval '-1' month preceding and interval '1' month following) from testintervals; --error, negative intervals not allowed

drop table testintervals;
drop table testintervals2;
