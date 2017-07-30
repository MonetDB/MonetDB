-- use accumulated aggregation
create stream table stmp10 (t timestamp, sensor integer, val decimal(8,2)) ;

create table tmp_aggregate(tmp_total decimal(8,2), tmp_count decimal(8,2));
insert into tmp_aggregate values(0.0,0.0);

-- The default more is to tumble towards the next window
create procedure cq_collector()
begin
    update tmp_aggregate
        set tmp_total = tmp_total + (select sum(val) from sys.stmp10),
            tmp_count = tmp_count + (select count(*) from sys.stmp10);
end;

insert into stmp10 values('2005-09-23 12:34:26.000',1,9.0);
insert into stmp10 values('2005-09-23 12:34:27.000',1,11.0);
insert into stmp10 values('2005-09-23 12:34:28.000',1,13.0);
insert into stmp10 values('2005-09-23 12:34:28.000',1,15.0);

start continuous sys.cq_collector() with cycles 3;

call cquery.wait(4000);

pause continuous sys.cq_collector();
stop continuous sys.cq_collector();

select * from tmp_aggregate;

drop procedure cq_collector;
drop table stmp10;
drop table tmp_aggregate;
