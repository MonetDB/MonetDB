create table stmp10 (t timestamp, sensor integer, val decimal(8,2));
create table tmp_aggregate(tmp_total decimal(8,2), tmp_count decimal(8,2));
insert into tmp_aggregate values(0.0,0.0);

create procedure cq_collector()
begin
    update tmp_aggregate
        set tmp_total = tmp_total + (select sum(val) from sys.stmp10),
            tmp_count = tmp_count + (select count(*) from sys.stmp10);
end;

call cq_collector();

drop procedure cq_collector;
drop table stmp10;
drop table tmp_aggregate;
