-- introduce a heartbeat query
set schema iot;
set optimizer='iot_pipe';

create table tmp_aggregate(tmp_total decimal(8,2), tmp_count decimal(8,2));
insert into tmp_aggregate values(0.0,0.0);


create procedure collector()
begin
	update tmp_aggregate
		set tmp_total = tmp_total + (select sum(val) from iot.stream_tmp),
			tmp_count = tmp_total + (select count(*) from iot.stream_tmp);
end;

-- alternative is a simple query
iot.pause();
iot.query('iot','collector');
iot.dump();
iot.resume();
iot.stop();
