-- use a consolidated view
set schema iot;
set optimizer='iot_pipe';

create view temp_view(temp_total, temp_count) 
as select sum(val), count(*) from stream_tmp;

-- alternative is a simple query
iot.pause();
iot.query('iot','temp_view');
iot.dump();
iot.resume();
iot.stop();

select * from temp_view;
drop view temp_view;
