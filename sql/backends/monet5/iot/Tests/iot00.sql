-- Clear the stream testing environment
set schema iot;
set optimizer='iot_pipe';

create stream table stream_tmp (t timestamp, sensor integer, val decimal(8,2)) ;

insert into stream_tmp values(timestamp '2016-03-13 08:58:14', 1, 23.4);

select * from stream_tmp;
