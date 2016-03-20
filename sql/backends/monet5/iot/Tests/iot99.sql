-- initialize the stream testing environment
set schema iot;
set optimizer='iot_pipe';

call iot.receptor();
drop procedure clk1;
drop procedure clk3;
drop procedure collector;
drop table stream_tmp;
