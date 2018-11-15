start transaction;
create table onet (a text, b text, c text);
insert into onet values ('a', 'b', 'c');
create function get_onet(d text) returns table (aa text, bb text, cc text) return table(select * from onet where a = d);
prepare select * from get_onet(?);
exec **('a');
drop function get_onet;
drop table onet;
rollback;
