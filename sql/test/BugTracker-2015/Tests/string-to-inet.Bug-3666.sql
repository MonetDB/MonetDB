create table iptable (textip varchar(20));
insert into iptable values ('192.168.0.1');
insert into iptable values ('192.168.0.1');
insert into iptable values ('192.168.0.1');
insert into iptable values ('192.168.10.2');
select cast(textip as inet) from iptable;
drop table iptable;
