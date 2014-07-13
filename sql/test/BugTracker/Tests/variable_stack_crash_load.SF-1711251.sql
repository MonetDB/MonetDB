create table test1711251 (pre integer not null primary key, size integer not null,
level smallint not null, kind smallint not null, prop char (32));

copy 3 records into test1711251 from stdin DELIMITERS ',', '\n', '"';
0, 2, 0, 6,"auctionG.xml"
1, 1, 1, 1,"foo"
2, 0, 2, 1,"bar"

select * from test1711251;
