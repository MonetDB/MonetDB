create table xy ( time timestamp, x decimal(6,4), y decimal(6,4));
prepare insert into xy values (?,?,?);
exec ** (timestamp '2007-03-07 15:28:16.577', 0.6841, 0.684);
exec ** (timestamp '2007-03-07 15:28:16.577', -0.6841, -0.684);
select * from xy;
DROP TABLE xy;
