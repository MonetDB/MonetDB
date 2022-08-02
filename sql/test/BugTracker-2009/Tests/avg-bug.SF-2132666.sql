CREATE TABLE car (
	age int,
	gender char(1),
	price bigint,
	category varchar(20),
	class varchar(20),
	codville char(3),
	sage smallint,
	iprice int
);

insert into car values 
	(2, 'm', 123, 'a', 'b', 'c', 1, 1),
	(3, 'm', 123, 'a', 'b', 'c', 2, 1),
	(4, 'm', 123, 'a', 'b', 'c', 3, 9),
	(6, 'm', 123, 'a', 'b', 'c', 8, 1),
	(8, 'm', 123, 'a', 'b', 'c', 9, 1);
	
select count(*), gender , cast( sum (sage) as int), avg(sage) from car group by gender;
select count(*), gender , cast( sum (age) as bigint), avg(age) from car group by gender;

select gender, count(*),cast( sum(iprice) as bigint), avg(price), avg(iprice) from car group by gender;

drop table car;
