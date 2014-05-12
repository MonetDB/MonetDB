select cast( '2008-03-01 00:00' as timestamp);
select cast( '10' as interval second);

select * from generate_series(
	cast( '2008-03-01 00:00' as timestamp),
	cast( '2008-03-04 12:00' as timestamp), 
	cast( '10' as interval hour));

select * from generate_series(
	cast( '2008-03-01 00:00' as timestamp),
	cast( '2008-03-04 12:00' as timestamp), 
	cast( '1' as interval day));
