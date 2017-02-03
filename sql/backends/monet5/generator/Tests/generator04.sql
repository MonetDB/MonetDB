set optimizer = 'sequential_pipe'; -- to get predictable errors

-- some unit  tests
select * from generate_series('a','a','c');

select * from generate_series(false,true,false) ;

select * from generate_series(null,null,1);
select * from generate_series(10,null,1);
select * from generate_series(null,10,1);

select * from generate_series(
	timestamp '2008-03-01 00:00',
	timestamp '2008-03-04 12:00');
