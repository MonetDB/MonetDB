-- some error unit  tests
select * from generate_series('a','a','c');

select * from generate_series(false,true,false) ;

-- casting errors
select * from generate_series(null,10) ;
select * from generate_series(10,null) ;
select * from generate_series(null,-1) ;
select * from generate_series(-1,null) ;

select * from generate_series(null,null,1);
select * from generate_series(10,null,1);
select * from generate_series(null,10,1);


-- negative step size in positive range
select * from generate_series(0,10,-2) as v
where value <7 and value >3;

-- you need the step size
select * from generate_series(
	timestamp '2008-03-01 00:00',
	timestamp '2008-03-04 12:00');
