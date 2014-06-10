-- some errors
select * from generate_series(0,10,-2);
select * from generate_series(10,2,2);

select * from generate_series(0,10,0) ;

select * from generate_series(0,10,null) ;

select * from generate_series(0,10,-2) as v
where value <7 and value >3;
