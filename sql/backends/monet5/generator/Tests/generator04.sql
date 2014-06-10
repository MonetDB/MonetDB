-- some unit  tests
select * from generate_series('a','b','c');

select * from generate_series(false,true,false) ;

select * from generate_series(null,null,1);
select * from generate_series(10,null,1);
select * from generate_series(null,10,1);
