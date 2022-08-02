-- some errors
select * from generate_series(0,10,-2);
select * from generate_series(10,2,2);

select * from generate_series(0,10,0) ;

select * from generate_series(0,10,null) ;
select * from generate_series(null,10,1) ;
select * from generate_series(cast(null as tinyint),10,1) ;
select * from generate_series(null,cast(10 as tinyint),cast(1 as tinyint)) ;
select * from generate_series(cast(null as tinyint),cast(10 as tinyint),cast(1 as tinyint)) ;
select * from generate_series(cast(null as smallint),10,1) ;
select * from generate_series(null,cast(10 as smallint),cast(1 as smallint)) ;
select * from generate_series(cast(null as smallint),cast(10 as smallint),cast(1 as smallint)) ;
select * from generate_series(cast(null as integer),10,1) ;
select * from generate_series(null,cast(10 as integer),cast(1 as integer)) ;
select * from generate_series(cast(null as integer),cast(10 as integer),cast(1 as integer)) ;
select * from generate_series(cast(null as bigint),10,1) ;
select * from generate_series(null,cast(10 as bigint),cast(1 as bigint)) ;
select * from generate_series(cast(null as bigint),cast(10 as bigint),cast(1 as bigint)) ;
select * from generate_series(cast(null as tinyint),cast(10 as smallint),cast(1 as bigint)) ;
select * from generate_series(cast(null as timestamp),10,1) ;
select * from generate_series(null,cast(10 as timestamp),cast(1 as interval second)) ;
select * from generate_series(cast(null as timestamp),cast(10 as timestamp),cast(1 as interval second)) ;
select * from generate_series(10,null,1) ;

select * from generate_series(null,10) ;
select * from generate_series(10,null) ;

select * from generate_series(null,-1) ;
select * from generate_series(-1,null) ;

select * from generate_series(0,10,-2) as v
where value <7 and value >3;
