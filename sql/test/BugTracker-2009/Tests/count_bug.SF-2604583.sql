create table countt1( a int , b int ) ;
insert into countt1 values ( 3, 1 ) , ( 4, 2 ) , ( 5, 3 ) , ( 6, 4 ) , ( 7, 5 ) ;

select * from countt1;
select cast( ( select count( * )+1 from countt1 as tt3 where tt3.b < tt2.b ) as bigint) from countt1 as tt2 order by 1;
--the result : the last SQL should returns 1,2,3,4,5 , not null,2,3,4,5

drop table countt1;
