create table two (
	id int,
	name string,
	istrue boolean,
	joindate date,
	jointime timestamp,
	count int
);

copy 6 records into two from stdin using delimiters ',','\n';
1,null,false,2017-12-08,2014-06-30 14:05:31.000000,2
2,vijay,false,2017-12-08,2014-06-30 14:05:59.000000,3
3,krish,true,2017-12-08,2014-06-30 14:06:17.000000,5
4,bat,true,null,null,null
5,gotham,false,null,null,null
6,wayne,false,null,null,3


select * from two;

select two.id, two.name, two.count, twoAlias.id as id2, twoAlias.name as name2 from two left join two as twoAlias on two.count=twoAlias.id; 

drop table two;

