
select * from 
	( select * from tables union all select * from tables) as a;

select * from 
	( select * from tables union select * from tables) as a;

select * from 
	( select * from tables union distinct select * from tables) as a;
