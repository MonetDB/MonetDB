-- query 6

-- added * 

select (
	(select count(*) from X00003) +
	(select count(*) from X00788)
	);
