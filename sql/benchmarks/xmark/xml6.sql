-- query 6

select ((select count(*) from X00003) +
        (select count(*) from X00160) +
	(select count(*) from X00317) +
	(select count(*) from X00474) +
	(select count(*) from X00631) +
	(select count(*) from X00788));

