-- query 7

select ((select count(*) from X00017) +
        (select count(*) from X00174) +
	(select count(*) from X00331) +
	(select count(*) from X00488) +
	(select count(*) from X00645) +
	(select count(*) from X00802) +
	(select count(*) from X00950) +
	(select count(*) from X01133) +
	(select count(*) from X01270) +
	(select count(*) from X00032) +
	(select count(*) from X00193) +
	(select count(*) from X00358) +
	(select count(*) from X00504) +
	(select count(*) from X00671) +
	(select count(*) from X00818));

