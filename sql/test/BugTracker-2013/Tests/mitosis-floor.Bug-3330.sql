create table error (x int, y int, intensity int);
copy 2 records into error from stdin delimiters ',','\n';
1,2,3
4,5,6

select
	tilex,
	tiley,
	intensity,
	count(*) as count
from (
	select
		floor(x/16) as tilex,
		floor(y/16) as tiley,
		intensity
	from
		error
) as image
group by
	tilex,
	tiley,
	intensity
;


drop table error;
