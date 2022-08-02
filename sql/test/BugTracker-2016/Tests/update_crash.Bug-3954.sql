create table temp (t timestamp, sensor integer, val decimal(8,2)) ;

insert into temp values(timestamp '2016-03-13 08:58:14', 1, 23.4);

select * from temp;

create table temp_aggregate(temp_total decimal(8,2), temp_count decimal(8,2));
insert into temp_aggregate values(0.0,0.0);

create procedure collect()
begin
	    update temp_aggregate
	        set temp_total = temp_total + (select sum(val) from temp),
		            temp_count = temp_count + (select count(*) from temp);
end;

drop  procedure collect;
drop table temp;
drop table temp_aggregate;
