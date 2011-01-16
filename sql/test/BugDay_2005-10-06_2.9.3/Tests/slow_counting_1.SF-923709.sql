
create table slow_count ( i int);
insert into slow_count values(1);

create function gen_insert(i int) returns int
begin
	while i > 0 do 
		set i = i - 1;
		insert into slow_count select * from slow_count;
	end while;
	return 0;
end;
select gen_insert(17);	-- previous call was 25

select count(*) from slow_count;
