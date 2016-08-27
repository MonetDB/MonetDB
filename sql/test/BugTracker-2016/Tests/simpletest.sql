create table simple1(i integer);

insert into simple values(1);

create procedure correct()
begin
	declare b boolean;
	declare i integer;
	set i = (select count(*));
	set b= (i>0);
	if (b)
	then
		insert into simple1 values(2);
	end if;
end;

create procedure correct1()
begin
	declare b boolean;
	set b= (select count(*)>0);
	if (b)
	then
		insert into simple1 values(2);
	end if;
end;

-- questionable wrong SQL expression
create procedure wrong2()
begin
	if (select count(*)>0)
	then
		insert into simple1 values(3);
	end if;
end;
