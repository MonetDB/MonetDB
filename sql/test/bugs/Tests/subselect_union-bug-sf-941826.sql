select col1 from 
(
	select 'a' as col1
	union
	select 'b' as col1
) as a;

select "col1" from 
(
	select 'a' as "col1"
	union
	select 'b' as "col1"
) as a;

select col1 from 
(
	select 'a' as col1
	union
	select 'b' as col1
) as a where col1 like 'a';
