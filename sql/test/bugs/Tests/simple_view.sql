
create view tv as select 'str' as S, 1 as I;

select tt.name, tv.* from tables as tt, tv;
