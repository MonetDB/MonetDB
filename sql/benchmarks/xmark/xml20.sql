-- query 20

select
(select count(income.tail)
 from X01068 income
 where cast(income.tail as real) >= 100000),
(select count(income.tail)
 from X01068 income
 where cast(income.tail as real) >= 30000
 and cast(income.tail as real) <= 100000),
(select count(income.tail)
 from X01068 income
 where cast(income.tail as real) < 30000),
(select count(profile.tail)
 from X01067 profile
 where not exists (select *
		   from X01068 income
		   where profile.tail = income.head));

