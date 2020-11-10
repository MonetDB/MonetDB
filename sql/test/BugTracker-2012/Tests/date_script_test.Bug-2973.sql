-- We need a proof that these coercions indeed work on all platforms

select str_to_date('12-01-01','%y-%m-%d');
select str_to_date('2012-01-01','%Y-%m-%d');

select date_to_str('2012-02-11','%y/%m/%d');
select date_to_str('2012-02-11','%Y/%m/%d');
-- and a few more
