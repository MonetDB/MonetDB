-- We need a proof that these coercions indeed work on all platforms

select str_to_date('2012','%y');

select date_to_str(now(),'%y%m');
-- and a few more
