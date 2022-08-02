-- 'decimal' and 'numeric' data types should be equivalent.
-- 'dec' is a synonym for 'decimal'
--
-- SEMANTICS: 
--
-- dec(precision, scale) data type accepts decimal numbers with the following format:
--  up to <precision> digits in total (sign and '.' are -not- counted)
--  up to (<precision> - <scale>) digits before the '.'  (integer part) 
--  up to <scale> digits after the '.'  (decimal part)
--
-- If the integer part exceeds the allowed representation, an error should be reported
-- If the decimal part exceeds the allowed representation, it should be rounded (not truncated)
-- If only the integer part is entered, a '.0' decimal part (<scale> digits long) should be assumed
--
--
-- Example: 
--  The allowed range of decimal numbers for dec(5,2) is (-999.99 , 999.99).
--  1000 is not allowed, as the integer part is 4 digits long, which exceeds (5 - 2)=3 
--  999.6666666 is allowed and it should be rounded to 999.67
--
--
-- Results from MySQL are completely wrong with respect to this semantics.
-- Results from PostgreSQL are correct

start transaction;

create table decimaltest ( t1 dec(5,2) );

-- these should succeed
insert into decimaltest values (1.1);
insert into decimaltest values (-1.1);
insert into decimaltest values (12.12);
insert into decimaltest values (-12.12);
insert into decimaltest values (123.12);
insert into decimaltest values (-123.12);
      -- next value should be rounded to 123.12
insert into decimaltest values (123.123);
      -- next value should be rounded to -123.12
insert into decimaltest values (-123.123);
      -- next value should be rounded to 123.13
insert into decimaltest values (123.128);
     -- next value should be rounded to -123.13
insert into decimaltest values (-123.128);
commit;

-- these should fail
insert into decimaltest values (1234);
      -- a '.00' decimal part is assumed for the next value 
insert into decimaltest values (-1234);
      -- a '.00' decimal part is assumed for the next value 
insert into decimaltest values (1234.1);
insert into decimaltest values (-1234.1);


select * from decimaltest;

drop table decimaltest;

