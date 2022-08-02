-- test support for MySQL/MariaDB specific data type: MEDIUMINT
-- Note: in MySQL/MariaDB a MEDIUMINT is stored in 3 bytes and has a valid range of: -8388608 .. 8388607

-- in MonetDB mediumint is accepted, but mapped to an int
create table meditbl(medi MEDIUMINT);

\d meditbl
-- note: that the data type is now changed into: INTEGER
select name, type, type_digits, type_scale, number from sys.columns where name = 'medi' and table_id in (select id from sys.tables where name = 'meditbl');

-- it accepts all 32-bit signed integer values which are also possible in an int data type
INsert into meditbl values (0), (1), (-1), (32767), (-32767), (8388607), (-8388607), (2147483647), (-2147483647), (NULL);

-- check for overflows (same as on an int)
INsert into meditbl values (2147483648);	-- Error: overflow in conversion of 2147483648 to int.
INsert into meditbl values (-2147483648);	-- Error: overflow in conversion of -2147483648 to int.

select * from meditbl order by 1;

drop table meditbl;

