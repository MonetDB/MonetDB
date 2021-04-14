set time zone interval '+01:00' hour to minute;

select cast(123 as varchar(10));
select convert(123, decimal(10,3));

select cast(true as smallint);
select cast(42 as int);
select cast(123.45 as real);
select cast('123.45' as double precision);
select cast(23.45 as decimal(5,2));    -- precision of 5 digits of which 2 decimal digits

select cast('2020-07-29' as date);
select cast('17:44:59' as time);
select cast('17:44:59.123456' as time);
select cast('2020-07-29 17:44:59' as timestamp);
select cast('2020-07-29T17:44:59' as timestamp);
select cast('2020-07-29 17:44:59.123456' as timestamp);
select cast('17:44:59.321+01:30' as timetz);
select cast('2020-07-29 17:44:59.321+01:30' as timestamptz);
select cast('1234' as interval month);
select cast('86400.123' as interval second);

select cast('abcd' as blob);
select cast('abcde' as clob);
select cast('192.168.1.5/24' as inet);
select cast(r'{"a":[1,2,4]}' as json);
select cast('https://www.monetdb.org/Home' as url);
select cast('e31960fb-dc8b-452d-ab30-b342723e756a' as uuid);

-- or using convert instead of cast:
select convert('a4cd' , blob);
select convert('abcde' , clob);
select convert('192.168.1.5/24' , inet);
select convert(r'{"a":[1,2,4]}' , json);
select convert('https://www.monetdb.org/Home' , url);
select convert('e31960fb-dc8b-452d-ab30-b342723e756a' , uuid);

-- using prefix operators
select x'abcd';
select blob 'abcd';
select clob 'abcde';	-- we dont support this at the moment
select cast('abcde' as clob);
select inet '192.168.1.5/24';
select json '{"a":[1,2,4]}';
select url 'https://www.monetdb.org/Home';
select uuid 'e31960fb-dc8b-452d-ab30-b342723e756a';
select E'EA\fB\tC\n\\Z';
select e'eA\fB\tC\n\\Z';
select R'RA\fB\tC\n\\Z';
select r'rA\fB\tC\n\\Z';

