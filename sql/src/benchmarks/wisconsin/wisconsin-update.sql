START TRANSACTION;
insert into tenk1 (unique1, unique2, two, four, ten, twenty, hundred, thousand, twothousand, fivethous, tenthous, odd, even,stringu1,stringu2, string4) values (10001, 74001, 0, 2, 0, 10, 50, 688, 1950, 4950, 9950, 1, 100, 'ron may choi','jae kwang choi', 'u. c. berkeley');
insert into tenk1 (unique1, unique2, two, four, ten, twenty, hundred, thousand, twothousand, fivethous, tenthous, odd, even,stringu1,stringu2, string4) values (19991, 60001, 0, 2, 0, 10, 50, 688, 1950, 4950, 9950, 1, 100, 'ron may choi','jae kwang choi', 'u. c. berkeley');
delete from tenk1 where tenk1.unique2 = 877;
delete from tenk1 where tenk1.unique2 = 876;
update tenk1 set unique2 = 10001 where tenk1.unique2 =1491;
update tenk1 set unique2 = 10023 where tenk1.unique2 =1480;
insert into tenk1 (unique1, unique2, two, four, ten, twenty, hundred, thousand, twothousand, fivethous, tenthous, odd, even, stringu1, stringu2, string4) values (20002, 70002, 0, 2, 0, 10, 50, 688, 1950, 4950, 9950, 1, 100, 'ron may choi', 'jae kwang choi', 'u. c. berkeley');
insert into tenk1 (unique1, unique2, two, four, ten, twenty, hundred, thousand, twothousand, fivethous, tenthous, odd, even, stringu1, stringu2, string4) values (50002, 40002, 0, 2, 0, 10, 50, 688, 1950, 4950, 9950, 1, 100, 'ron may choi', 'jae kwang choi', 'u. c. berkeley');
delete from tenk1 where tenk1.unique2 = 10001;
delete from tenk1 where tenk1.unique2 = 900;
update tenk1 set unique2 = 10088 where tenk1.unique2 =187;
update tenk1 set unique2 = 10003 where tenk1.unique2 =2000;
update tenk1 set unique2 = 10020 where tenk1.unique2 =1974;
update tenk1 set unique2 = 16001 where tenk1.unique2 =1140;

commit;
