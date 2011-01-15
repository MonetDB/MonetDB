CREATE SEQUENCE "test_seq" as bigint;
select next value for test_seq;
alter sequence test_seq restart with 3000000000;
select next value for test_seq;
drop SEQUENCE test_seq;
