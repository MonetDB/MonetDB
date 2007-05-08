-- [ 1711242 ] M5/SQL: wrong result for range predicate

create table test (pre integer not null primary key, size integer not
null, level smallint not null, kind smallint not null, prop char (32));
insert into test values(0, 2, 0, 6, 'auctionG.xml');
insert into test values(1, 1, 1, 1, 'foo');
insert into test values(2, 0, 2, 1, 'bar');

select * from test;

SELECT c0002.* FROM test AS c0002, test AS c0001 WHERE c0001.pre +
c0001.size >= c0002.pre AND c0002.pre > c0001.pre AND c0001.prop ='auctionG.xml';
-- This statement should return the foo and bar tuples -- but it doesn't.
-- Throwing away the first (non-effective) condition produces the expected
-- result:

SELECT c0002.* FROM test AS c0002, test AS c0001 WHERE c0002.pre >
c0001.pre AND c0001.prop = 'auctionG.xml';

drop table test;
