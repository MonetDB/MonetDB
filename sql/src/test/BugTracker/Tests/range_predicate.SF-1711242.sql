SELECT c0002.* FROM test1711251 AS c0002, test1711251 AS c0001 WHERE c0001.pre + c0001.size >= c0002.pre AND c0002.pre > c0001.pre AND c0001.prop = 'auctionG.xml';

SELECT c0002.* FROM test1711251 AS c0002, test1711251 AS c0001 WHERE c0002.pre > c0001.pre AND c0001.prop = 'auctionG.xml';

select distinct c2.pre from test1711251 as c2, test1711251 as c1 where c1.kind = 6 and c1.prop = 'auctionG.xml' and c2.kind = 1 and c2.pre > c1.pre and c1.pre + c1.size >= c2.pre;
