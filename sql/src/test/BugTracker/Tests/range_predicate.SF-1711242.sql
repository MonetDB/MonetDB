SELECT c0002.* FROM test1711251 AS c0002, test1711251 AS c0001 WHERE c0001.pre + c0001.size >= c0002.pre AND c0002.pre > c0001.pre AND c0001.prop = 'auctionG.xml';

SELECT c0002.* FROM test1711251 AS c0002, test1711251 AS c0001 WHERE c0002.pre > c0001.pre AND c0001.prop = 'auctionG.xml';
