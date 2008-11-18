create table way_tags (way integer, k string);
SELECT * FROM way_tags WHERE way IN (SELECT way WHERE k = 'bridge');
drop table way_tags;
