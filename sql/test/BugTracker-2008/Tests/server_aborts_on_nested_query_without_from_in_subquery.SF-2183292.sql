create table way_tags_2183292 (way integer, k string);
SELECT * FROM way_tags_2183292 WHERE way IN (SELECT way WHERE k = 'bridge');
drop table way_tags_2183292;
