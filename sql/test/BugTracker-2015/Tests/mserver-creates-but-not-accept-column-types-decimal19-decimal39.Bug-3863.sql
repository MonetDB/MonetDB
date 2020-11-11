CREATE TABLE bar (name CHAR(8), ct INTEGER, ct2 INTEGER);
INSERT INTO bar VALUES ('A', 123, 456);
CREATE TABLE foo AS SELECT cast(SUM(ct + ct2) as bigint) / 100.0 AS eur FROM bar GROUP BY name WITH DATA;

select "name", "query", "type", "remark" from describe_table('sys', 'foo');
select * from describe_columns('sys', 'foo');

DROP TABLE foo;
DROP TABLE bar;

