CREATE TABLE data_1mto10m (
id      INTEGER,
col1   INTEGER,
col2   VARCHAR(5)
) ;
PREPARE select id from data_1mto10m where id > CAST(? AS INTEGER) limit 10;
exec **('100');
