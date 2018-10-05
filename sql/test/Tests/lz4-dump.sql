CREATE TABLE outputlz4 (a bigint, b real, c clob);
CREATE TABLE readlz4 (a bigint, b real, c clob);

COPY 4 RECORDS INTO outputlz4 (a, b, c) FROM STDIN USING DELIMITERS ',','\n','"' NULL AS '';
1,2.0,"another"
2,2.1,"test"
3,2.2,"to perform"
,1.0,
SELECT a, b, c FROM outputlz4;
COPY (SELECT a, b, c FROM outputlz4) INTO '/tmp/testing-dump.lz4' USING DELIMITERS ',','\n','"' NULL AS '';

COPY 4 RECORDS INTO readlz4 (a, b, c) FROM '/tmp/testing-dump.lz4' USING DELIMITERS ',','\n','"' NULL AS '';
SELECT a, b, c FROM readlz4;

DROP TABLE outputlz4;
DROP TABLE readlz4;
