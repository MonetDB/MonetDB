SELECT '#' || trim('zzzytrimxxxx', 'zyx') || '#';
SELECT '#' || trim('zzëzytrimxxëxx', 'zëyx') || '#';
SELECT '#' || trim('zzë颖zytrimxx颖ëxx', 'zëy颖x') || '#';

SELECT '#' || ltrim('zzzytrim', 'zyx') || '#';
SELECT '#' || ltrim('zzëzytrim', 'zëyx') || '#';
SELECT '#' || ltrim('zzë颖zytrim', 'zëy颖x') || '#';

SELECT '#' || rtrim('trimxxxx', 'zyx') || '#';
SELECT '#' || rtrim('trimxxëxx', 'zëyx') || '#';
SELECT '#' || rtrim('trimxx颖ëxx', 'zëy颖x') || '#';

CREATE TABLE t (s VARCHAR(20), s2 VARCHAR(10));
INSERT INTO t VALUES ('zzzytrimxxxx', 'zyx'), ('zzëzytrimxxëxx', 'zëyx'), ('zzë颖zytrimxx颖ëxx', 'zëy颖x');
SELECT trim(s, 'zëy颖x') FROM t;
SELECT trim(s, s2) FROM t;

CREATE TABLE lt (s VARCHAR(20), s2 VARCHAR(10));
INSERT INTO lt VALUES ('zzzytrim', 'zyx'), ('zzëzytrim', 'zëyx'), ('zzë颖zytrim', 'zëy颖x');
SELECT ltrim(s, 'zëy颖x') FROM lt;
SELECT ltrim(s, s2) FROM lt;

CREATE TABLE rt (s VARCHAR(20), s2 VARCHAR(10));
INSERT INTO rt VALUES ('trimxxxx', 'zyx'), ('trimxxëxx', 'zëyx'), ('trimxx颖ëxx', 'zëy颖x');
SELECT rtrim(s, 'zëy颖x') FROM rt;
SELECT rtrim(s, s2) FROM rt;

DROP TABLE t;
DROP TABLE lt;
DROP TABLE rt;

