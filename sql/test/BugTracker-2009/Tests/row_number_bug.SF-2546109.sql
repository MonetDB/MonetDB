CREATE TABLE a(n varchar(255));
INSERT INTO a VALUES('ONE');
INSERT INTO a VALUES('TWO');
INSERT INTO a VALUES('THREE');

CREATE VIEW b AS
SELECT row_number() over () AS id, n
FROM   a;

select * from b;

SELECT * FROM b WHERE  n = 'TWO';

drop view b;
drop table a;
