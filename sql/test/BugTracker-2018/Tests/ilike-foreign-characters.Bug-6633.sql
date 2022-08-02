START TRANSACTION;

CREATE TABLE debugme (acol CLOB);

INSERT INTO debugme VALUES ('aa'), ('Aä'), ('AÄ'), ('aä'), ('aÄ'), ('oo'), ('öO'), ('ÖO'), ('öo'), ('Öo');

SELECT acol FROM debugme WHERE acol ILIKE 'aä';
SELECT acol FROM debugme WHERE acol ILIKE '%ä';
SELECT acol FROM debugme WHERE acol ILIKE '%ä%';
SELECT acol FROM debugme WHERE acol ILIKE 'ö%';
SELECT acol FROM debugme WHERE acol ILIKE '%ö%';
SELECT acol FROM debugme WHERE acol ILIKE 'ö_';
SELECT acol FROM debugme WHERE acol ILIKE 'öo';

ROLLBACK;
