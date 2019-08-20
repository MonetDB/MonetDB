CREATE SEQUENCE seqBool AS boolean;
CREATE SEQUENCE seqChar AS char;
CREATE SEQUENCE seqClob AS CLOB;
CREATE SEQUENCE seqBlob AS BLOB;
CREATE SEQUENCE seqDate AS date;
CREATE SEQUENCE seqTime AS time;
CREATE SEQUENCE seqInet AS inet;
CREATE SEQUENCE seqUuid AS uuid;

start transaction;
CREATE SEQUENCE seqTiny AS tinyint;
CREATE SEQUENCE seqSmall AS smallint;
CREATE SEQUENCE seqInt AS int;
CREATE SEQUENCE seqBint AS bigint;
CREATE SEQUENCE seqInteger AS integer;

select name, start, minvalue, maxvalue, increment, cacheinc, cycle from sequences
where name in ('seqbool', 'seqchar', 'seqclob', 'seqblob', 'seqdate', 'seqtime', 'seqinet', 'sequuid', 'seqtiny',
               'seqsmall', 'seqint', 'seqbint', 'seqinteger');
rollback;
