CREATE TABLE bar (name CHAR(8) PRIMARY KEY);
CREATE TABLE foo (id INTEGER, barname CHAR(8));
ALTER TABLE foo ADD CONSTRAINT "fname" FOREIGN KEY ("barname")
REFERENCES bar ("name") ON DELETE CASCADE;

SELECT ps.name, pkt.name, pkkc.name, fkkc.name, fkkc.nr, fkk.name,
fkk."action", fs.name, fkt.name FROM sys._tables fkt, sys.objects fkkc,
sys.keys fkk, sys._tables pkt, sys.objects pkkc, sys.keys pkk,
sys.schemas ps, sys.schemas fs WHERE fkt.id = fkk.table_id AND pkt.id =
pkk.table_id AND fkk.id = fkkc.id AND pkk.id = pkkc.id AND fkk.rkey =
pkk.id AND fkkc.nr = pkkc.nr AND pkt.schema_id = ps.id AND fkt.schema_id
= fs.id AND fkt.system = FALSE ORDER BY fs.name,fkt.name, fkk.name, nr;

DROP TABLE foo;
DROP TABLE bar;
