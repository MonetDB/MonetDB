-- disable parallelism (mitosis & dataflow) to avoid ambiguous results 
set optimizer='sequential_pipe';

CREATE TABLE mytypes (id integer, systemname varchar(256));
INSERT INTO mytypes VALUES (0, 'void'),(1,'bat'),(2,'ptr'),(3,'bit'),(4,'str'),(5,'str'),(6,'str'),(7,'oid'),(8,'bte'),(9,'sht'),
(10,'int'),(11,'lng'),(13,'bte'),(14,'sht'),(15,'int'),(16,'lng'),(18,'flt'),(19,'dbl'),
(20,'int'),(21,'lng'),(22,'daytime'),(23,'daytime'),(24,'date'),(25,'timestamp'),(26,'timestamp'),(27,'blob');

select '~BeginVariableOutput~';
TRACE SELECT count(*) FROM mytypes;
select '~EndVariableOutput~';
SELECT COUNT(*) FROM tracelog();

DROP TABLE mytypes;
