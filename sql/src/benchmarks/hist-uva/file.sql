set pagesize 50
set linesize 300

column name format a40

select file#, name, bytes, creation_time, last_time, blocks, block_size
from V$DATAFILE;

select *
from V$FILESTAT;
