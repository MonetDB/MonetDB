SELECT tbl.action FROM sys.keys tbl;
SELECT tbl.action FROM tmp.keys tbl;

SELECT tbl.default FROM sys._columns tbl;
SELECT tbl.default FROM sys.columns tbl;
SELECT tbl.default FROM tmp._columns tbl;

SELECT tbl.schema FROM sys.statistics tbl;
SELECT tbl.schema FROM sys.storage tbl;
SELECT tbl.schema FROM sys.storagemodel tbl;
SELECT tbl.schema FROM sys.storagemodelinput tbl;
SELECT tbl.schema FROM sys.tablestoragemodel tbl;

SELECT tbl.start FROM sys.querylog_calls tbl;
SELECT tbl.start FROM sys.querylog_history tbl;
SELECT tbl.start FROM sys.sequences tbl;

SELECT tbl.statement FROM sys.triggers tbl;
SELECT tbl.statement FROM tmp.triggers tbl;

SELECT tbl.user FROM sys.connections tbl;
SELECT tbl.user FROM sys.queue tbl;
SELECT tbl.user FROM sys.sessions tbl;
SELECT tbl.user FROM sys.tracelog tbl;

SELECT action FROM sys.keys;
SELECT action FROM tmp.keys;

SELECT default FROM sys._columns;
SELECT default FROM sys.columns;
SELECT default FROM tmp._columns;

SELECT schema FROM sys.statistics;
SELECT schema FROM sys.storage;
SELECT schema FROM sys.storagemodel;
SELECT schema FROM sys.storagemodelinput;
SELECT schema FROM sys.tablestoragemodel;

SELECT start FROM sys.querylog_calls;
SELECT start FROM sys.querylog_history;
SELECT start FROM sys.sequences;

SELECT statement FROM sys.triggers;
SELECT statement FROM tmp.triggers;
