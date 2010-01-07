-- The contents of this file are subject to the MonetDB Public License
-- Version 1.1 (the "License"); you may not use this file except in
-- compliance with the License. You may obtain a copy of the License at
-- http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
--
-- Software distributed under the License is distributed on an "AS IS"
-- basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
-- License for the specific language governing rights and limitations
-- under the License.
--
-- The Original Code is the MonetDB Database System.
--
-- The Initial Developer of the Original Code is CWI.
-- Copyright August 2008-2010 MonetDB B.V.
-- All Rights Reserved.


-- The master contains a builtin table whose content is updated each
-- time a slave initiates a session, starts processing a logfile
-- and leaves the scene.

CREATE FUNCTION sys.slaves()
RETURNS TABLE (
	uri              varchar(100),
	last_connect     timestamp,	
	last_disconnect  timestamp,	-- null when connected
	last_tag		 bigint,	-- tag associated with log file finished
	tag_delay        bigint,	-- replicationTag - logfile tag in processs
	time_delay       timestamp 
) EXTERNAL NAME master."slaves";

-- Each slave contains a table of replication requests successfully executed.
-- It can serve multiple masters and provides a persistent
-- store for the master name. A comment can be left behind for a posteriori
-- error analysis.
CREATE TABLE sys.replicas (
	uri varchar(100) NOT NULL, 
	tag int, 
	stamp timestamp, 
	remark string
);

-- If your are the master return its uri. Otherwise locate the first master
-- value in the replicas table.
CREATE FUNCTION master() RETURNS string EXTERNAL NAME master."getName";

-- Initialize this table with the location of the current system
INSERT INTO sys.replicas VALUES(master(), 0, now(), 'master created');

-- Controling the synchronisation by the slave
CREATE PROCEDURE synchronizeWithMaster(uri string) EXTERNAL NAME master."start";
CREATE PROCEDURE synchronizeWithMaster(uri string, tag bigint) EXTERNAL NAME master."start";
CREATE PROCEDURE stopSynchronization(uri string) EXTERNAL NAME master."stop";
