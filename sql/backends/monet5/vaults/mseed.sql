-- The contents of this file are subject to the MonetDB Public License
-- Version 1.1 (the "License"); you may not use this file except in
-- compliance with the License. You may obtain a copy of the License at
-- http://www.monetdb.org/Legal/MonetDBLicense
--
-- Software distributed under the License is distributed on an "AS IS"
-- basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
-- License for the specific language governing rights and limitations
-- under the License.
--
-- The Original Code is the MonetDB Database System.
--
-- The Initial Developer of the Original Code is CWI.
-- Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
-- Copyright August 2008-2014 MonetDB B.V.
-- All Rights Reserved.

-- this schema is intended to experiment with accessing mseed files
DROP FUNCTION mseedImport();
DROP TABLE mseedCatalog;

-- all records in the mseed files correspond to a row in the catalog
CREATE TABLE mseedCatalog (
mseed			int, 			-- Vault file id
seqno			int,			-- SEED record sequence number, should be between 0 and 999999
		 PRIMARY KEY (mseed,seqno),
dataquality 	char,			-- Data record indicator, should be 'D=data unknown qual', 
								-- 'R=raw no quality', 'Q= quality controlled' or 'M'
network			varchar(11),	-- Network
station			varchar(11),	-- Station
location		varchar(11),	-- Location
channel			varchar(11),	-- Channel
starttime 		timestamp,		-- Record start time, the time of the first sample, as a high precision epoch time 
samplerate		double,			-- Nominal sample rate (Hz) 
sampleindex		int,			-- record offset in the file
samplecnt		int,			-- Number of samples in record 
sampletype		string,			-- storage type in mseed record
minval			float,			-- statistics for search later
maxval			float
); 

-- this function inserts the mseed record information into the catalog
-- errors are returned for off-line analysis.

CREATE FUNCTION mseedImport(vid int, entry string)
RETURNS int
EXTERNAL NAME mseed.import;

CREATE FUNCTION mseedLoad(entry string)
RETURNS TABLE (a int)
EXTERNAL NAME mseed.load;
