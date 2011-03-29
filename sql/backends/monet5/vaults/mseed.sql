-- this schema is intended to experiment with accessing mseed files
DROP FUNCTION mseedImport;
DROP FUNCTION mseedLoad;
DROP TABLE mseed;
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

-- The reference table for querying is simply
CREATE TABLE mseed(
mseed			int, 			-- Vault file id
seqno			int,			-- SEED record sequence number, should be between 0 and 999999
time			timestamp,		-- click
data			int,			-- The actual measurement value.
FOREIGN KEY (mseed,seqno) REFERENCES mseedCatalog(mseed,seqno)
); 

-- this function inserts the mseed record information into the catalog
-- errors are returned for off-line analysis.

CREATE FUNCTION mseedImport(vid int, entry string)
RETURNS int
EXTERNAL NAME mseed.import;

CREATE FUNCTION mseedLoad(entry string)
RETURNS TABLE (a int)
EXTERNAL NAME mseed.load;
