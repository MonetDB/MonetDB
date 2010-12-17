-- this schema is intended to experiment with accessing mseed files
drop PROCEDURE mseedImport();
drop table mseedCatalog;
drop table mseedRepository;

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
samplecnt		int,			-- Number of samples in record 
sampletype		string,			-- storage type in mseed record
minval			float,			-- statistics for search later
maxval			float
); 

-- this function inserts the mseed record information into the catalog
-- errors are returned for off-line analysis.
CREATE PROCEDURE mseedimport(vid int, source string, target string) 
EXTERNAL NAME mseed.import;

-- The records are collected in SQL tables of the following structure
-- The are ordered on timestamp
--CREATE TABLE chunk<mseed> (
--time	timestamp,
--data	int (or float,double,varchar(20),	dependent on type
--); 
