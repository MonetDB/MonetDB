-- this schema is intended to experiment with accessing mseed files
drop FUNCTION mseedImport();
drop table mseedCatalog;
drop table mseedRepository;

-- all records in the mseed files correspond to a row in the catalog
CREATE TABLE mseedCatalog (
mseed			int, 			-- Vault file id
chunk			varchar(255),	-- SQL volumn storage container name
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
CREATE FUNCTION mseedImport(vid int)
	RETURNS string
EXTERNAL NAME mseed.import;

-- mseed data volumns may appear in different formats
-- we try to postpone them, assuming the optimizer can guide JIT.
--CREATE TABLE chunkname (
--time	timestamp,
--mseed	int,
--adata	varchar(20),	dependent on type
--idata	int,
--fdata	float,
--ddata	double
--); 
