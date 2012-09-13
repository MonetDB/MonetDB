-- Optimized schema for mSEED (meta-)data.
CREATE SCHEMA mseed;

CREATE TABLE "mseed"."files" (
--	"file_id"  INTEGER,
	"file_location" STRING,
-- 	"file_last_modified"	TIMESTAMP,
	"dataquality"	CHAR(1),
	"network"	VARCHAR(10),
	"station"	VARCHAR(10),
	"location"	VARCHAR(10),
	"channel"	VARCHAR(10),
	"encoding"	TINYINT,
	"byte_order"	BOOLEAN,
--	CONSTRAINT "files_pkey_file_id" PRIMARY KEY (file_id)
	CONSTRAINT "files_pkey_file_loc" PRIMARY KEY (file_location)
);

--INSERT INTO mseed.files VALUES ( 1, 'location1', '2012-02-03 09:54:49.504', 'D', 'YF', 'AMS', 'NL', 'BHE', 20.0, 3, 0);

--INSERT INTO mseed.files VALUES ('location1', 'D', 'YF', 'AMS', 'NL', 'BHE', 20.0, 3, 0);

CREATE TABLE "mseed"."catalog" (
--	"file_id" INTEGER,	
	"file_location" STRING,
	"seq_no"	INTEGER,
	"record_length"	INTEGER,
	"start_time"	TIMESTAMP,
	"frequency"	DOUBLE,
	"sample_count"   BIGINT,
	"sample_type"   CHAR(1),
--	CONSTRAINT "catalog_file_id_seq_no_pkey" PRIMARY KEY (file_id, seq_no),
--	CONSTRAINT "cat_fkey_files_file_id" FOREIGN KEY (file_id) REFERENCES mseed.files(file_id)
	CONSTRAINT "catalog_file_loc_seq_no_pkey" PRIMARY KEY (file_location, seq_no),
	CONSTRAINT "cat_fkey_files_file_loc" FOREIGN KEY (file_location) REFERENCES mseed.files(file_location)
);

--INSERT INTO mseed.catalog VALUES (1, 20, 8192, '2001-05-04 19:50:34.250', 3001, 'i');
--INSERT INTO mseed.catalog VALUES (1, 21, 16384, '2001-05-04 19:52:39.247', 2999, 'i');
--INSERT INTO mseed.catalog VALUES (1, 22, 4096, '2001-05-04 19:54:21.980', 3000, 'i');

--INSERT INTO mseed.catalog VALUES ('location1', 20, 8192, '2001-05-04 19:50:34.250', 3001, 'i');
--INSERT INTO mseed.catalog VALUES ('location1', 21, 16384, '2001-05-04 19:52:39.247', 2999, 'i');
--INSERT INTO mseed.catalog VALUES ('location1', 22, 4096, '2001-05-04 19:54:21.980', 3000, 'i');

CREATE TABLE "mseed"."data" (
--	"file_id"      INTEGER,
	"file_location" STRING,
	"seq_no"       INTEGER,
	"sample_time"  TIMESTAMP,
	"sample_value" INTEGER
--	CONSTRAINT "data_file_id_seq_no_sample_time_pkey" PRIMARY KEY (file_id, seq_no, sample_time),
--	CONSTRAINT "data_fkey_cat_file_id_seq_no" FOREIGN KEY (file_id, seq_no) REFERENCES mseed.catalog(file_id, seq_no)
-- 	CONSTRAINT "data_file_loc_seq_no_sample_time_pkey" PRIMARY KEY (file_location, seq_no, sample_time),
-- 	CONSTRAINT "data_fkey_cat_file_loc_seq_no" FOREIGN KEY (file_location, seq_no) REFERENCES mseed.catalog(file_location, seq_no)
);

-- INSERT INTO mseed.data VALUES (1, 20, '2001-05-04 19:50:34.250', 60);
-- INSERT INTO mseed.data VALUES (1, 20, '2001-05-04 19:50:34.300', 62);
-- INSERT INTO mseed.data VALUES (1, 20, '2001-05-04 19:50:34.350', 59);
-- INSERT INTO mseed.data VALUES (1, 21, '2001-05-04 19:52:39.247', 57);
-- INSERT INTO mseed.data VALUES (1, 21, '2001-05-04 19:52:39.297', 56);

-- INSERT INTO mseed.data VALUES ('location1', 20, '2001-05-04 19:50:34.250', 60);
-- INSERT INTO mseed.data VALUES ('location1', 20, '2001-05-04 19:50:34.300', 62);
-- INSERT INTO mseed.data VALUES ('location1', 20, '2001-05-04 19:50:34.350', 59);
-- INSERT INTO mseed.data VALUES ('location1', 21, '2001-05-04 19:52:39.247', 57);
-- INSERT INTO mseed.data VALUES ('location1', 21, '2001-05-04 19:52:39.297', 56);

CREATE VIEW mseed.dataview AS
SELECT f.file_location, dataquality, network, station, location, channel, encoding, byte_order, c.seq_no, record_length, start_time, frequency, sample_count, sample_type, sample_time, sample_value
FROM mseed.files AS f 
	JOIN mseed.catalog AS c
		ON f.file_location = c.file_location 
	JOIN mseed.data AS d 
		ON c.file_location = d.file_location AND c.seq_no = d.seq_no;

-- CREATE VIEW mseed.dataview AS
-- SELECT d.file_id, dataquality, network, station, location, channel, frequency, encoding, byte_order, d.seq_no, record_length, start_time, sample_count, sample_type, sample_time, sample_value
-- FROM mseed.files AS f 
-- 	JOIN mseed.catalog AS c
-- 		ON f.file_id = c.file_id 
-- 	JOIN mseed.data AS d 
-- 		ON c.file_id = d.file_id AND c.seq_no = d.seq_no;
-- 


-- CREATE TABLE "mseed"."fake_data" (
-- -- 	"file_id"      	INTEGER,
-- 	"file_location"	STRING,
-- 	"seq_no"	INTEGER,
-- 	"sample_time"	TIMESTAMP,
-- 	"sample_value"	INTEGER
-- );

-- INSERT INTO mseed.fake_data SELECT * FROM miniseed.mount("/net/singha/export/data1/knmi/ORFEUS/2010/005/FR_ARBF_00_BHN.2010.005.12.15.36.mseed");

-- 
-- INSERT INTO mseed.fake_data VALUES (1, 20, '2001-05-04 19:50:34.250', 60);
-- INSERT INTO mseed.fake_data VALUES (1, 20, '2001-05-04 19:50:34.300', 62);
-- INSERT INTO mseed.fake_data VALUES (1, 20, '2001-05-04 19:50:34.350', 59);
-- INSERT INTO mseed.fake_data VALUES (1, 21, '2001-05-04 19:52:39.247', 57);
-- INSERT INTO mseed.fake_data VALUES (1, 21, '2001-05-04 19:52:39.297', 56);

-- INSERT INTO mseed.fake_data VALUES ('location1', 20, '2001-05-04 19:50:34.250', 60);
-- INSERT INTO mseed.fake_data VALUES ('location1', 20, '2001-05-04 19:50:34.300', 62);
-- INSERT INTO mseed.fake_data VALUES ('location1', 20, '2001-05-04 19:50:34.350', 59);
-- INSERT INTO mseed.fake_data VALUES ('location1', 21, '2001-05-04 19:52:39.247', 57);
-- INSERT INTO mseed.fake_data VALUES ('location1', 21, '2001-05-04 19:52:39.297', 56);
-- 
-- 
-- COPY INTO mseed.data FROM '/export/scratch2/kargin/csv/mseed_example_data.csv';

