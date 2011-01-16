CREATE TABLE DBObjects(
   name varchar(128) NOT NULL,
   type varchar(64) NOT NULL,
   access varchar(8) NOT NULL,
   description varchar(256) NOT NULL,
   "text" varchar(7200) NOT NULL,
   rank int DEFAULT (0), --NOT NULL
CONSTRAINT pk_DBObjects_name PRIMARY KEY
(
   name
));

COPY 9 RECORDS INTO DBObjects FROM stdin USING DELIMITERS '\t', '\t\n', '"';
"Algorithm"	"U"	"U"	" Contains a paragraph of text for each algorithm "	" The Glossary table contains cross-references to entries in the Algorithm table. "	"0"	
"BestTarget2Sector"	"U"	"U"	" Map PhotoObj which are potential targets to sectors  "	" PhotoObj should only appear once in this list because any ra,dec  should belong to a unique sector "	"0"	
"Chunk"	"U"	"U"	" Contains basic data for a Chunk
 "	" A Chunk is a unit for SDSS data export. 
  It is a part of an SDSS stripe, a 2.5 degree wide cylindrical segment 
  aligned at a great circle between the survey poles. 
  A Chunk has had both strips completely observed. Since 
  the SDSS camera has gaps between its 6 columns of CCDs, each stripe has 
  to be scanned twice (these are the strips) resulting in 12 slightly 
  overlapping narrow observation segments. <P>
  Only those parts of a stripe are ready for export where the observation 
  is complete, hence the declaration of a chunk, usually consisting of 2 runs. 
 "	"0"	
"Columns"	"V"	"U"	" Aliias the DBColumns table also as Columns, for legacy SkyQuery "	"none"	"0"	
"CoordType"	"V"	"U"	" Contains the CoordType enumerated values from DataConstants as int "	"none"	"0"	
"DataConstants"	"U"	"U"	" The table is storing various enumerated constants for flags, etc "	"none"	"0"	
"DBColumns"	"U"	"U"	" Every column of every table has a description in this table "	"none"	"0"	
"DBObjects"	"U"	"U"	" Every SkyServer database object has a one line description in this table "	"none"	"0"	
"DBViewCols"	"U"	"U"	" The columns of each view are stored for the auto-documentation "	" * means that every column from the  parent is propagated. "	"0"	

select * from DBObjects;
select count(*) from DBObjects;

drop table DBObjects;
