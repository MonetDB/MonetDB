CREATE TABLE Dictionary (ID INTEGER,val VARCHAR(20000),PRIMARY KEY (ID));

CREATE TABLE
bench_booktitle 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));
CREATE TABLE
bench_cdrom 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
swrc_pages 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
foaf_homepage 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
swrc_editor 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
swrc_number 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
dc_creator 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
rdfs_seeAlso 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
dcterms_partOf 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
dcterms_references 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
dcterms_issued 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
swrc_volume 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
dc_publisher 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
swrc_note 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
swrc_chapter 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
swrc_address 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
swrc_series 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
swrc_month 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
foaf_name 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
rdf_type 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
dc_title 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
swrc_journal 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
bench_abstract 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));

CREATE TABLE
swrc_isbn 
	(subject INTEGER,object INTEGER,FOREIGN KEY (subject) REFERENCES Dictionary(ID),FOREIGN KEY (object) REFERENCES Dictionary(ID));


CREATE VIEW Triples AS
( SELECT subject,'bench:booktitle' AS predicate,object FROM bench_booktitle )
UNION
( SELECT subject,'bench:cdrom' AS predicate,object FROM bench_cdrom )
UNION
( SELECT subject,'swrc:pages' AS predicate,object FROM swrc_pages )
UNION
( SELECT subject,'foaf:homepage' AS predicate,object FROM foaf_homepage )
UNION
( SELECT subject,'swrc:editor' AS predicate,object FROM swrc_editor )
UNION
( SELECT subject,'swrc:number' AS predicate,object FROM swrc_number )
UNION
( SELECT subject,'dc:creator' AS predicate,object FROM dc_creator )
UNION
( SELECT subject,'rdfs:seeAlso' AS predicate,object FROM rdfs_seeAlso )
UNION
( SELECT subject,'dcterms:partOf' AS predicate,object FROM dcterms_partOf )
UNION
( SELECT subject,'dcterms:references' AS predicate,object FROM dcterms_references )
UNION
( SELECT subject,'dcterms:issued' AS predicate,object FROM dcterms_issued )
UNION
( SELECT subject,'swrc:volume' AS predicate,object FROM swrc_volume )
UNION
( SELECT subject,'dc:publisher' AS predicate,object FROM dc_publisher )
UNION
( SELECT subject,'swrc:note' AS predicate,object FROM swrc_note )
UNION
( SELECT subject,'swrc:chapter' AS predicate,object FROM swrc_chapter )
UNION
( SELECT subject,'swrc:address' AS predicate,object FROM swrc_address )
UNION
( SELECT subject,'swrc:series' AS predicate,object FROM swrc_series )
UNION
( SELECT subject,'swrc:month' AS predicate,object FROM swrc_month )
UNION
( SELECT subject,'foaf:name' AS predicate,object FROM foaf_name )
UNION
( SELECT subject,'rdf:type' AS predicate,object FROM rdf_type )
UNION
( SELECT subject,'dc:title' AS predicate,object FROM dc_title )
UNION
( SELECT subject,'swrc:journal' AS predicate,object FROM swrc_journal )
UNION
( SELECT subject,'bench:abstract' AS predicate,object FROM bench_abstract )
UNION
( SELECT subject,'swrc:isbn' AS predicate,object FROM swrc_isbn );

select count(*) from dependencies where depend_type = 5;
