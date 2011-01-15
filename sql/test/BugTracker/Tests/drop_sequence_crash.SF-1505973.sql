
CREATE TABLE userss (
	id integer PRIMARY KEY AUTO_INCREMENT,
	name varchar(20) UNIQUE,
	passwd varchar(8), 
	email varchar(30),
	affiliation varchar(20)
);

CREATE TABLE system (
	id integer PRIMARY KEY AUTO_INCREMENT,
	name varchar(20) UNIQUE,
	hardware_platform varchar(30),
	RAM varchar(10),
	disk_type varchar(10),
	disk_size varchar(10)
);

CREATE TABLE target (
	id integer PRIMARY KEY AUTO_INCREMENT,
	name varchar(20) UNIQUE,
	measure1 integer,
	measure2 integer,
	measure3 integer
);

CREATE SEQUENCE conf_id_seq AS int
	START WITH 1
	INCREMENT BY 1
	NO CYCLE;

CREATE TABLE configuration (
	id integer PRIMARY KEY DEFAULT NEXT VALUE FOR conf_id_seq,
	name varchar(20),
	system_id integer references system(id),
	target_id integer references target(id)	
);

CREATE TABLE tapestry (
	id integer PRIMARY KEY AUTO_INCREMENT,
	name varchar(20) UNIQUE,
	rows_ integer,
	columns_ integer,
	seed integer,
	comments varchar(30)
);

CREATE TABLE "partition" (
	id integer PRIMARY KEY AUTO_INCREMENT,
	name varchar(20) UNIQUE,
	corr integer,
	X integer,
	W integer,
	L integer,
	H integer,
	root_x integer,
	root_y integer ,
	depth integer
);

CREATE TABLE query_walk (
	id integer PRIMARY KEY AUTO_INCREMENT,
	name varchar(30) UNIQUE,
	partition_id integer references "partition"(id),
	start_ varchar(10),
	X integer,
	width integer,
	low integer,
	high integer,
	rows_ integer,
	columns_ integer,
	sigma decimal,
	morph varchar(10),
	shift integer,
	step_x integer,
	step_y integer,
	query_select varchar(40),
	query_where varchar(40),
	query_orderby varchar(20),
	length integer	
);

CREATE SEQUENCE scenario_id_seq AS int
	START WITH 1
	INCREMENT BY 1
	NO CYCLE;

CREATE TABLE scenario (
	id integer PRIMARY KEY DEFAULT NEXT VALUE FOR scenario_id_seq,
	name varchar(30),
	tapestry_id integer references tapestry(id),
	configuration_id integer references configuration(id),
	query_walk_id integer references query_walk(id),
	time_estimated time
);

CREATE SEQUENCE results_id_seq AS int
	START WITH 1
	INCREMENT BY 1
	NO CYCLE;

CREATE TABLE results (
	id integer PRIMARY KEY DEFAULT NEXT VALUE FOR results_id_seq,
	m0 float,
	m1 float,
	m2 float,
	m3 float,
	m4 float,
	m5 float,
	m6 float,
	m7 float,
	m8 float,
	m9 float
);


CREATE TABLE experiment (
	id integer PRIMARY KEY AUTO_INCREMENT,
	name varchar(20),
	scenario_id integer references scenario(id),
	user_id integer references userss(id),
	results_id integer references results(id),
	privacy varchar(10),
	notes varchar(100)
);


INSERT INTO system VALUES (1,'P2','Pentium II 450MHz', '128MB', 'SCSI',
'20GB');
INSERT INTO system VALUES (2,'P3','PentiumIII 800 MHz', '256MB',
'SCSI','20GB');
INSERT INTO system VALUES (3,'P4','Pentium IV 2.4GHz', '512MB',
'SCSI','20GB');
										
INSERT INTO target VALUES (1,'MonetDB4',1, 1, 1);
INSERT INTO target VALUES (2,'MonetDB5',1, 1, 1);
INSERT INTO target VALUES (3,'MySQL',1, 1, 1);
INSERT INTO target VALUES (4,'PostgreSQL',1, 1, 1);


INSERT INTO configuration (name,system_id, target_id) 
		VALUES ('c1',1,1);
INSERT INTO tapestry VALUES (1,'tap1',10240,2,1024,'tapestry comments');
INSERT INTO "partition" VALUES (1,'p1',0,0,2,0,2,null,null,null);
INSERT INTO query_walk VALUES
(1,'q1',1,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,2);
INSERT INTO scenario (name, tapestry_id,	configuration_id,
query_walk_id, time_estimated) 
	VALUES ('non sense', 1,1,1,'01:20:21');

INSERT INTO userss (name,passwd,email,affiliation)
	VALUES ('anonymous',null, null, null);
INSERT INTO userss(name,passwd,email,affiliation) 
	VALUES ('manzano','manzano','manzano@cwi.nl','cwi');
INSERT INTO userss(name,passwd,email,affiliation) 
	VALUES ('manzano3','manzano','manzano@cwi.nl','cwi');
INSERT INTO userss(name,passwd,email,affiliation) 
	VALUES ('manzano4','manzano','manzano@cwi.nl','cwi');
INSERT INTO userss(name,passwd,email,affiliation) 
	VALUES ('manzano5','manzano','manzano@cwi.nl','cwi');
INSERT INTO userss(name,passwd,email,affiliation) 
	VALUES ('manzano6','manzano','manzano@cwi.nl','cwi');
INSERT INTO userss(name,passwd,email,affiliation) 
	VALUES ('manzano7','manzano','manzano@cwi.nl','cwi');
INSERT INTO userss(name,passwd,email,affiliation) 
	VALUES ('manzano8','manzano','manzano@cwi.nl','cwi');
INSERT INTO userss(name,passwd,email,affiliation) 
	VALUES ('manzano9','manzano','manzano@cwi.nl','cwi');
INSERT INTO userss(name,passwd,email,affiliation) 
	VALUES ('manzano10','manzano','manzano@cwi.nl','cwi');
INSERT INTO userss(name,passwd,email,affiliation) 
	VALUES ('manzano11','manzano','manzano@cwi.nl','cwi');
INSERT INTO userss(name,passwd,email,affiliation) 
	VALUES ('manzano12','manzano','manzano@cwi.nl','cwi');
INSERT INTO userss(name,passwd,email,affiliation) 
	VALUES ('manzano13','manzano','manzano@cwi.nl','cwi');
INSERT INTO userss(name,passwd,email,affiliation) 
	VALUES ('manzano14','manzano','manzano@cwi.nl','cwi');
INSERT INTO userss(name,passwd,email,affiliation) 
	VALUES ('manzano15','manzano','manzano@cwi.nl','cwi');

