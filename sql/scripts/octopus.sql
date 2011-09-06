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
-- Copyright August 2008-2011 MonetDB B.V.
-- All Rights Reserved.

-- The Octopus code base aims at Cloud based distribution of work.
-- A SQL catalog table contains names and access properties of workers
-- that can be used to execute queries
-- The default 'merovingian' allows the optimizer to use merovigian
-- to discover and use every site in view using local credentials.

create table aquarium(
	connection string not null primary key,
	host string not null,
	prt int not null,
	usr string,
	pwd string
);
-- insert into octopusWorkers values('merovingian','localhost',50000,'monetdb','monetdb');

-- enter the details of a new octopus acquarium
-- each call will update the list of platforms for execution.
create procedure newAquarium(nme string, 
	host string, 
	prt int, 
	usr string, 
	pw string) external name sql.newAquarium;
--create procedure dropAquarium(nme string) external name sql.dropAquarium;

-- updates on these tables are propagated to the octopus scheduler.
create trigger acquariumInsert
	after insert on aquarium referencing new row as r
	for each row call newAquarium(r.connection,r.host,r.prt,r.usr,r.pwd);
create trigger octopusDelete
	after insert on aquarium referencing old row as r
	for each row call dropAquarium(r.connection);
