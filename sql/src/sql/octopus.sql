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
-- Copyright August 2008-2009 MonetDB B.V.
-- All Rights Reserved.

-- The Octopus code base aims at Cloud based distribution of work
-- A SQL catalog table contains names and access attributes of workers
-- that can be used toe execute queries
-- The default 'merovingian' allows the optimizer to use merovigian
-- to discover and use every site in view.

create table aquarium(
	connection string,
	host string,
	prt int,
	usr string,
	pwd string
);
-- insert into octopusWorkers values('merovingian','localhost',50000,'monetdb','monetdb');

-- inter the details of a new octopus acquarium
-- each call will update the list of platforms for execution.
create procedure newAquarium(nme string, 
	host string, 
	prt int, 
	usr string, 
	pw string) external name sql.newAquarium;
create procedure dropAquarium(nme string) external name sql.dropAquarium;
