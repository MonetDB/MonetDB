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
-- Copyright August 2008-2016 MonetDB B.V.
-- All Rights Reserved.

create schema iot;

create procedure iot.query(qry string)
	external name iot.query;

create procedure iot.query("schema" string, name string)
	external name iot.query;

create procedure iot.pause ()
    external name iot.pause;

create procedure iot.pause ("schema" string, name string)
    external name iot.pause;

create procedure iot.resume ()
    external name iot.resume;

create procedure iot.resume ("schema" string, name string)
    external name iot.resume;

create procedure iot.stop ()
    external name iot.stop;

create procedure iot."drop" ()
    external name iot."drop";


create procedure iot.stop ("schema" string, name string)
    external name iot.stop;

create procedure iot.dump()
	external name iot.dump;

create procedure iot.baskets()
	external name iot.baskets;

create procedure iot.petrinet()
	external name iot.petrinet;

create procedure iot.receptors()
	external name iot.receptors;


-- Inspection tables

--create function iot.baskets()
--returns table( "schema" string,  "table" string, threshold int, winsize int, winstride int,  timeslice int, timestride int, beat int, seen timestamp, events int)
--external name baskets.table;

create function iot.queries()
returns table( "schema" string,  "function" string, status string, lastrun timestamp, cycles int, events int, time bigint, error string)
external name petrinet.queries;

--create function iot.receptors()
--returns table( "primary" string,  "secondary" string, status string)
--external name receptor."table";

-- create function iot.errors()
-- returns table( "schema" string,  "table" string, error string)
-- external name iot.errors;
