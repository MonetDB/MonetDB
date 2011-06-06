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
-- Copyright August 2008-2011 MonetDB B.V.
-- All Rights Reserved.

-- Datacell basket  wrappers

-- Datacell receptor wrappers

create schema receptor;
create procedure receptor.start (sch string, tbl string, host string, port int, protocol string)
    external name receptor.start;

create procedure receptor.pause (sch string, tbl string)
    external name receptor.pause;

create procedure receptor.resume (sch string, tbl string)
    external name receptor.resume;

create procedure receptor.drop (sch string, tbl string)
    external name receptor.drop;

-- Datacell emitter wrappers

create schema emitter;
create procedure emitter.start (sch string, tbl string, host string, port int, protocol string)
    external name emitter.start;

create procedure emitter.pause (sch string, tbl string)
    external name emitter.pause;

create procedure emitter.resume (sch string, tbl string)
    external name emitter.resume;

create procedure emitter.drop (sch string, tbl string)
    external name emitter.drop;

