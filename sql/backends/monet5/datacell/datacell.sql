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

-- Datacell wrappers
create procedure receptor (url string, act string)
    external name datacell.receptor_action;

-- MonetDB tuple formatted message field extractors
create function receptor_int(col int) 
returns int
external name datacell.receptor_int;

create function receptor_str(col int) 
returns str
external name datacell.receptor_str;
