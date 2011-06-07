# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2011 MonetDB B.V.
# All Rights Reserved.

-- Datacell basket  wrappers
create schema datacell;
create procedure datacell.basket(sch string, tbl string)
   external name datacell.basket;

create function datacell.inventory()
returns table (kind string, nme string)
   external name datacell.inventory;

create procedure datacell.receptor(sch string, tbl string, host string, port int, protocol string)
    external name receptor.start;

create procedure datacell.emitter(sch string, tbl string, host string, port int, protocol string)
    external name emitter.start;

create procedure datacell.pause (sch string, tbl string)
    external name datacell.pause;

create procedure datacell.resume (sch string, tbl string)
    external name datacell.resume;

create procedure datacell.drop (sch string, tbl string)
    external name datacell.drop;

create procedure datacell.query(sch string, proc string)
	extern name datacell.query;

create procedure datacell.run()
	extern name datacell.run;

create procedure datacell.stop()
	extern name datacell.stop;
