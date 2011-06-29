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
create procedure datacell.basket(tbl string)
   external name datacell.basket;

create function datacell.inventory()
returns table (kind string, nme string)
   external name datacell.inventory;

create procedure datacell.receptor(tbl string, host string, portid integer)
    external name datacell.receptor;

create procedure datacell.emitter(tbl string, host string, portid integer)
    external name datacell.emitter;

create procedure datacell.mode(tbl string, mode string)
	external name datacell.mode;

create procedure datacell.protocol(tbl string, protocol string)
	external name datacell.protocol;

create procedure datacell.pause (tbl string)
    external name datacell.pause;

create procedure datacell.resume (tbl string)
    external name datacell.resume;

create procedure datacell.query(proc string, def string)
	external name datacell.query;

create procedure datacell.remove (obj string)
    external name datacell.remove;

-- scheduler activation
create procedure datacell.prelude()
	external name datacell.prelude;

create procedure datacell.postlude()
	external name datacell.postlude;

create procedure datacell.pause()
	external name datacell.pause;

create procedure datacell.resume()
	external name datacell.resume;

create procedure datacell.dump()
	external name datacell.dump;


-- Continueous query predicates.
create function datacell.threshold(bskt string, mi integer)
returns boolean
	external name datacell.threshold;

create function datacell.window(bskt string, size integer, slide integer)
returns boolean
	external name datacell.window;

create function datacell.beat(bskt string, t integer)
returns boolean
	external name datacell.beat;
