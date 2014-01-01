# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
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
# Copyright August 2008-2014 MonetDB B.V.
# All Rights Reserved.

-- Datacell basket  wrappers
create schema datacell;
create procedure datacell.basket(tbl string)
   external name datacell.basket;

create procedure datacell.receptor(tbl string, host string, portid integer)
    external name datacell.receptor;

create procedure datacell.receptor(tbl string, host string, portid integer, proto string, mode string)
    external name datacell.receptor;

create procedure datacell.emitter(tbl string, host string, portid integer)
    external name datacell.emitter;

create procedure datacell.emitter(tbl string, host string, portid integer, proto string, mode string)
    external name datacell.emitter;

create procedure datacell.pause (tbl string)
    external name datacell.pause;

create procedure datacell.resume (tbl string)
    external name datacell.resume;

create procedure datacell.stop (tbl string)
    external name datacell.stop;

create procedure datacell.query(proc string, def string)
	external name datacell.query;

create procedure datacell.query(proc string)
	external name datacell.query;

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

create function datacell.window(bskt string, size integer, stride integer)
returns boolean
	external name datacell.window;

create function datacell.window(bskt string, size interval second, stride interval second)
returns boolean
	external name datacell.timewindow;

create function datacell.beat(bskt string, t integer)
returns boolean
	external name datacell.beat;

-- Inspection tables

create function datacell.baskets()
returns table( nme string, threshold int, winsize int, winstride int,  timeslice int, timestride int, beat int,
	seen timestamp, events int)
external name datacell.baskets;

create function datacell.receptors()
returns table( nme string, host string, port int, protocol string, mode string, status string, 
	lastseen timestamp, cycles int, received int, pending int)
external name datacell.receptors;

create function datacell.emitters()
returns table( nme string, host string, port int, protocol string, mode string, status string, 
	lastsent timestamp, cycles int, sent int, pending int)
external name datacell.emitters;

create function datacell.queries()
returns table( nme string, status string, lastrun timestamp, cycles int, events int, time bigint, error string, def string)
external name datacell.queries;

create function datacell.errors()
returns table( nme string, error string)
external name datacell.errors;
