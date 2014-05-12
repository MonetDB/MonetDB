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
-- Copyright August 2008-2014 MonetDB B.V.
-- All Rights Reserved.

-- Author M.Kersten
-- The vault is the container for all foreign file support functionalities

-- example of a (void) foreign file interface

create function sys.generate_series(first tinyint, last tinyint)
returns table (value tinyint)
external name vault.generate_series;

create function sys.generate_series(first tinyint, last tinyint, stepsize tinyint)
returns table (value tinyint)
external name vault.generate_series;

create function sys.generate_series(first int, last int)
returns table (value int)
external name vault.generate_series;

create function sys.generate_series(first int, last int, stepsize int)
returns table (value int)
external name vault.generate_series;

create function sys.generate_series(first bigint, last bigint)
returns table (value bigint)
external name vault.generate_series;

create function sys.generate_series(first bigint, last bigint, stepsize bigint)
returns table (value bigint)
external name vault.generate_series;

create function sys.generate_series(first real, last real, stepsize real)
returns table (value real)
external name vault.generate_series;

create function sys.generate_series(first double, last double, stepsize double)
returns table (value double)
external name vault.generate_series;

create function sys.generate_series(first decimal(10,2), last decimal(10,2), stepsize decimal(10,2))
returns table (value decimal(10,2))
external name vault.generate_series;

create function sys.generate_series(first timestamp, last timestamp, stepsize interval second)
returns table (value timestamp)
external name vault.generate_series;

-- create function sys.generate_series(first timestamp, last timestamp, stepsize interval minute)
-- returns table (value timestamp)
-- external name vault.generate_series;
-- 
-- create function sys.generate_series(first timestamp, last timestamp, stepsize interval hour)
-- returns table (value timestamp)
-- external name vault.generate_series;
-- 
-- create function sys.generate_series(first timestamp, last timestamp, stepsize interval day)
-- returns table (value timestamp)
-- external name vault.generate_series;
-- 
-- create function sys.generate_series(first timestamp, last timestamp, stepsize interval month)
-- returns table (value timestamp)
-- external name vault.generate_series;
