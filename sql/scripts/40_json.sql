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

-- (co) Martin Kersten
-- The JSON type comes with a few operators.

create schema json;

create type json external name json;

-- access the top level key by name, return its value
create function json.filter(js json, name string)
returns json external name json.filter;

create function json.filter(js json, name integer)
returns json external name json.filter;

create function json.filter_all(js json, name string)
returns json external name json.filterall;

-- a simple path extractor
create function json.path(js json, e string)
returns json external name json.path;

-- a simple path extractor as plain text
create function json.text(js json, e string)
returns string external name json.text;

-- test string for JSON compliancy
create function json.isvalid(js string)
returns bool external name json.isvalid;

create function json.isvalidobject(js string)
returns bool external name json.isvalidobject;

create function json.isvalidarray(js string)
returns bool external name json.isvalidarray;

-- return the number of primary components
create function json.length(js json)
returns integer external name json.length;

-- The remainder awaits the implementation of
-- proper functions with table type arguments.

-- unnesting the JSON structure

-- create function json.unnest(js json)
-- returns table( id integer, k string, v string) external name json.unnest;

-- create function json.unnest(js json)
-- returns table( k string, v string) external name json.unnest;

-- create function json.unnest(js json)
-- returns table( v string) external name json.unnest;

-- create function json.nest table( id integer, k string, v string)
-- returns json external name json.nest;

-- create function json.names(js json)
-- returns table ( nme string) external name json.names;

-- create function json.values(js json)
-- returns table ( val string) external name json."values";

-- rendering functions
-- create function json.object(*)
-- returns json external name json.objectrender;
-- create function json.array(*)
-- returns json external name json.arrayrender;
