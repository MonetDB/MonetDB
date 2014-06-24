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
create function json.filter(js json, pathexpr string)
returns json external name json.filter;

create function json.filter(js json, name tinyint)
returns json external name json.filter;

create function json.filter(js json, name integer)
returns json external name json.filter;

create function json.filter(js json, name bigint)
returns json external name json.filter;

create function json.text(js json, e string)
returns string external name json.text;

create function json.number(js json)
returns float external name json.number;

create function json."integer"(js json)
returns bigint external name json."integer";

-- test string for JSON compliancy
create function json.isvalid(js string)
returns bool external name json.isvalid;

create function json.isobject(js string)
returns bool external name json.isobject;

create function json.isarray(js string)
returns bool external name json.isarray;

create function json.isvalid(js json)
returns bool external name json.isvalid;

create function json.isobject(js json)
returns bool external name json.isobject;

create function json.isarray(js json)
returns bool external name json.isarray;

-- return the number of primary components
create function json.length(js json)
returns integer external name json.length;

create function json.keyarray(js json)
returns json external name json.keyarray;

create function json.valuearray(js json)
returns  json external name json.valuearray;

create function json.text(js json)
returns string external name json.text;
create function json.text(js string)
returns string external name json.text;
create function json.text(js int)
returns string external name json.text;

-- The remainder awaits the implementation 

create aggregate json.output(js json)
returns string external name json.output;

-- create function json.object(*) returns json external name json.objectrender;

-- create function json.array(*) returns json external name json.arrayrender;

-- unnesting the JSON structure

-- create function json.unnest(js json)
-- returns table( id integer, k string, v string) external name json.unnest;

-- create function json.unnest(js json)
-- returns table( k string, v string) external name json.unnest;

-- create function json.unnest(js json)
-- returns table( v string) external name json.unnest;

-- create function json.nest table( id integer, k string, v string)
-- returns json external name json.nest;

create aggregate json.tojsonarray( x string ) returns string external name aggr.jsonaggr;
create aggregate json.tojsonarray( x double ) returns string external name aggr.jsonaggr;
