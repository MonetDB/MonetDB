-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

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
