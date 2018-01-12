-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

-- (co) Martin Kersten
-- The JSON type comes with a few operators.

create schema json;

create type json external name json;

-- access the top level key by name, return its value
create function json.filter(js json, pathexpr string)
returns json external name json.filter;
GRANT EXECUTE ON FUNCTION json.filter(json, string) TO PUBLIC;

create function json.filter(js json, name tinyint)
returns json external name json.filter;
GRANT EXECUTE ON FUNCTION json.filter(json, tinyint) TO PUBLIC;

create function json.filter(js json, name integer)
returns json external name json.filter;
GRANT EXECUTE ON FUNCTION json.filter(json, integer) TO PUBLIC;

create function json.filter(js json, name bigint)
returns json external name json.filter;
GRANT EXECUTE ON FUNCTION json.filter(json, bigint) TO PUBLIC;

create function json.text(js json, e string)
returns string external name json.text;
GRANT EXECUTE ON FUNCTION json.text(json, string) TO PUBLIC;

create function json.number(js json)
returns float external name json.number;
GRANT EXECUTE ON FUNCTION json.number(json) TO PUBLIC;

create function json."integer"(js json)
returns bigint external name json."integer";
GRANT EXECUTE ON FUNCTION json."integer"(json) TO PUBLIC;

-- test string for JSON compliancy
create function json.isvalid(js string)
returns bool external name json.isvalid;
GRANT EXECUTE ON FUNCTION json.isvalid(string) TO PUBLIC;

create function json.isobject(js string)
returns bool external name json.isobject;
GRANT EXECUTE ON FUNCTION json.isobject(string) TO PUBLIC;

create function json.isarray(js string)
returns bool external name json.isarray;
GRANT EXECUTE ON FUNCTION json.isarray(string) TO PUBLIC;

create function json.isvalid(js json)
returns bool external name json.isvalid;
GRANT EXECUTE ON FUNCTION json.isvalid(json) TO PUBLIC;

create function json.isobject(js json)
returns bool external name json.isobject;
GRANT EXECUTE ON FUNCTION json.isobject(json) TO PUBLIC;

create function json.isarray(js json)
returns bool external name json.isarray;
GRANT EXECUTE ON FUNCTION json.isarray(json) TO PUBLIC;

-- return the number of primary components
create function json.length(js json)
returns integer external name json.length;
GRANT EXECUTE ON FUNCTION json.length(json) TO PUBLIC;

create function json.keyarray(js json)
returns json external name json.keyarray;
GRANT EXECUTE ON FUNCTION json.keyarray(json) TO PUBLIC;

create function json.valuearray(js json)
returns  json external name json.valuearray;
GRANT EXECUTE ON FUNCTION json.valuearray(json) TO PUBLIC;

create function json.text(js json)
returns string external name json.text;
GRANT EXECUTE ON FUNCTION json.text(json) TO PUBLIC;
create function json.text(js string)
returns string external name json.text;
GRANT EXECUTE ON FUNCTION json.text(string) TO PUBLIC;
create function json.text(js int)
returns string external name json.text;
GRANT EXECUTE ON FUNCTION json.text(int) TO PUBLIC;

-- The remainder awaits the implementation

create aggregate json.output(js json)
returns string external name json.output;
GRANT EXECUTE ON AGGREGATE json.output(json) TO PUBLIC;

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
GRANT EXECUTE ON AGGREGATE json.tojsonarray( string ) TO PUBLIC;
create aggregate json.tojsonarray( x double ) returns string external name aggr.jsonaggr;
GRANT EXECUTE ON AGGREGATE json.tojsonarray( double ) TO PUBLIC;
