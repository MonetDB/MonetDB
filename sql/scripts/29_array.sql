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
-- Copyright August 2008-2012 MonetDB B.V.
-- All Rights Reserved.

-- Vacuum a relational table should be done with care.
-- For, the oid's are used in join-indices.

-- Vacuum of tables may improve IO performance and disk footprint.
-- The foreign key constraints should be dropped before
-- and re-established after the cluster operation.

-- TODO: DATE, TIME, TIMESTAMP, CHAR and VARCHAR not supported yet
create function array_series("start" tinyint, step tinyint, stop tinyint, N integer, M integer) returns table (id bigint, dimval tinyint)
	external name "array".series_;
create function array_series("start" smallint, step smallint, stop smallint, N integer, M integer) returns table (id bigint, dimval smallint)
	external name "array".series_;
create function array_series("start" integer, step integer, stop integer, N integer, M integer) returns table (id bigint, dimval integer)
	external name "array".series_;
create function array_series("start" bigint, step bigint, stop bigint, N integer, M integer) returns table (id bigint, dimval bigint)
	external name "array".series_;
create function array_series("start" float, step float, stop float, N integer, M integer) returns table (id bigint, dimval float)
	external name "array".series_;

create function array_series1("start" tinyint, step tinyint, stop tinyint, N integer, M integer) returns table (idimval tinyint)
	external name "array".series;
create function array_series1("start" smallint, step smallint, stop smallint, N integer, M integer) returns table (dimval smallint)
	external name "array".series;
create function array_series1("start" integer, step integer, stop integer, N integer, M integer) returns table (dimval integer)
	external name "array".series;
create function array_series1("start" bigint, step bigint, stop bigint, N integer, M integer) returns table (dimval bigint)
	external name "array".series;
create function array_series1("start" float, step float, stop float, N integer, M integer) returns table (dimval float)
	external name "array".series;

create function array_filler(cnt bigint, val tinyint) returns table (id bigint, cellval tinyint)
	external name "array".filler_;
create function array_filler(cnt bigint, val smallint) returns table (id bigint, cellval smallint)
	external name "array".filler_;
create function array_filler(cnt bigint, val integer) returns table (id bigint, cellval integer)
	external name "array".filler_;
create function array_filler(cnt bigint, val bigint) returns table (id bigint, cellval bigint)
	external name "array".filler_;
create function array_filler(cnt bigint, val real) returns table (id bigint, cellval real)
	external name "array".filler_;
create function array_filler(cnt bigint, val double) returns table (id bigint, cellval double)
	external name "array".filler_;
create function array_filler(cnt bigint, val date) returns table (id bigint, vals date)
	external name "array".filler_;
create function array_filler(cnt bigint, val time) returns table (id bigint, vals time)
	external name "array".filler_;
create function array_filler(cnt bigint, val timestamp) returns table (id bigint, vals timestamp)
	external name "array".filler_;
create function array_filler(cnt bigint, val char(2048)) returns table (id bigint, vals char(2048))
	external name "array".filler_;
create function array_filler(cnt bigint, val varchar(2048)) returns table (id bigint, vals varchar(2048))
	external name "array".filler_;
create function array_filler(cnt bigint, val blob) returns table (id bigint, vals blob)
	external name "array".filler_;
create function array_filler(cnt bigint, val clob) returns table (id bigint, vals clob)
	external name "array".filler_;

