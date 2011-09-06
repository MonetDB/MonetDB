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
-- Copyright August 2008-2011 MonetDB B.V.
-- All Rights Reserved.

-- SQL statements to make the RDF Relational Storage Schema query-able from
-- the sql frontend.

-- It only needs to be run once after a fresh installation of MonetDB/SQL.

-- create an RDF schema
create schema rdf;

-- create a graph_name|id table
create table rdf.graph (gname string, gid int);

-- create a procudure to load an RDF document
-- the chema string should be removed in the future and auto-fill it from
-- the backend
create procedure rdf_shred(location string, gname string, sch string)
	external name sql.rdfshred;
