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

-- Clustering a relational table should be done with care.
-- For, the oid's are used in join-indices.

-- Clustering of tables may improve IO performance
-- The foreign key constraints should be dropped before
-- and re-established after the cluster operation.

create procedure cluster1(sys string, tab string)
	external name sql.cluster1;

create procedure cluster2(sys string, tab string)
	external name sql.cluster2;

