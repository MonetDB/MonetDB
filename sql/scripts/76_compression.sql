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
-- Make the compression techniques applied for each column visible
-- Each column consists of multiple blocks compressed differently
-- The total compression factor achieved is shown always

create function sys."compression"()
returns table (
	"schema" string, 
	"table" string, 
	"column" string, 
	"type" string, 
	"count" bigint, -- total column size
	technique string, -- any of the built-in compressors
	blocks bigint, -- number of compressed blocks
	cover bigint, -- number of elements compressed this way
	factor  double	-- compression factor achieved
	)
external name sql."compression";

create view sys."compression" as select * from sys."compression"();

