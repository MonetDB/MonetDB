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
-- Routines to assess the opportunities for compression columns

-- inspect the layout of a compressioned column

create schema mosaic;

create function mosaic.layout(sch string, tbl string, col string)
returns table (
	"technique" string, 	-- any of the built-in compressors + header
	"count" bigint, 	-- entries covered 
	inputsize bigint,	-- original storage footprint
	outputsize bigint,	-- after compression
	properties string	-- additional information
	) external name sql."mosaicLayout";

-- provide an analysis of the possible mosaic storage layouts 
create function mosaic.analysis(sch string, tbl string, col string)
returns table(
	technique string, 	-- compression techniques being used
	outputsize bigint,	-- after compression
	factor double		-- compression factor
) external name sql."mosaicAnalysis";

create function mosaic.analysis(sch string, tbl string, col string, compressions string)
returns table(
	technique string, 	-- compression techniques being used
	outputsize bigint,	-- after compression
	factor double		-- compression factor
) external name sql."mosaicAnalysis";
