-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

-- support routines for the compressed store
create schema mosaic;

create function mosaic.layout(sch string, tbl string, col string) 
returns table(technique string, "count" bigint, inputsize bigint, outputsize bigint,properties string)
external name sql.mosaiclayout;

create function mosaic.analysis(sch string, tbl string, col string) 
returns table(technique string, outputsize bigint, factor float, "compress" bigint, "decompress" bigint)
external name sql.mosaicanalysis;

create function mosaic.analysis(sch string, tbl string, col string, compression string) 
returns table(technique string, outputsize bigint, factor float, "compress" bigint, "decompress" bigint)
external name sql.mosaicanalysis;
