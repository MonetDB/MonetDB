-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

-- (c) Author M.Kersten

create function sys.generate_series(first tinyint, last tinyint)
returns table (value tinyint)
external name generator.series;

create function sys.generate_series(first tinyint, last tinyint, stepsize tinyint)
returns table (value tinyint)
external name generator.series;

create function sys.generate_series(first smallint, last smallint)
returns table (value smallint)
external name generator.series;

create function sys.generate_series(first smallint, last smallint, stepsize smallint)
returns table (value smallint)
external name generator.series;

create function sys.generate_series(first int, last int)
returns table (value int)
external name generator.series;

create function sys.generate_series(first int, last int, stepsize int)
returns table (value int)
external name generator.series;

create function sys.generate_series(first bigint, last bigint)
returns table (value bigint)
external name generator.series;

create function sys.generate_series(first bigint, last bigint, stepsize bigint)
returns table (value bigint)
external name generator.series;

create function sys.generate_series(first real, last real, stepsize real)
returns table (value real)
external name generator.series;

create function sys.generate_series(first double, last double, stepsize double)
returns table (value double)
external name generator.series;

create function sys.generate_series(first decimal(10,2), last decimal(10,2), stepsize decimal(10,2))
returns table (value decimal(10,2))
external name generator.series;

create function sys.generate_series(first timestamp, last timestamp, stepsize interval second)
returns table (value timestamp)
external name generator.series;
