-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 2024, 2025 MonetDB Foundation;
-- Copyright August 2008 - 2023 MonetDB B.V.;
-- Copyright 1997 - July 2008 CWI.

-- (c) Author M.Kersten

create function sys.generate_series(first tinyint, "limit" tinyint)
returns table (value tinyint)
external name generator.series;
grant execute on function sys.generate_series(tinyint, tinyint) to public;

create function sys.generate_series(first tinyint, "limit" tinyint, stepsize tinyint)
returns table (value tinyint)
external name generator.series;
grant execute on function sys.generate_series(tinyint, tinyint, tinyint) to public;

create function sys.generate_series(first smallint, "limit" smallint)
returns table (value smallint)
external name generator.series;
grant execute on function sys.generate_series(smallint, smallint) to public;

create function sys.generate_series(first smallint, "limit" smallint, stepsize smallint)
returns table (value smallint)
external name generator.series;
grant execute on function sys.generate_series(smallint, smallint, smallint) to public;

create function sys.generate_series(first int, "limit" int)
returns table (value int)
external name generator.series;
grant execute on function sys.generate_series(int, int) to public;

create function sys.generate_series(first int, "limit" int, stepsize int)
returns table (value int)
external name generator.series;
grant execute on function sys.generate_series(int, int, int) to public;

create function sys.generate_series(first bigint, "limit" bigint)
returns table (value bigint)
external name generator.series;
grant execute on function sys.generate_series(bigint, bigint) to public;

create function sys.generate_series(first bigint, "limit" bigint, stepsize bigint)
returns table (value bigint)
external name generator.series;
grant execute on function sys.generate_series(bigint, bigint, bigint) to public;

create function sys.generate_series(first real, "limit" real, stepsize real)
returns table (value real)
external name generator.series;
grant execute on function sys.generate_series(real, real, real) to public;

create function sys.generate_series(first double, "limit" double, stepsize double)
returns table (value double)
external name generator.series;
grant execute on function sys.generate_series(double, double, double) to public;

create function sys.generate_series(first decimal(10,2), "limit" decimal(10,2), stepsize decimal(10,2))
returns table (value decimal(10,2))
external name generator.series;
grant execute on function sys.generate_series(decimal(10,2), decimal(10,2), decimal(10,2)) to public;

create function sys.generate_series(first date, "limit" date, stepsize interval month)
returns table (value date)
external name generator.series;
grant execute on function sys.generate_series(date, date, interval month) to public;

create function sys.generate_series(first date, "limit" date, stepsize interval day)
returns table (value date)
external name generator.series;
grant execute on function sys.generate_series(date, date, interval day) to public;

create function sys.generate_series(first timestamp, "limit" timestamp, stepsize interval second)
returns table (value timestamp)
external name generator.series;
grant execute on function sys.generate_series(timestamp, timestamp, interval second) to public;

create function sys.generate_series(first timestamp, "limit" timestamp, stepsize interval day)
returns table (value timestamp)
external name generator.series;
grant execute on function sys.generate_series(timestamp, timestamp, interval day) to public;
