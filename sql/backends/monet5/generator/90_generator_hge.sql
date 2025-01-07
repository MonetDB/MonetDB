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

create function sys.generate_series(first hugeint, "limit" hugeint)
returns table (value hugeint)
external name generator.series;
grant execute on function sys.generate_series(hugeint, hugeint) to public;

create function sys.generate_series(first hugeint, "limit" hugeint, stepsize hugeint)
returns table (value hugeint)
external name generator.series;
grant execute on function sys.generate_series(hugeint, hugeint, hugeint) to public;
