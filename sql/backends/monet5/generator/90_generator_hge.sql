-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- For copyright information, see the file debian/copyright.

-- (c) Author M.Kersten

create function sys.generate_series(first hugeint, "limit" hugeint)
returns table (value hugeint)
external name generator.series;
grant execute on function sys.generate_series(hugeint, hugeint) to public;

create function sys.generate_series(first hugeint, "limit" hugeint, stepsize hugeint)
returns table (value hugeint)
external name generator.series;
grant execute on function sys.generate_series(hugeint, hugeint, hugeint) to public;
