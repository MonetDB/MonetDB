-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at https://mozilla.org/MPL/2.0/.
--
-- For copyright information, see the file debian/copyright.

-- Author M.Kersten
-- This script gives the database administrator insight in the actual
-- value distribution over all tables in the database.

create aggregate uniques_guess(c hugeint)
returns bigint external name aggr.uniques_guess;
grant execute on aggregate uniques_guess(hugeint) to public;
