-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.

-- (co) Martin Kersten
-- The JSON type comes with a few operators.

create function json.filter(js json, name hugeint)
returns json external name json.filter;
GRANT EXECUTE ON FUNCTION json.filter(json, hugeint) TO PUBLIC;
