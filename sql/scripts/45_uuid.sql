-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

-- (co) Martin Kersten
-- The UUID type comes with a few operators.

create type uuid external name uuid;

-- generate a new uuid
create function sys.uuid()
returns uuid external name uuid."new";

create function sys.isaUUID(s string)
returns boolean external name uuid."isaUUID";
