-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 2008-2015 MonetDB B.V.

-- (co) Martin Kersten
-- The UUID type comes with a few operators.

create type uuid external name uuid;

-- generate a new uuid
create function sys.uuid()
returns uuid external name uuid."new";

create function sys.isaUUID(u uuid)
returns uuid external name uuid."isaUUID";

create function sys.isaUUID(u string)
returns uuid external name uuid."isaUUID";
