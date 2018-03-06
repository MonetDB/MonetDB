-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

-- This table used to be in 99_system.sql but we need the systemfunctions table
-- in sys.describe_all_objects and sys.commented_function_signatures defined below.
CREATE TABLE sys.systemfunctions (function_id INTEGER NOT NULL);
GRANT SELECT ON sys.systemfunctions TO PUBLIC;
