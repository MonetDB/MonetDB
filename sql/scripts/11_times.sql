-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

-- Provide a simple equivalent for the UNIX times command
-- times 0 ms user 0 ms system 0 ms 0 reads 0 writes

create procedure times()
external name sql.times;

grant execute on procedure times to public;
