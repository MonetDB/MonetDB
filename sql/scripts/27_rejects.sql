-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

-- COPY into reject management

create function sys.rejects()
returns table(
	rowid bigint,
	fldid int,
	"message" string,
	"input" string
)
external name sql.copy_rejects;

grant execute on function rejects to public;

create view sys.rejects as select * from sys.rejects();
create procedure sys.clearrejects()
external name sql.copy_rejects_clear;
