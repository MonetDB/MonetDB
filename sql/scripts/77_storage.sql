-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.

create function sys.insertonly_persist(sname string)
returns table(
	"table" string,
	"table_id" bigint,
	"rowcount" bigint
)
external name sql.insertonly_persist;
grant execute on function sys.insertonly_persist(string) to public;

create function sys.insertonly_persist(sname string, tname string)
returns table(
	"table" string,
	"table_id" bigint,
	"rowcount" bigint
)
external name sql.insertonly_persist;
grant execute on function sys.insertonly_persist(string, string) to public;
