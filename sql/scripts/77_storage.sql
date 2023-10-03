-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.

CREATE FUNCTION sys.insertonly_persist()
RETURNS TABLE(
	"table" STRING,
	"table_id" INT,
	"rowcount" BIGINT
)
EXTERNAL NAME sql.insertonly_persist;
GRANT EXECUTE ON FUNCTION sys.insertonly_persist() TO PUBLIC;

CREATE FUNCTION sys.insertonly_persist(sname STRING)
RETURNS TABLE(
	"table" STRING,
	"table_id" INT,
	"rowcount" BIGINT
)
EXTERNAL NAME sql.insertonly_persist;
GRANT EXECUTE ON FUNCTION sys.insertonly_persist(string) TO PUBLIC;

CREATE FUNCTION sys.insertonly_persist(sname STRING, tname STRING)
RETURNS TABLE(
	"table" STRING,
	"table_id" INT,
	"rowcount" BIGINT
)
EXTERNAL NAME sql.insertonly_persist;
GRANT EXECUTE ON FUNCTION sys.insertonly_persist(string, string) TO PUBLIC;
