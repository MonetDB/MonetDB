-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- For copyright information, see the file debian/copyright.

CREATE FUNCTION sys.persist_unlogged(sname STRING, tname STRING)
RETURNS TABLE(
	"table" STRING,
	"table_id" INT,
	"rowcount" BIGINT
)
EXTERNAL NAME sql.persist_unlogged;
GRANT EXECUTE ON FUNCTION sys.persist_unlogged(string, string) TO PUBLIC;
