/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include <monetdbe.h>

#define error(msg) do{fprintf(stderr, "Failure: %s\n", msg); return -1;}while(0)

int
main(void)
{
	char* err = NULL;
	monetdbe_database mdbe;

	if (monetdbe_open(&mdbe, NULL, NULL))
		error("Failed to open database");

	if ((err = monetdbe_query(mdbe, "CREATE TABLE test (b bool, t tinyint, s smallint, x integer, l bigint, "
							  "h bigint, "
							  "f float, d double, y string)", NULL, NULL)) != NULL)
		error(err);
	if ((err = monetdbe_query(mdbe, "INSERT INTO test VALUES (TRUE, 42, 42, 42, 42, 42, 42.42, 42.42, 'Hello'), (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'World')", NULL, NULL)) != NULL)
		error(err);

	err = monetdbe_dump_database(mdbe, "/tmp/backup");
	if (err)
		error(err);
	if (monetdbe_close(mdbe))
		error("Failed to close database");
	return 0;
}
