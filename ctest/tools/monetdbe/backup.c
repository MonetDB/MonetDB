/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "stream.h"
#include "mstring.h"
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include "mapi.h"
#include <monetdbe.h>

extern int dump_database(Mapi mid, stream *toConsole, bool describe, bool useInserts, bool noescape);

#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); return -1;}

int
main(void)
{
	char* err = NULL;
	Mapi mid = (Mapi)malloc(sizeof(struct MapiStruct));

	if ((mid->msg = monetdbe_open(&mid->mdbe, NULL)) != NULL)
		error(mid->msg);

	if ((err = monetdbe_query(mid->mdbe, "CREATE TABLE test (b bool, t tinyint, s smallint, x integer, l bigint, "
#ifdef HAVE_HGE
		"h hugeint, "
#else
		"h bigint, "
#endif
		"f float, d double, y string)", NULL, NULL)) != NULL)
		error(err)
	if ((err = monetdbe_query(mid->mdbe, "INSERT INTO test VALUES (TRUE, 42, 42, 42, 42, 42, 42.42, 42.42, 'Hello'), (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'World')", NULL, NULL)) != NULL)
		error(err)

	/* open file stream */
	stream *fd = open_wastream("/tmp/backup");
	if (fd) {
		if (dump_database(mid, fd, 0, 0, false)) {
			if (mid->msg)
				error(mid->msg)
			error("database backup failed\n");
		}
		close_stream(fd);
	} else {
		fprintf(stderr, "Failure: unable to open file /tmp/backup: %s", mnstr_peek_error(NULL));
	}

	if ((mid->msg = monetdbe_close(mid->mdbe)) != NULL)
		error(mid->msg);
}
