/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql.h"
#include "mal_backend.h"

backend *
backend_reset(backend *b)
{
	*b = (backend) {
		.mvc = b->mvc,
		.client = b->client,
		.out = b->client->fdout,
	    .first_statement_generated = false,
		.output_format = OFMT_CSV,
		.rowcnt = -1,
		.last_id = -1,
		.sizeheader = false
	};
	return b;
}

backend *
backend_create(mvc *m, Client c)
{
	backend *b = MNEW(backend);

	if (!b)
		return NULL;
	*b = (backend) {
		.mvc = m,
		.client = c,
	};
	return backend_reset(b);
}

void
backend_destroy(backend *b)
{
	_DELETE(b);
}
