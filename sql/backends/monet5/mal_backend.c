/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql.h"
#include "mal_backend.h"

backend *
backend_reset(backend *b)
{
	b->out = b->client->fdout;
	b->language = 0;

	b->vtop = 0;
	b->q = NULL;
	b->mb = NULL;
	b->mvc_var = 0;
	b->output_format = OFMT_CSV;
	return b;
}

backend *
backend_create(mvc *m, Client c)
{
	backend *b = MNEW(backend);

	if( b== NULL)
		return NULL;
	b->console = isAdministrator(c);
	b->mvc = m;
	b->client = c;
	b->mvc_var = 0;
	b->output_format = OFMT_CSV;
	return backend_reset(b);
}

void
backend_destroy(backend *b)
{
	_DELETE(b);
}

