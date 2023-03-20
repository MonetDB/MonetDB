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
#include "sql.h"
#include "mal_backend.h"

backend *
backend_reset(backend *b)
{
	if (b->subbackend)
		b->subbackend->reset(b->subbackend);
	*b = (backend) {
		.mvc = b->mvc,
		.client = b->client,
		.out = b->client->fdout,
		.output_format = OFMT_CSV,
		.rowcnt = -1,
		.last_id = -1,
		.subbackend = b->subbackend,
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
	if (b && be_funcs.sub_backend)
		b->subbackend = be_funcs.sub_backend(m, c);
	return backend_reset(b);
}

void
backend_destroy(backend *b)
{
	if (b->subbackend)
		b->subbackend->destroy(b->subbackend);
	_DELETE(b);
}

/* for recursive functions, if the implementation is not set yet, take it from the current compilation */
str
backend_function_imp(backend *b, sql_func *f)
{
	str res = sql_func_imp(f);

	if (b->mvc->forward && strcmp(res, "") == 0 && b->mvc->forward->base.id == f->base.id)
		res = b->fimp;
	return res;
}
