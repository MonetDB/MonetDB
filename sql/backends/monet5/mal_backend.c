/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
	b->mvc_var = 0;
	b->output_format = OFMT_CSV;
	return b;
}

backend *
backend_create(mvc *m, Client c)
{
	backend *b = MNEW(backend);

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

