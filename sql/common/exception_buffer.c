/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "exception_buffer.h"
#include <string.h>

exception_buffer *
eb_init(exception_buffer *eb)
{
	if (eb) {
		eb->enabled = 0;
		eb->code = 0;
		eb->msg = NULL;
	}
	return eb;
}

void 
eb_error( exception_buffer *eb, char *msg, int val ) 
{
	eb->code = val;
	eb->msg = msg;
	fprintf(stderr, "%s\n", msg?msg:"ERROR");
	longjmp(eb->state, eb->code);
}
