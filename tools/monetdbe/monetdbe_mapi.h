/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef MONETDBE_MAPI_H_
#define MONETDBE_MAPI_H_

#include "monetdb_config.h"
#include "stream.h"
#include "mstring.h"
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <monetdbe.h>

struct MapiStruct {
	monetdbe_database mdbe;
	char *msg;
};

struct MapiStatement {
	Mapi mid;
	char *query;
	monetdbe_result *result;
	char **mapi_row;	/* keep buffers for string return values */
	monetdbe_cnt current_row;
	monetdbe_cnt affected_rows;
	char *msg;
};

#endif // MONETDBE_MAPI_H_
