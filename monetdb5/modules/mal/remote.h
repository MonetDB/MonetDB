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

#ifndef _REMOTE_DEF
#define _REMOTE_DEF

#include "mal.h"

typedef struct {
	bat id;
	char *colname;
	char *tpename;
	int digits;
	int scale;
} columnar_result;

typedef struct {
	void *context;
	str (*call)(void *context, char *tblname, columnar_result * columns,
				size_t nrcolumns);
} columnar_result_callback;

mal_export str RMTdisconnect(void *ret, const char *const *conn);

#endif /* _REMOTE_DEF */
