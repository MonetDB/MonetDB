/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _REMOTE_DEF
#define _REMOTE_DEF

#include "mal.h"
#include "mal_client.h"

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

mal_export str RMTdisconnect(Client cntxt, void *ret, const char *const *conn);

#endif /* _REMOTE_DEF */
