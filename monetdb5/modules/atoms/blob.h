/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*
 * @* Implementation Code
 */
#ifndef __BLOB_H__
#define __BLOB_H__
#include "mal.h"
#include "mal_exception.h"

typedef struct blob {
	size_t nitems;
	char data[FLEXIBLE_ARRAY_MEMBER] __attribute__((__nonstring__));
} blob;

mal_export int TYPE_blob;

mal_export var_t blobsize(size_t nitems);
mal_export ssize_t BLOBfromstr(const char *instr, size_t *l, blob **val, bool external);
mal_export ssize_t BLOBtostr(str *tostr, size_t *l, const blob *pin, bool external);

#endif /* __BLOB_H__ */
