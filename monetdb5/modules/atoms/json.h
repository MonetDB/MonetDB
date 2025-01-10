/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef __JSON_H__
#define __JSON_H__

#include "monetdb_config.h"
#include "gdk.h"

typedef enum JSONkind {
	JSON_OBJECT = 1,
	JSON_ARRAY,
	JSON_ELEMENT,
	JSON_VALUE,
	JSON_STRING,
	JSON_NUMBER,
	JSON_BOOL,
	JSON_NULL
} JSONkind;

/* The JSON index structure is meant for short lived versions */
typedef struct JSONterm {
	JSONkind kind;
	char *name;					/* exclude the quotes */
	size_t namelen;
	const char *value;			/* start of string rep */
	size_t valuelen;
	int child, next, tail;		/* next offsets allow you to walk array/object chains
								   and append quickly */
	/* An array or object item has a number of components */
} JSONterm;

typedef struct JSON {
	JSONterm *elm;
	str error;
	int size;
	int free;
} JSON;

extern JSON *JSONparse(const char *j);
extern void JSONfree(JSON *jt);

#endif /* __JSON_H__ */
