/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _REL_PREDICATES_H_
#define _REL_PREDICATES_H_

#include "rel_rel.h"
#include "rel_exp.h"
#include "mal_backend.h"

typedef struct pl {
	sql_column *c;
	comp_type cmp;
	atom *r; /* if r is NULL then a full match is required */
	atom *f; /* make it match range expressions */
	uint8_t
	 anti:1,
	 semantics:1;
} pl;

extern void rel_predicates(backend *be, sql_rel *rel);

#endif /*_REL_PREDICATES_H_*/
