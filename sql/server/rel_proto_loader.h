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

#ifndef _REL_PROTO_LOADER_H_
#define _REL_PROTO_LOADER_H_

#include "sql_types.h"
#include "sql_mvc.h"

typedef str (*pl_add_types_fptr)(mvc *sql, sql_subfunc *f, char *url, list *res_exps, char *name);
typedef void *(*pl_load_fptr)(void *be, sql_subfunc *f, char *url, sql_exp *topn); /* use void * as both return type and be
																			argument are unknown types at this layer */

typedef struct proto_loader_t {
	char *name;
	pl_add_types_fptr add_types;
	pl_load_fptr load;
} proto_loader_t;

sql_export int pl_register(char *name, pl_add_types_fptr add_types, pl_load_fptr pl_load);
sql_export void pl_unregister(char *name);
extern proto_loader_t* pl_find(char *name);

extern void pl_exit(void);

#endif /*_REL_PROTO_LOADER_H_*/
