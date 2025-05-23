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

#ifndef _SQL_BACKEND_H_
#define _SQL_BACKEND_H_

#include "sql_mem.h"
#include "sql_catalog.h"
#include "sql_relation.h"

typedef void (*freecode_fptr) (const char *mod, int clientid, const char *name);

typedef char *(*create_user_fptr) (ptr mvc, char *user, char *passwd, bool enc, char *fullname, sqlid schema_id, char *schema_path, sqlid grantor_id, lng max_memory, int max_workers, str optimizer, sqlid role_id);
typedef int  (*drop_user_fptr) (ptr mvc, char *user);
typedef oid  (*find_user_fptr) (ptr mvc, char *user);
typedef void (*create_privileges_fptr) (ptr mvc, sql_schema *s, const char *initpasswd);
typedef int  (*schema_has_user_fptr) (ptr mvc, sql_schema *s);
typedef int  (*alter_user_fptr) (ptr mvc, str user, str passwd, bool enc, sqlid schema_id, char *schema_path, str oldpasswd, sqlid role_id, lng max_memory, int max_workers);
typedef int  (*rename_user_fptr) (ptr mvc, str olduser, str newuser);
typedef void*  (*schema_user_dependencies) (ptr mvc, int schema_id);
typedef void  (*create_function) (ptr mvc, str name, sql_rel *rel, sql_table *t);
typedef int  (*resolve_function) (ptr mvc, sql_func *f, const char *fimp, bool *side_effect);
typedef int  (*has_module_function) (ptr mvc, char *name);
typedef void *(*create_sub_backend) (void *mvc, void *client);
typedef int  (*find_role_fptr) (ptr mvc, char *role, sqlid *role_id);

/* backing struct for this interface */
typedef struct _backend_functions {
	freecode_fptr fcode;
	create_user_fptr fcuser;
	drop_user_fptr fduser;
	find_user_fptr ffuser;
	find_role_fptr ffrole;
	create_privileges_fptr fcrpriv;
	schema_has_user_fptr fshuser;
	alter_user_fptr fauser;
	rename_user_fptr fruser;
	schema_user_dependencies fschuserdep;
	resolve_function fresolve_function;
	has_module_function fhas_module_function;
	create_sub_backend sub_backend;
	void (*setIdle)(int, time_t);
} backend_functions;

extern void backend_freecode(const char *mod, int clientid, const char *name);

extern char *backend_create_user(ptr mvc, char *user, char *passwd, bool enc, char *fullname, sqlid defschemid, char *schema_path, sqlid grantor, lng max_memory, int max_workers, char *optimizer, sqlid role_id);
extern int  backend_drop_user(ptr mvc, char *user);
extern oid  backend_find_user(ptr mp, char *user);
extern void backend_create_privileges(ptr mvc, sql_schema *s, const char *initpasswd);
extern int  backend_schema_has_user(ptr mvc, sql_schema *s);
extern int	backend_alter_user(ptr mvc, str user, str passwd, bool enc, sqlid schema_id, char *schema_path, str oldpasswd, sqlid role_id, lng max_memory, int max_workers);
extern int	backend_rename_user(ptr mvc, str olduser, str newuser);
extern void*	backend_schema_user_dependencies(ptr trans, sqlid schema_id);
extern int	backend_resolve_function(ptr trans, sql_func *f, const char *fimp, bool *side_effect);
extern int	backend_has_module(ptr M, char *name);
extern int  backend_find_role(ptr mp, char *role, sqlid *role_id);
extern void backend_set_idle(int clientid, time_t t);

extern backend_functions be_funcs;

#endif /* _SQL_BACKEND_H_ */
