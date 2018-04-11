/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _SQL_BACKEND_H_
#define _SQL_BACKEND_H_

#include "sql_mem.h"
#include "sql_catalog.h"
#include "sql_relation.h"
#include "sql_types.h"

typedef void (*freestack_fptr) (int clientid, backend_stack stk);
typedef void (*freecode_fptr) (int clientid, backend_code code, backend_stack stk, int nr, char *name);

typedef char *(*create_user_fptr) (ptr mvc, char *user, char *passwd, char enc, char *fullname, sqlid schema_id, sqlid grantor_id);
typedef int  (*drop_user_fptr) (ptr mvc, char *user);
typedef int  (*find_user_fptr) (ptr mvc, char *user);
typedef void (*create_privileges_fptr) (ptr mvc, sql_schema *s);
typedef int  (*schema_has_user_fptr) (ptr mvc, sql_schema *s);
typedef int  (*alter_user_fptr) (ptr mvc, str user, str passwd, char enc, sqlid schema_id, str oldpasswd);
typedef int  (*rename_user_fptr) (ptr mvc, str olduser, str newuser);
typedef void*  (*schema_user_dependencies) (ptr mvc, int schema_id);
typedef void  (*create_function) (ptr mvc, str name, sql_rel *rel, sql_table *t);
typedef int  (*resolve_function) (ptr mvc, sql_func *f);

/* backing struct for this interface */
typedef struct _backend_functions {
	freestack_fptr fstack;
	freecode_fptr fcode;
	create_user_fptr fcuser;
	drop_user_fptr fduser;
	find_user_fptr ffuser;
	create_privileges_fptr fcrpriv;
	schema_has_user_fptr fshuser;
	alter_user_fptr fauser;
	rename_user_fptr fruser;
	schema_user_dependencies fschuserdep;
	resolve_function fresolve_function;
} backend_functions;

extern void backend_freestack(int clientid, backend_stack stk);
extern void backend_freecode(int clientid, backend_code code, backend_stack stk, int nr, char *name);

extern char *backend_create_user(ptr mvc, char *user, char *passwd, char enc, char *fullname, sqlid defschemid, sqlid grantor);
extern int  backend_drop_user(ptr mvc, char *user);
extern int  backend_find_user(ptr mp, char *user);
extern void backend_create_privileges(ptr mvc, sql_schema *s);
extern int  backend_schema_has_user(ptr mvc, sql_schema *s);
extern int	backend_alter_user(ptr mvc, str user, str passwd, char enc, sqlid schema_id, str oldpasswd);
extern int	backend_rename_user(ptr mvc, str olduser, str newuser);
extern void*	backend_schema_user_dependencies(ptr trans, int schema_id);
extern int	backend_resolve_function(ptr trans, sql_func *f);

extern backend_functions be_funcs;

#endif /* _SQL_BACKEND_H_ */
