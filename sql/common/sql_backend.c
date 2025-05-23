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

/*
 * The back end structure collects the information needed to support
 * compilation and execution of the SQL code against a back-end engine.
 * Note that any back-end can be called upon by the front-end
 * to handle specific tasks, such as catalog management (sql_mvc)
 * and query execution (sql_qc). For this purpose, the front-end needs
 * access to operations defined in the back-end, in particular for
 * freeing the stack and code segment.
 *
 * The front-end should not rely on knowledge how the back end handles
 * the actual query processing. Using the sample back end Monet5
 * are used to find the common ground here. The structure currently is a
 * simple functional wrapper. It assumes that a single back-end is used
 * for the duration of a session.
 */

#include "monetdb_config.h"
#include "sql_backend.h"

backend_functions be_funcs;

void
backend_freecode(const char *mod, int clientid, const char *name)
{
	if (be_funcs.fcode != NULL)
		be_funcs.fcode(mod, clientid, name);
}

char *
backend_create_user(ptr mvc, char *user, char *passwd, bool enc, char *fullname, sqlid defschemid, char *schema_path, sqlid grantor, lng max_memory, int max_workers, char *optimizer, sqlid role_id)
{
	if (be_funcs.fcuser != NULL)
		return(be_funcs.fcuser(mvc, user, passwd, enc, fullname, defschemid, schema_path, grantor, max_memory,
					max_workers, optimizer, role_id));
	return(NULL);
}

int
backend_drop_user(ptr mvc, char *user)
{
	if (be_funcs.fduser != NULL)
		return(be_funcs.fduser(mvc,user));
	return FALSE;
}

oid
backend_find_user(ptr m, char *user)
{
	if (be_funcs.ffuser != NULL)
		return(be_funcs.ffuser(m, user));
	return(0);
}

void
backend_create_privileges(ptr mvc, sql_schema *s, const char *initpasswd)
{
	if (be_funcs.fcrpriv != NULL)
		be_funcs.fcrpriv(mvc, s, initpasswd);
}

int
backend_schema_has_user(ptr mvc, sql_schema *s)
{
	if (be_funcs.fshuser != NULL)
		return(be_funcs.fshuser(mvc, s));
	return(FALSE);
}

int
backend_alter_user(ptr mvc, str user, str passwd, bool enc,
				   sqlid schema_id, char *schema_path, str oldpasswd, sqlid role_id, lng max_memory, int max_workers)
{
	if (be_funcs.fauser != NULL)
		return(be_funcs.fauser(mvc, user, passwd, enc, schema_id, schema_path, oldpasswd, role_id, max_memory, max_workers));
	return(FALSE);
}

int
backend_rename_user(ptr mvc, str olduser, str newuser)
{
	if (be_funcs.fruser != NULL)
		return(be_funcs.fruser(mvc, olduser, newuser));
	return(FALSE);
}

void*
backend_schema_user_dependencies(ptr trans, sqlid schema_id)
{
	if (be_funcs.fschuserdep != NULL)
		return(be_funcs.fschuserdep(trans, schema_id));
	return NULL;
}

int
backend_resolve_function(ptr M, sql_func *f, const char *fimp, bool *side_effect)
{
	if (be_funcs.fresolve_function != NULL)
		return be_funcs.fresolve_function(M, f, fimp, side_effect);
	return 1;
}

int
backend_has_module(ptr M, char *name)
{
	if (be_funcs.fhas_module_function != NULL)
		return be_funcs.fhas_module_function(M, name);
	return 1;
}

int
backend_find_role(ptr mvc, char *name, sqlid *role_id)
{
	if (be_funcs.ffrole != NULL)
		return be_funcs.ffrole(mvc, name, role_id);
	return 0;
}

void
backend_set_idle(int clientid, time_t t)
{
	if (be_funcs.setIdle != NULL)
		be_funcs.setIdle(clientid, t);
}
