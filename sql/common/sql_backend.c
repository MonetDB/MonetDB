/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
backend_freestack(int clientid, backend_stack stk)
{
	if (be_funcs.fstack != NULL)
		be_funcs.fstack(clientid, stk);
}

void
backend_freecode(int clientid, backend_code code, backend_stack stk, int nr, char *name)
{
	if (be_funcs.fcode != NULL)
		be_funcs.fcode(clientid, code, stk, nr, name);
}

char *
backend_create_user(ptr mvc, char *user, char *passwd, char enc, char *fullname, sqlid defschemid, sqlid grantor)
{
	if (be_funcs.fcuser != NULL)
		return(be_funcs.fcuser(mvc, user, passwd, enc, fullname, defschemid, grantor));
	return(NULL);
}

int
backend_drop_user(ptr mvc, char *user)
{
	if (be_funcs.fduser != NULL)
		return(be_funcs.fduser(mvc,user));
	return FALSE;
}

int
backend_find_user(ptr m, char *user)
{
	if (be_funcs.ffuser != NULL)
		return(be_funcs.ffuser(m, user));
	return(0);
}

void
backend_create_privileges(ptr mvc, sql_schema *s)
{
	if (be_funcs.fcrpriv != NULL)
		be_funcs.fcrpriv(mvc, s);
}

int
backend_schema_has_user(ptr mvc, sql_schema *s)
{
	if (be_funcs.fshuser != NULL)
		return(be_funcs.fshuser(mvc, s));
	return(FALSE);
}

int
backend_alter_user(ptr mvc, str user, str passwd, char enc,
		sqlid schema_id, str oldpasswd)
{
	if (be_funcs.fauser != NULL)
		return(be_funcs.fauser(mvc, user, passwd, enc, schema_id, oldpasswd));
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
backend_schema_user_dependencies(ptr trans, int schema_id)
{
	if (be_funcs.fschuserdep != NULL)
		return(be_funcs.fschuserdep(trans, schema_id));
	return NULL;
}

int	
backend_resolve_function(ptr M, sql_func *f)
{
	if (be_funcs.fresolve_function != NULL)
		return be_funcs.fresolve_function(M, f);
	return 0;
}
