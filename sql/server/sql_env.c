/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*
 * Environment variables
 * =====================
 *
 * The processing setting of the SQL front-end can collect information
 * for postprocessing and debugging by setting a flag
 * using the SQL construct:
 * SET <variable>=<string>
 * SET <variable>=<boolean>
 * SET <variable>=<int>
 *
 * The SQL engine comes with a limited set of environment variables
 * to control its behavior.
 * The 'debug' variable takes an integer and sets the Mserver global
 * debug flag. (See MonetDB documentation.)
 *
 * By default all remaining variables are stored as strings and
 * any type analysis is up to the user. The can be freely used by the
 * SQL programmer for inclusion in his queries.
 *
 * The limited number of built-in variables defined above are
 * strongly typed the hard way.
 * Moreover, they have a counterpart representation in the
 * MVC structure to ease inspection during query processing.
 *
 * The variables can be retrieved using the table producing function var();
 */

#include "monetdb_config.h"
#include "sql_env.h"
#include "sql_semantic.h"
#include "sql_privileges.h"
#include "mal_exception.h"

#ifdef HAVE_HGE
#define	VAR_UPCAST hge
#else
#define	VAR_UPCAST lng
#endif

str
sql_update_var(mvc *m, const char *name, ValPtr ptr)
{
	if (strcmp(name, "debug") == 0 || strcmp(name, "current_timezone") == 0 || strcmp(name, "cache") == 0) {
		VAR_UPCAST sgn = val_get_number(ptr);

		if (VALisnil(ptr))
			throw(SQL,"sql.update_var", SQLSTATE(42000) "%s cannot be NULL\n", name);
		if (sgn <= (VAR_UPCAST) GDK_int_min)
			throw(SQL,"sql.update_var", SQLSTATE(42000) "Value too small for %s\n", name);
		if (sgn > (VAR_UPCAST) GDK_int_max)
			throw(SQL,"sql.update_var", SQLSTATE(42000) "Value too large for %s\n", name);

		if (strcmp(name, "debug") == 0)
			m->debug = (int) sgn;
		else if (strcmp(name, "current_timezone") == 0)
			m->timezone = (int) sgn;
		else if (strcmp(name, "cache") == 0)
			m->cache = (int) sgn;
	} else if (strcmp(name, "current_schema") == 0 || strcmp(name, "current_role") == 0) {
		if (VALisnil(ptr))
			throw(SQL,"sql.update_var", SQLSTATE(42000) "%s cannot be NULL\n", name);
		if (strcmp(name, "current_schema") == 0 && !mvc_set_schema(m, ptr->val.sval))
			throw(SQL,"sql.update_var", SQLSTATE(3F000) "Schema (%s) missing\n", ptr->val.sval);
		else if (strcmp(name, "current_role") == 0 && !mvc_set_role(m, ptr->val.sval))
			throw(SQL,"sql.update_var", SQLSTATE(42000) "Role (%s) missing\n", ptr->val.sval);
	}
	return NULL;
}

int
sql_create_env(mvc *m, sql_schema *s)
{
	list *res, *ops;

	res = sa_list(m->sa);
	list_append(res, sql_create_arg(m->sa, "name", sql_bind_subtype(m->sa, "varchar", 1024, 0), ARG_OUT));
	list_append(res, sql_create_arg(m->sa, "value", sql_bind_subtype(m->sa, "varchar", 2048, 0), ARG_OUT));

	/* add function */
	ops = sa_list(m->sa);
	mvc_create_func(m, NULL, s, "env", ops, res, F_UNION, FUNC_LANG_SQL, "sql", "sql_environment", "CREATE FUNCTION env () RETURNS TABLE( name varchar(1024), value varchar(2048)) EXTERNAL NAME sql.sql_environment;", FALSE, FALSE, TRUE);

	res = sa_list(m->sa);
	list_append(res, sql_create_arg(m->sa, "name", sql_bind_subtype(m->sa, "varchar", 1024, 0), ARG_OUT));

	/* add function */
	ops = sa_list(m->sa);
	mvc_create_func(m, NULL, s, "var", ops, res, F_UNION, FUNC_LANG_SQL, "sql", "sql_variables", "CREATE FUNCTION var() RETURNS TABLE( name varchar(1024)) EXTERNAL NAME sql.sql_variables;", FALSE, FALSE, TRUE);
	return 0;
}
