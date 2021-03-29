/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * @f sql_user
 * @t SQL catalog management
 * @a N. Nes, F. Groffen
 * @+ SQL user
 * The SQL user and authorisation implementation differs per backend.  This
 * file implements the authorisation and user management based on the M5
 * system authorisation.
 */
#include "monetdb_config.h"
#include "sql_user.h"
#include "sql_mvc.h"
#include "sql_privileges.h"
#include "bat5.h"
#include "mal_interpreter.h"
#include "mal_authorize.h"
#include "mcrypt.h"

static int
monet5_drop_user(ptr _mvc, str user)
{
	mvc *m = (mvc *) _mvc;
	oid rid;
	sql_schema *sys;
	sql_table *users;
	sql_column *users_name;
	str err;
	Client c = MCgetClient(m->clientid);

	oid grant_user = c->user;
	c->user = MAL_ADMIN;
	err = AUTHremoveUser(c, user);
	c->user = grant_user;
	if (err !=MAL_SUCCEED) {
		(void) sql_error(m, 02, "DROP USER: %s", getExceptionMessage(err));
		freeException(err);
		return FALSE;
	}
	sys = find_sql_schema(m->session->tr, "sys");
	users = find_sql_table(m->session->tr, sys, "db_user_info");
	users_name = find_sql_column(users, "name");

	sqlstore *store = m->session->tr->store;
	rid = store->table_api.column_find_row(m->session->tr, users_name, user, NULL);
	if (!is_oid_nil(rid))
		store->table_api.table_delete(m->session->tr, users, rid);
	/* FIXME: We have to ignore this inconsistency here, because the
	 * user was already removed from the system authorisation. Once
	 * we have warnings, we could issue a warning about this
	 * (seemingly) inconsistency between system and sql shadow
	 * administration. */

	return TRUE;
}

#define outside_str 1
#define inside_str 2
#define default_schema_path "\"sys\"" /* "sys" will be the default schema path */

static str
parse_schema_path_str(mvc *m, str schema_path, bool build) /* this function for both building and validating the schema path */
{
	list *l = m->schema_path;
	char next_schema[1024]; /* needs one extra character for null terminator */
	int status = outside_str;
	size_t bp = 0;

	if (strNil(schema_path))
		throw(SQL, "sql.schema_path", SQLSTATE(42000) "A schema path cannot be NULL");

	if (build) {
		while (l->t) /* if building, empty schema_path list */
			(void) list_remove_node(l, NULL, l->t);
		m->schema_path_has_sys = 0;
		m->schema_path_has_tmp = 0;
	}

	for (size_t i = 0; schema_path[i]; i++) {
		char next = schema_path[i];

		if (next == '"') {
			if (status == inside_str && schema_path[i + 1] == '"') {
				next_schema[bp++] = '"';
				i++; /* has to advance two positions */
			} else if (status == inside_str) {
				if (bp == 0)
					throw(SQL, "sql.schema_path", SQLSTATE(42000) "A schema name cannot be empty");
				if (bp == 1023)
					throw(SQL, "sql.schema_path", SQLSTATE(42000) "A schema has up to 1023 characters");

				if (build) {
					char *val = NULL;
					next_schema[bp++] = '\0';
					if (!(val = _STRDUP(next_schema)) || !list_append(l, val)) {
						_DELETE(val);
						throw(SQL, "sql.schema_path", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					}
					if (strcmp(next_schema, "sys") == 0)
						m->schema_path_has_sys = 1;
					else if (strcmp(next_schema, "tmp") == 0)
						m->schema_path_has_tmp = 1;
				}

				bp = 0;
				status = outside_str;
			} else {
				assert(status == outside_str);
				status = inside_str;
			}
		} else if (next == ',') {
			if (status == outside_str && schema_path[i + 1] == '"') {
				status = inside_str;
				i++; /* has to advance two positions */
			} else if (status == inside_str) {
				if (bp == 1023)
					throw(SQL, "sql.schema_path", SQLSTATE(42000) "A schema has up to 1023 characters");
				next_schema[bp++] = ','; /* used inside a schema name */
			} else if (status == outside_str) {
				throw(SQL, "sql.schema_path", SQLSTATE(42000) "The '\"' character is expected after the comma separator");
			}
		} else if (status == inside_str) {
			if (bp == 1023)
				throw(SQL, "sql.schema_path", SQLSTATE(42000) "A schema has up to 1023 characters");
			if (bp == 0 && next == '%')
				throw(SQL, "sql.schema_path", SQLSTATE(42000) "The character '%%' is not allowed as the first schema character");
			next_schema[bp++] = next;
		} else {
			assert(status == outside_str);
			throw(SQL, "sql.schema_path", SQLSTATE(42000) "A schema in the path must be within '\"'");
		}
	}
	if (status == inside_str)
		throw(SQL, "sql.schema_path", SQLSTATE(42000) "A schema path cannot end inside inside a schema name");
	return MAL_SUCCEED;
}

static str
monet5_create_user(ptr _mvc, str user, str passwd, char enc, str fullname, sqlid schema_id, str schema_path, sqlid grantorid)
{
	mvc *m = (mvc *) _mvc;
	oid uid = 0;
	str ret, pwd;
	sqlid user_id;
	sql_schema *s = find_sql_schema(m->session->tr, "sys");
	sql_table *db_user_info, *auths;
	Client c = MCgetClient(m->clientid);
	sqlstore *store = m->session->tr->store;

	if (!schema_path)
		schema_path = default_schema_path;
	if ((ret = parse_schema_path_str(m, schema_path, false)) != MAL_SUCCEED)
		return ret;

	if (!enc) {
		if (!(pwd = mcrypt_BackendSum(passwd, strlen(passwd))))
			throw(MAL, "sql.create_user", SQLSTATE(42000) "Crypt backend hash not found");
	} else {
		pwd = passwd;
	}
	/* add the user to the M5 authorisation administration */
	oid grant_user = c->user;
	c->user = MAL_ADMIN;
	ret = AUTHaddUser(&uid, c, user, pwd);
	c->user = grant_user;
	if (!enc)
		free(pwd);
	if (ret != MAL_SUCCEED)
		return ret;

	user_id = store_next_oid(m->session->tr->store);
	db_user_info = find_sql_table(m->session->tr, s, "db_user_info");
	auths = find_sql_table(m->session->tr, s, "auths");
	store->table_api.table_insert(m->session->tr, db_user_info, &user, &fullname, &schema_id, &schema_path);
	store->table_api.table_insert(m->session->tr, auths, &user_id, &user, &grantorid);
	return NULL;
}

static int
monet5_find_user(ptr mp, str user)
{
	BAT *uid, *nme;
	BUN p;
	mvc *m = (mvc *) mp;
	Client c = MCgetClient(m->clientid);
	str err;

	if ((err = AUTHgetUsers(&uid, &nme, c)) != MAL_SUCCEED) {
		freeException(err);
		return -1;
	}
	p = BUNfnd(nme, user);
	BBPunfix(uid->batCacheid);
	BBPunfix(nme->batCacheid);

	/* yeah, I would prefer to return something different too */
	return (p == BUN_NONE ? -1 : 1);
}

str
db_users_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *r = getArgReference_bat(stk, pci, 0);
	BAT *uid, *nme;
	str err;

	(void) mb;
	if ((err = AUTHgetUsers(&uid, &nme, cntxt)) != MAL_SUCCEED)
		return err;
	BBPunfix(uid->batCacheid);
	*r = nme->batCacheid;
	BBPkeepref(*r);
	return MAL_SUCCEED;
}

str
db_password_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;

	if (stk->stk[pci->argv[0]].vtype == TYPE_bat) {
		BAT *b = BATdescriptor(*getArgReference_bat(stk, pci, 1));
		if (b == NULL)
			throw(SQL, "sql.password", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		BAT *bn = COLnew(b->hseqbase, TYPE_str, BATcount(b), TRANSIENT);
		if (bn == NULL) {
			BBPunfix(b->batCacheid);
			throw(SQL, "sql.password", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		BATiter bi = bat_iterator(b);
		BUN p, q;
		BATloop(b, p, q) {
			char *hash, *msg;
			msg = AUTHgetPasswordHash(&hash, cntxt, BUNtvar(bi, p));
			if (msg != MAL_SUCCEED) {
				BBPunfix(b->batCacheid);
				BBPreclaim(bn);
				return msg;
			}
			if (BUNappend(bn, hash, false) != GDK_SUCCEED) {
				BBPunfix(b->batCacheid);
				BBPreclaim(bn);
				throw(SQL, "sql.password", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			GDKfree(hash);
		}
		BBPunfix(b->batCacheid);
		BBPkeepref(bn->batCacheid);
		*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
		return MAL_SUCCEED;
	}
	str *hash = getArgReference_str(stk, pci, 0);
	str *user = getArgReference_str(stk, pci, 1);

	return AUTHgetPasswordHash(hash, cntxt, *user);
}

static void
monet5_create_privileges(ptr _mvc, sql_schema *s)
{
	sql_table *t, *uinfo;
	mvc *m = (mvc *) _mvc;
	sqlid schema_id = 0;
	list *res, *ops;

	/* create the authorisation related tables */
	t = mvc_create_table(m, s, "db_user_info", tt_table, 1, SQL_PERSIST, 0, -1, 0);
	mvc_create_column_(m, t, "name", "varchar", 1024);
	mvc_create_column_(m, t, "fullname", "varchar", 2048);
	mvc_create_column_(m, t, "default_schema", "int", 9);
	mvc_create_column_(m, t, "schema_path", "clob", 0);
	uinfo = t;

	res = sa_list(m->sa);
	list_append(res, sql_create_arg(m->sa, "name", sql_bind_subtype(m->sa, "varchar", 2048, 0), ARG_OUT));

	/* add function */
	ops = sa_list(m->sa);
	/* following funcion returns a table (single column) of user names
	   with the approriate scenario (sql) */
	mvc_create_func(m, NULL, s, "db_users", ops, res, F_UNION, FUNC_LANG_SQL, "sql", "db_users", "CREATE FUNCTION db_users () RETURNS TABLE( name varchar(2048)) EXTERNAL NAME sql.db_users;", FALSE, FALSE, TRUE);

	t = mvc_init_create_view(m, s, "users",
			    "create view sys.users as select u.\"name\" as \"name\", "
			    "ui.\"fullname\", ui.\"default_schema\", "
				"ui.\"schema_path\" from sys.db_users() as u "
				"left join \"sys\".\"db_user_info\" as ui "
			    "on u.\"name\" = ui.\"name\";");
	if (!t) {
		TRC_CRITICAL(SQL_TRANS, "Failed to create 'users' view\n");
		return ;
	}

	mvc_create_column_(m, t, "name", "varchar", 2048);
	mvc_create_column_(m, t, "fullname", "varchar", 2048);
	mvc_create_column_(m, t, "default_schema", "int", 9);
	mvc_create_column_(m, t, "schema_path", "clob", 0);

	schema_id = sql_find_schema(m, "sys");
	assert(schema_id >= 0);

	sqlstore *store = m->session->tr->store;
	char *username = "monetdb";
	char *fullname = "MonetDB Admin";
	char *schema_path = default_schema_path;
	store->table_api.table_insert(m->session->tr, uinfo, &username, &fullname, &schema_id, &schema_path);
}

static int
monet5_schema_has_user(ptr _mvc, sql_schema *s)
{
	mvc *m = (mvc *) _mvc;
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *users = find_sql_table(m->session->tr, sys, "db_user_info");
	sql_column *users_schema = find_sql_column(users, "default_schema");
	sqlid schema_id = s->base.id;

	sqlstore *store = m->session->tr->store;
	rid = store->table_api.column_find_row(m->session->tr, users_schema, &schema_id, NULL);
	if (is_oid_nil(rid))
		return FALSE;
	return TRUE;
}

static int
monet5_alter_user(ptr _mvc, str user, str passwd, char enc, sqlid schema_id, str schema_path, str oldpasswd)
{
	mvc *m = (mvc *) _mvc;
	Client c = MCgetClient(m->clientid);
	str err;

	if (passwd != NULL) {
		str pwd = NULL;
		str opwd = NULL;
		if (!enc) {
			pwd = mcrypt_BackendSum(passwd, strlen(passwd));
			if (pwd == NULL) {
				(void) sql_error(m, 02, SQLSTATE(42000) "ALTER USER: crypt backend hash not found");
				return FALSE;
			}
			if (oldpasswd != NULL) {
				opwd = mcrypt_BackendSum(oldpasswd, strlen(oldpasswd));
				if (opwd == NULL) {
					free(pwd);
					(void) sql_error(m, 02, SQLSTATE(42000) "ALTER USER: crypt backend hash not found");
					return FALSE;
				}
			}
		} else {
			pwd = passwd;
			opwd = oldpasswd;
		}
		if (user == NULL) {
			err = AUTHchangePassword(c, opwd, pwd);
			if (!enc) {
				free(pwd);
				free(opwd);
			}
			if (err !=MAL_SUCCEED) {
				(void) sql_error(m, 02, "ALTER USER: %s", getExceptionMessage(err));
				freeException(err);
				return (FALSE);
			}
		} else {
			str username = NULL;
			if ((err = AUTHresolveUser(&username, c->user)) !=MAL_SUCCEED) {
				if (!enc) {
					free(pwd);
					free(opwd);
				}
				(void) sql_error(m, 02, "ALTER USER: %s", getExceptionMessage(err));
				freeException(err);
				return (FALSE);
			}
			if (strcmp(username, user) == 0) {
				/* avoid message about changePassword (from MAL level) */
				GDKfree(username);
				if (!enc) {
					free(pwd);
					free(opwd);
				}
				(void) sql_error(m, 02, "ALTER USER: "
					"use 'ALTER USER SET [ ENCRYPTED ] PASSWORD xxx "
					"USING OLD PASSWORD yyy' "
					"when changing your own password");
				return (FALSE);
			}
			GDKfree(username);
			err = AUTHsetPassword(c, user, pwd);
			if (!enc) {
				free(pwd);
				free(opwd);
			}
			if (err !=MAL_SUCCEED) {
				(void) sql_error(m, 02, "ALTER USER: %s", getExceptionMessage(err));
				freeException(err);
				return (FALSE);
			}
		}
	}

	sqlstore *store = m->session->tr->store;
	if (schema_id) {
		sql_schema *sys = find_sql_schema(m->session->tr, "sys");
		sql_table *info = find_sql_table(m->session->tr, sys, "db_user_info");
		sql_column *users_name = find_sql_column(info, "name");
		sql_column *users_schema = find_sql_column(info, "default_schema");

		/* FIXME: we don't really check against the backend here */
		oid rid = store->table_api.column_find_row(m->session->tr, users_name, user, NULL);
		if (is_oid_nil(rid))
			return FALSE;
		store->table_api.column_update_value(m->session->tr, users_schema, rid, &schema_id);
	}

	if (schema_path) {
		sql_schema *sys = find_sql_schema(m->session->tr, "sys");
		sql_table *info = find_sql_table(m->session->tr, sys, "db_user_info");
		sql_column *users_name = find_sql_column(info, "name");
		sql_column *sp = find_sql_column(info, "schema_path");

		if ((err = parse_schema_path_str(m, schema_path, false)) != MAL_SUCCEED) {
			(void) sql_error(m, 02, "ALTER USER: %s", getExceptionMessage(err));
			freeException(err);
			return (FALSE);
		}

		oid rid = store->table_api.column_find_row(m->session->tr, users_name, user, NULL);
		if (is_oid_nil(rid))
			return FALSE;
		store->table_api.column_update_value(m->session->tr, sp, rid, schema_path);
	}

	return TRUE;
}

static int
monet5_rename_user(ptr _mvc, str olduser, str newuser)
{
	mvc *m = (mvc *) _mvc;
	Client c = MCgetClient(m->clientid);
	str err;
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *info = find_sql_table(m->session->tr, sys, "db_user_info");
	sql_column *users_name = find_sql_column(info, "name");
	sql_table *auths = find_sql_table(m->session->tr, sys, "auths");
	sql_column *auths_name = find_sql_column(auths, "name");

	if ((err = AUTHchangeUsername(c, olduser, newuser)) !=MAL_SUCCEED) {
		(void) sql_error(m, 02, "ALTER USER: %s", getExceptionMessage(err));
		freeException(err);
		return (FALSE);
	}

	sqlstore *store = m->session->tr->store;
	rid = store->table_api.column_find_row(m->session->tr, users_name, olduser, NULL);
	if (is_oid_nil(rid)) {
		(void) sql_error(m, 02, "ALTER USER: local inconsistency, "
				 "your database is damaged, user not found in SQL catalog");
		return (FALSE);
	}
	store->table_api.column_update_value(m->session->tr, users_name, rid, newuser);

	rid = store->table_api.column_find_row(m->session->tr, auths_name, olduser, NULL);
	if (is_oid_nil(rid)) {
		(void) sql_error(m, 02, "ALTER USER: local inconsistency, "
				 "your database is damaged, auth not found in SQL catalog");
		return (FALSE);
	}
	store->table_api.column_update_value(m->session->tr, auths_name, rid, newuser);

	return (TRUE);
}

static void *
monet5_schema_user_dependencies(ptr _trans, int schema_id)
{
	rids *A, *U;
	sql_trans *tr = (sql_trans *) _trans;
	sql_schema *s = find_sql_schema(tr, "sys");

	sql_table *auths = find_sql_table(tr, s, "auths");
	sql_column *auth_name = find_sql_column(auths, "name");

	sql_table *users = find_sql_table(tr, s, "db_user_info");
	sql_column *users_name = find_sql_column(users, "name");
	sql_column *users_sch = find_sql_column(users, "default_schema");

	sqlstore *store = tr->store;
	/* select users with given schema */
	U = store->table_api.rids_select(tr, users_sch, &schema_id, &schema_id, NULL);
	/* select all authorization ids */
	A = store->table_api.rids_select(tr, auth_name, NULL, NULL);
	/* join all authorization with the selected users */
	A = store->table_api.rids_join(tr, A, auth_name, U, users_name);
	store->table_api.rids_destroy(U);
	return A;
}

void
monet5_user_init(backend_functions *be_funcs)
{
	be_funcs->fcuser = &monet5_create_user;
	be_funcs->fduser = &monet5_drop_user;
	be_funcs->ffuser = &monet5_find_user;
	be_funcs->fcrpriv = &monet5_create_privileges;
	be_funcs->fshuser = &monet5_schema_has_user;
	be_funcs->fauser = &monet5_alter_user;
	be_funcs->fruser = &monet5_rename_user;
	be_funcs->fschuserdep = &monet5_schema_user_dependencies;
}

str
monet5_user_get_def_schema(mvc *m, int user)
{
	oid rid;
	sqlid schema_id = int_nil;
	sql_schema *sys = NULL;
	sql_table *user_info = NULL;
	sql_table *schemas = NULL;
	sql_table *auths = NULL;
	str username = NULL;
	str schema = NULL;
	sqlstore *store = m->session->tr->store;
	ptr cbat;

	sys = find_sql_schema(m->session->tr, "sys");
	auths = find_sql_table(m->session->tr, sys, "auths");
	user_info = find_sql_table(m->session->tr, sys, "db_user_info");
	schemas = find_sql_table(m->session->tr, sys, "schemas");

	rid = store->table_api.column_find_row(m->session->tr, find_sql_column(auths, "id"), &user, NULL);
	if (is_oid_nil(rid))
		return NULL;
	username = store->table_api.column_find_string_start(m->session->tr, find_sql_column(auths, "name"), rid, &cbat);
	rid = store->table_api.column_find_row(m->session->tr, find_sql_column(user_info, "name"), username, NULL);
	store->table_api.column_find_string_end(cbat);

	if (!is_oid_nil(rid))
		schema_id = store->table_api.column_find_sqlid(m->session->tr, find_sql_column(user_info, "default_schema"), rid);
	if (!is_int_nil(schema_id)) {
		rid = store->table_api.column_find_row(m->session->tr, find_sql_column(schemas, "id"), &schema_id, NULL);
		if (!is_oid_nil(rid)) {
			str sname = store->table_api.column_find_string_start(m->session->tr, find_sql_column(schemas, "name"), rid, &cbat);
			schema = sa_strdup(m->session->sa, sname);
			store->table_api.column_find_string_end(cbat);
		}
	}
	return schema;
}

str
monet5_user_set_def_schema(mvc *m, oid user)
{
	oid rid;
	sqlid schema_id;
	sql_schema *sys = NULL;
	sql_table *user_info = NULL;
	sql_column *users_name = NULL;
	sql_column *users_schema = NULL;
	sql_column *users_schema_path = NULL;
	sql_table *schemas = NULL;
	sql_column *schemas_name = NULL;
	sql_column *schemas_id = NULL;
	sql_table *auths = NULL;
	sql_column *auths_name = NULL;
	str path_err = NULL, other = NULL, schema = NULL, schema_path = NULL, username = NULL, err = NULL;
	void *p = 0;

	TRC_DEBUG(SQL_TRANS, OIDFMT "\n", user);

	if ((err = AUTHresolveUser(&username, user)) != MAL_SUCCEED) {
		freeException(err);
		return (NULL);	/* don't reveal that the user doesn't exist */
	}

	if (mvc_trans(m) < 0) {
		GDKfree(username);
		return NULL;
	}

	sys = find_sql_schema(m->session->tr, "sys");
	user_info = find_sql_table(m->session->tr, sys, "db_user_info");
	users_name = find_sql_column(user_info, "name");
	users_schema = find_sql_column(user_info, "default_schema");
	users_schema_path = find_sql_column(user_info, "schema_path");

	sqlstore *store = m->session->tr->store;
	rid = store->table_api.column_find_row(m->session->tr, users_name, username, NULL);
	if (is_oid_nil(rid)) {
		if (m->session->tr->active && (other = mvc_rollback(m, 0, NULL, false)) != MAL_SUCCEED)
			freeException(other);
		GDKfree(username);
		return NULL;
	}
	schema_id = store->table_api.column_find_sqlid(m->session->tr, users_schema, rid);

	p = store->table_api.column_find_value(m->session->tr, users_schema_path, rid);
	assert(p);
	schema_path = (str) p;

	schemas = find_sql_table(m->session->tr, sys, "schemas");
	schemas_name = find_sql_column(schemas, "name");
	schemas_id = find_sql_column(schemas, "id");
	auths = find_sql_table(m->session->tr, sys, "auths");
	auths_name = find_sql_column(auths, "name");

	rid = store->table_api.column_find_row(m->session->tr, schemas_id, &schema_id, NULL);
	if (!is_oid_nil(rid))
		schema = store->table_api.column_find_value(m->session->tr, schemas_name, rid);

	if (schema) {
		char *old = schema;
		schema = sa_strdup(m->session->sa, schema);
		_DELETE(old);
	}

	/* only set schema if user is found */
	rid = store->table_api.column_find_row(m->session->tr, auths_name, username, NULL);
	if (!is_oid_nil(rid)) {
		sql_column *auths_id = find_sql_column(auths, "id");
		sqlid id = store->table_api.column_find_sqlid(m->session->tr, auths_id, rid);

		m->user_id = m->role_id = id;
	} else {
		schema = NULL;
	}

	/* while getting the session's schema, set the search path as well */
	if (!schema || !mvc_set_schema(m, schema) || (path_err = parse_schema_path_str(m, schema_path, true)) != MAL_SUCCEED) {
		if (m->session->tr->active) {
			if ((other = mvc_rollback(m, 0, NULL, false)) != MAL_SUCCEED)
				freeException(other);
		}
		GDKfree(username);
		_DELETE(schema_path);
		freeException(path_err);
		return NULL;
	}
	/* reset the user and schema names */
	if (!sqlvar_set_string(find_global_var(m, sys, "current_schema"), schema) ||
		!sqlvar_set_string(find_global_var(m, sys, "current_user"), username) ||
		!sqlvar_set_string(find_global_var(m, sys, "current_role"), username)) {
		schema = NULL;
	}
	GDKfree(username);
	_DELETE(schema_path);
	if ((other = mvc_rollback(m, 0, NULL, false)) != MAL_SUCCEED) {
		freeException(other);
		return NULL;
	}
	return schema;
}
