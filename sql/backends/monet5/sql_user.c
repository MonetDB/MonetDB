/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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

#if 0
int
sql_find_auth_schema(mvc *m, str auth)
{
	int res = -1;
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *users = find_sql_table(sys, "db_user_info");
	sql_column *users_name = find_sql_column(users, "name");

	rid = table_funcs.column_find_row(m->session->tr, users_name, auth, NULL);

	if (!is_oid_nil(rid)) {
		sql_column *users_schema = find_sql_column(users, "default_schema");
		int *p = (int *) table_funcs.column_find_value(m->session->tr, users_schema, rid);

		if (p) {
			res = *p;
			_DELETE(p);
		}
	}
	return res;
}
#endif

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

	err = AUTHremoveUser(c, user);
	if (err !=MAL_SUCCEED) {
		(void) sql_error(m, 02, "DROP USER: %s", getExceptionMessage(err));
		_DELETE(err);
		return FALSE;
	}
	sys = find_sql_schema(m->session->tr, "sys");
	users = find_sql_table(sys, "db_user_info");
	users_name = find_sql_column(users, "name");

	rid = table_funcs.column_find_row(m->session->tr, users_name, user, NULL);
	if (!is_oid_nil(rid))
		table_funcs.table_delete(m->session->tr, users, rid);
	/* FIXME: We have to ignore this inconsistency here, because the
	 * user was already removed from the system authorisation. Once
	 * we have warnings, we could issue a warning about this
	 * (seemingly) inconsistency between system and sql shadow
	 * administration. */

	return TRUE;
}

static str
monet5_create_user(ptr _mvc, str user, str passwd, char enc, str fullname, sqlid schema_id, sqlid grantorid)
{
	mvc *m = (mvc *) _mvc;
	oid uid = 0;
	bat bid = 0;
	str ret;
	int user_id;
	str pwd;
	sql_schema *s = find_sql_schema(m->session->tr, "sys");
	sql_table *db_user_info, *auths;
	Client c = MCgetClient(m->clientid);

	if (!enc) {
		pwd = mcrypt_BackendSum(passwd, strlen(passwd));
		if (pwd == NULL) {
			BBPunfix(bid);
			throw(MAL, "sql.create_user", SQLSTATE(42000) "Crypt backend hash not found");
		}
	} else {
		pwd = passwd;
	}
	/* add the user to the M5 authorisation administration */
	ret = AUTHaddUser(&uid, c, user, pwd);
	if (!enc)
		free(pwd);
	if (ret != MAL_SUCCEED)
		return ret;

	user_id = store_next_oid();
	db_user_info = find_sql_table(s, "db_user_info");
	auths = find_sql_table(s, "auths");
	table_funcs.table_insert(m->session->tr, db_user_info, user, fullname, &schema_id);
	table_funcs.table_insert(m->session->tr, auths, &user_id, user, &grantorid);
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
		_DELETE(err);
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
			throw(SQL, "sql.password", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
				throw(SQL, "sql.password", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
	char *err = NULL;
	int schema_id = 0;
	str monetdbuser = "monetdb";
	list *res, *ops;

	/* create the authorisation related tables */
	t = mvc_create_table(m, s, "db_user_info", tt_table, 1, SQL_PERSIST, 0, -1);
	mvc_create_column_(m, t, "name", "varchar", 1024);
	mvc_create_column_(m, t, "fullname", "varchar", 2048);
	mvc_create_column_(m, t, "default_schema", "int", 9);
	uinfo = t;

	(void) err;
	res = sa_list(m->sa);
	list_append(res, sql_create_arg(m->sa, "name", sql_bind_subtype(m->sa, "varchar", 2048, 0), ARG_OUT));  

	/* add function */
	ops = sa_list(m->sa);
	/* following funcion returns a table (single column) of user names
	   with the approriate scenario (sql) */
	mvc_create_func(m, NULL, s, "db_users", ops, res, F_UNION, FUNC_LANG_SQL, "sql", "db_users", "CREATE FUNCTION db_users () RETURNS TABLE( name varchar(2048)) EXTERNAL NAME sql.db_users;", FALSE, FALSE, TRUE);

	t = mvc_create_view(m, s, "users", SQL_PERSIST,
			    "SELECT u.\"name\" AS \"name\", "
			    "ui.\"fullname\", ui.\"default_schema\" "
			    "FROM db_users() AS u LEFT JOIN "
			    "\"sys\".\"db_user_info\" AS ui "
			    "ON u.\"name\" = ui.\"name\" "
			    ";", 1);
	mvc_create_column_(m, t, "name", "varchar", 1024);
	mvc_create_column_(m, t, "fullname", "varchar", 2024);
	mvc_create_column_(m, t, "default_schema", "int", 9);

	schema_id = sql_find_schema(m, "sys");
	assert(schema_id >= 0);

	table_funcs.table_insert(m->session->tr, uinfo, monetdbuser, "MonetDB Admin", &schema_id);
}

static int
monet5_schema_has_user(ptr _mvc, sql_schema *s)
{
	mvc *m = (mvc *) _mvc;
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *users = find_sql_table(sys, "db_user_info");
	sql_column *users_schema = find_sql_column(users, "default_schema");
	sqlid schema_id = s->base.id;

	rid = table_funcs.column_find_row(m->session->tr, users_schema, &schema_id, NULL);
	if (is_oid_nil(rid))
		return FALSE;
	return TRUE;
}

static int
monet5_alter_user(ptr _mvc, str user, str passwd, char enc, sqlid schema_id, str oldpasswd)
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

	if (schema_id) {
		oid rid;
		sql_schema *sys = find_sql_schema(m->session->tr, "sys");
		sql_table *info = find_sql_table(sys, "db_user_info");
		sql_column *users_name = find_sql_column(info, "name");
		sql_column *users_schema = find_sql_column(info, "default_schema");

		/* FIXME: we don't really check against the backend here */
		rid = table_funcs.column_find_row(m->session->tr, users_name, user, NULL);
		if (is_oid_nil(rid))
			return FALSE;

		table_funcs.column_update_value(m->session->tr, users_schema, rid, &schema_id);
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
	sql_table *info = find_sql_table(sys, "db_user_info");
	sql_column *users_name = find_sql_column(info, "name");
	sql_table *auths = find_sql_table(sys, "auths");
	sql_column *auths_name = find_sql_column(auths, "name");

	if ((err = AUTHchangeUsername(c, olduser, newuser)) !=MAL_SUCCEED) {
		(void) sql_error(m, 02, "ALTER USER: %s", getExceptionMessage(err));
		freeException(err);
		return (FALSE);
	}

	rid = table_funcs.column_find_row(m->session->tr, users_name, olduser, NULL);
	if (is_oid_nil(rid)) {
		(void) sql_error(m, 02, "ALTER USER: local inconsistency, "
				 "your database is damaged, user not found in SQL catalog");
		return (FALSE);
	}
	table_funcs.column_update_value(m->session->tr, users_name, rid, newuser);

	rid = table_funcs.column_find_row(m->session->tr, auths_name, olduser, NULL);
	if (is_oid_nil(rid)) {
		(void) sql_error(m, 02, "ALTER USER: local inconsistency, "
				 "your database is damaged, auth not found in SQL catalog");
		return (FALSE);
	}
	table_funcs.column_update_value(m->session->tr, auths_name, rid, newuser);

	return (TRUE);
}

static void *
monet5_schema_user_dependencies(ptr _trans, int schema_id)
{
	rids *A, *U;
	sql_trans *tr = (sql_trans *) _trans;
	sql_schema *s = find_sql_schema(tr, "sys");

	sql_table *auths = find_sql_table(s, "auths");
	sql_column *auth_name = find_sql_column(auths, "name");

	sql_table *users = find_sql_table(s, "db_user_info");
	sql_column *users_name = find_sql_column(users, "name");
	sql_column *users_sch = find_sql_column(users, "default_schema");

	/* select users with given schema */
	U = table_funcs.rids_select(tr, users_sch, &schema_id, &schema_id, NULL);
	/* select all authorization ids */
	A = table_funcs.rids_select(tr, auth_name, NULL, NULL);
	/* join all authorization with the selected users */
	A = table_funcs.rids_join(tr, A, auth_name, U, users_name);
	table_funcs.rids_destroy(U);
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
	sqlid schema_id;
	sql_schema *sys = NULL;
	sql_table *user_info = NULL;
	sql_column *users_name = NULL;
	sql_column *users_schema = NULL;
	sql_table *schemas = NULL;
	sql_column *schemas_name = NULL;
	sql_column *schemas_id = NULL;
	sql_table *auths = NULL;
	sql_column *auths_id = NULL;
	sql_column *auths_name = NULL;
	void *p = 0;
	str username = NULL;
	str schema = NULL;

	sys = find_sql_schema(m->session->tr, "sys");
	auths = find_sql_table(sys, "auths");
	auths_id = find_sql_column(auths, "id");
	auths_name = find_sql_column(auths, "name");
	rid = table_funcs.column_find_row(m->session->tr, auths_id, &user, NULL);
	if (!is_oid_nil(rid))
		username = table_funcs.column_find_value(m->session->tr, auths_name, rid);

	user_info = find_sql_table(sys, "db_user_info");
	users_name = find_sql_column(user_info, "name");
	users_schema = find_sql_column(user_info, "default_schema");
	rid = table_funcs.column_find_row(m->session->tr, users_name, username, NULL);
	if (!is_oid_nil(rid))
		p = table_funcs.column_find_value(m->session->tr, users_schema, rid);

	_DELETE(username);
	assert(p);
	schema_id = *(sqlid *) p;
	_DELETE(p);

	schemas = find_sql_table(sys, "schemas");
	schemas_name = find_sql_column(schemas, "name");
	schemas_id = find_sql_column(schemas, "id");

	rid = table_funcs.column_find_row(m->session->tr, schemas_id, &schema_id, NULL);
	if (!is_oid_nil(rid))
		schema = table_funcs.column_find_value(m->session->tr, schemas_name, rid);
	if(!stack_set_string(m, "current_schema", schema))
		return NULL;
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
	sql_table *schemas = NULL;
	sql_column *schemas_name = NULL;
	sql_column *schemas_id = NULL;
	sql_table *auths = NULL;
	sql_column *auths_name = NULL;

	void *p = 0;

	str schema = NULL;
	str username = NULL;
	str err = NULL;

	if (m->debug &1)
		fprintf(stderr, "monet5_user_set_def_schema " OIDFMT "\n", user);

	if ((err = AUTHresolveUser(&username, user)) !=MAL_SUCCEED) {
		freeException(err);
		return (NULL);	/* don't reveal that the user doesn't exist */
	}

	if(mvc_trans(m) < 0) {
		GDKfree(username);
		return NULL;
	}

	sys = find_sql_schema(m->session->tr, "sys");
	user_info = find_sql_table(sys, "db_user_info");
	users_name = find_sql_column(user_info, "name");
	users_schema = find_sql_column(user_info, "default_schema");

	rid = table_funcs.column_find_row(m->session->tr, users_name, username, NULL);
	if (!is_oid_nil(rid))
		p = table_funcs.column_find_value(m->session->tr, users_schema, rid);

	assert(p);
	schema_id = *(sqlid *) p;
	_DELETE(p);

	schemas = find_sql_table(sys, "schemas");
	schemas_name = find_sql_column(schemas, "name");
	schemas_id = find_sql_column(schemas, "id");
	auths = find_sql_table(sys, "auths");
	auths_name = find_sql_column(auths, "name");

	rid = table_funcs.column_find_row(m->session->tr, schemas_id, &schema_id, NULL);
	if (!is_oid_nil(rid))
		schema = table_funcs.column_find_value(m->session->tr, schemas_name, rid);

	/* only set schema if user is found */
	rid = table_funcs.column_find_row(m->session->tr, auths_name, username, NULL);
	if (!is_oid_nil(rid)) {
		sql_column *auths_id = find_sql_column(auths, "id");
		int id;
		p = table_funcs.column_find_value(m->session->tr, auths_id, rid);
		id = *(int *) p;
		_DELETE(p);

		m->user_id = m->role_id = id;
	} else {
		schema = NULL;
	}

	if (!schema || !mvc_set_schema(m, schema)) {
		if (m->session->active)
			mvc_rollback(m, 0, NULL);
		GDKfree(username);
		return NULL;
	}
	/* reset the user and schema names */
	if(!stack_set_string(m, "current_schema", schema) ||
		!stack_set_string(m, "current_user", username) ||
		!stack_set_string(m, "current_role", username)) {
		schema = NULL;
	}
	GDKfree(username);
	mvc_rollback(m, 0, NULL);
	return schema;
}
