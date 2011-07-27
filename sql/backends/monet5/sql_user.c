/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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
#include "bat5.h"
#include "mal_authorize.h"

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

	if (rid != oid_nil) {
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
	mvc *m = (mvc *)_mvc;
	oid rid;
	sql_schema *sys;
	sql_table *users;
	sql_column *users_name;
	str err;
	Client c = MCgetClient(m->clientid);

	err = AUTHremoveUser(&c, &user);
	if (err != MAL_SUCCEED) {
		(void)sql_error(m, 02, "DROP USER: %s", err);
		_DELETE(err);
		return FALSE;
	}
	sys = find_sql_schema(m->session->tr, "sys");
	users = find_sql_table(sys, "db_user_info");
	users_name = find_sql_column(users, "name");

	rid = table_funcs.column_find_row(m->session->tr, users_name, user, NULL);
	if (rid != oid_nil)
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
	mvc *m = (mvc *)_mvc;
	oid uid = 0;
	bat bid = 0;
	BAT *scens;
	str ret;
	int user_id;
	str pwd;
	sql_schema *s = find_sql_schema(m->session->tr, "sys");
	sql_table *db_user_info, *auths;
	Client c = MCgetClient(m->clientid);

	/* prepare the scens BAT: it should contain the sql scenario */
	scens = BATnew(TYPE_str, TYPE_void, 1);
	if(scens == NULL)
		throw(SQL,"sql.create_user", MAL_MALLOC_FAIL);
	BUNins(scens, "sql", 0, FALSE);
	bid = BBPcacheid(scens);
	if (!enc) {
		int len = (int) strlen(passwd);
		if ((ret = AUTHBackendSum(&pwd, &passwd, &len)) != MAL_SUCCEED) {
			BBPunfix(bid);
			return ret;
		}
	} else {
		pwd = passwd;
	}
	/* add the user to the M5 authorisation administration */
	if ((ret = AUTHaddUser(&uid, &c, &user, &pwd, &bid)) != MAL_SUCCEED) {
		BBPunfix(bid);
		return ret;
	}
	BBPunfix(bid);
	if (!enc)
		GDKfree(pwd);

	user_id = store_next_oid();
	db_user_info = find_sql_table(s, "db_user_info");
	auths = find_sql_table(s, "auths");
	table_funcs.table_insert(m->session->tr, db_user_info, user, fullname, &schema_id);
	table_funcs.table_insert(m->session->tr, auths, &user_id, user, &grantorid);
	return NULL;
}

static BAT *
db_users(Client c)
{
	BAT *b;
	str tmp;

	/* prepare the scens BAT: it should contain the sql scenario */
	BAT *scens = BATnew(TYPE_str, TYPE_void, 1);
	if(scens == NULL)
		return NULL;

	BUNins(scens, "sql", 0, FALSE);
	if ((tmp = AUTHgetUsers(&b, &c, &scens->batCacheid)) != MAL_SUCCEED) {
		BBPunfix(scens->batCacheid);
		GDKfree(tmp);
		return(NULL);
	}
	BBPunfix(scens->batCacheid);
	return b;
}

static int
monet5_find_user(ptr mp, str user)
{
	BAT *users;
	BUN p;
	mvc *m = (mvc *)mp;
	Client c = MCgetClient(m->clientid);

	users = db_users(c);
	if (!users)
		return -1;
	p = BUNfnd(BATmirror(users), user);
	BBPunfix(users->batCacheid);

	/* yeah, I would prefer to return something different too */
	return(p == BUN_NONE ? -1 : 1);
}

str
db_users_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *r = (bat *)getArgReference(stk, pci, 0);
	BAT *b = db_users(cntxt);
	BAT *t = BATnew(TYPE_str, TYPE_bat, 1);
	if(t == NULL)
		throw(SQL,"sql.users_wrap", MAL_MALLOC_FAIL);
	(void)mb;

	BUNins(t, "name", &b->batCacheid, FALSE);
	BBPunfix(b->batCacheid);
	*r = t->batCacheid;
	BBPkeepref(*r);
	return MAL_SUCCEED;
}

str
db_password_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str ret = NULL;
	str *hash = (str *)getArgReference(stk, pci, 0);
	str *user = (str *)getArgReference(stk, pci, 1);
	(void)mb;

	ret = AUTHgetPasswordHash(hash, &cntxt, user);
	return ret;
}

static void
monet5_create_privileges(ptr _mvc, sql_schema *s)
{
	sql_table *t, *uinfo;
	mvc *m = (mvc *)_mvc;
	char *err = NULL;
	int schema_id = 0;
	str monetdbuser = "monetdb";
	sql_subtype tpe;
	list *l;

	/* create the authorisation related tables */
	t = mvc_create_table(m, s, "db_user_info", tt_table, 1, SQL_PERSIST, 0, -1);
	mvc_create_column_(m, t, "name", "varchar", 1024);
	mvc_create_column_(m, t, "fullname", "varchar", 2048);
	mvc_create_column_(m, t, "default_schema", "int", 9);
	uinfo = t;

	(void)err;
	t = mvc_create_generated(m, s, "#db_users", NULL, 1);
	mvc_create_column_(m, t, "name", "varchar", 2048);

	sql_find_subtype(&tpe, "table", 0, 0);
	tpe.comp_type = t;
	tpe.digits = t->base.id; /* pass the table through digits */

	/* add function */
	l = list_create((fdestroy) &arg_destroy);
	/* following funcion returns a table (single column) of user names
	   with the approriate scenario (sql) */
	mvc_create_func(m, s, "db_users", l, &tpe, FALSE, "sql", "db_users", "CREATE FUNCTION db_users () RETURNS TABLE( name varchar(2048)) EXTERNAL NAME sql.db_users;", 1);
	list_destroy(l);

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
	mvc *m = (mvc *)_mvc;
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *users = find_sql_table(sys, "db_user_info");
	sql_column *users_schema = find_sql_column(users, "default_schema");
	sqlid schema_id = s->base.id;

	rid = table_funcs.column_find_row(m->session->tr, users_schema, &schema_id, NULL);
	if (rid == oid_nil)
		return FALSE;
	return TRUE;
}

static int
monet5_alter_user(ptr _mvc, str user, str passwd, char enc,
		sqlid schema_id, str oldpasswd)
{
	mvc *m = (mvc *)_mvc;
	Client c = MCgetClient(m->clientid);
	str err;

	if (passwd != NULL) {
		str pwd = NULL;
		str opwd = NULL;
		if (!enc) {
			int len = (int) strlen(passwd);
			if ((err = AUTHBackendSum(&pwd, &passwd, &len)) != MAL_SUCCEED) {
				(void)sql_error(m, 02, "ALTER USER: %s", err);
				GDKfree(err);
				return FALSE;
			}
			if (oldpasswd != NULL) {
				len = (int)strlen(oldpasswd);
				if ((err = AUTHBackendSum(&opwd, &oldpasswd, &len))
						!= MAL_SUCCEED)
				{
					(void)sql_error(m, 02, "ALTER USER: %s", err);
					GDKfree(err);
					return FALSE;
				}
			}
		} else {
			pwd = passwd;
			opwd = oldpasswd;
		}
		if (user == NULL) {
			if ((err = AUTHchangePassword(&c, &opwd, &pwd)) != MAL_SUCCEED) {
				(void)sql_error(m, 02, "ALTER USER: %s", err);
				GDKfree(err);
				return(FALSE);
			}
		} else {
			str username = NULL;
			if ((err = AUTHresolveUser(&username, &c->user)) != MAL_SUCCEED) {
				(void)sql_error(m, 02, "ALTER USER: %s", err);
				GDKfree(err);
				return(FALSE);
			}
			if (strcmp(username, user) == 0) {
				/* avoid message about changePassword (from MAL level) */
				(void)sql_error(m, 02, "ALTER USER: "
						"use 'ALTER USER SET [ ENCRYPTED ] PASSWORD xxx "
						"USING OLD PASSWORD yyy' "
						"when changing your own password");
				return(FALSE);
			}
			if ((err = AUTHsetPassword(&c, &user, &pwd)) != MAL_SUCCEED) {
				(void)sql_error(m, 02, "ALTER USER: %s", err);
				GDKfree(err);
				return(FALSE);
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
		if (rid == oid_nil)
			return FALSE;

		table_funcs.column_update_value(m->session->tr, users_schema, rid, &schema_id);
	}

	return TRUE;
}

static int
monet5_rename_user(ptr _mvc, str olduser, str newuser)
{
	mvc *m = (mvc *)_mvc;
	Client c = MCgetClient(m->clientid);
	str err;
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *info = find_sql_table(sys, "db_user_info");
	sql_column *users_name = find_sql_column(info, "name");
	sql_table *auths = find_sql_table(sys, "auths");
	sql_column *auths_name = find_sql_column(auths, "name");

	if ((err = AUTHchangeUsername(&c, &olduser, &newuser)) != MAL_SUCCEED) {
		(void)sql_error(m, 02, "ALTER USER: %s", err);
		GDKfree(err);
		return(FALSE);
	}

	rid = table_funcs.column_find_row(m->session->tr, users_name,
			olduser, NULL);
	if (rid == oid_nil) {
		(void)sql_error(m, 02, "ALTER USER: local inconsistency, "
				"your database is damaged, user not found in SQL catalog");
		return(FALSE);
	}
	table_funcs.column_update_value(m->session->tr, users_name, rid,
			newuser);

	rid = table_funcs.column_find_row(m->session->tr, auths_name,
			olduser, NULL);
	if (rid == oid_nil) {
		(void)sql_error(m, 02, "ALTER USER: local inconsistency, "
				"your database is damaged, auth not found in SQL catalog");
		return(FALSE);
	}
	table_funcs.column_update_value(m->session->tr, auths_name, rid,
			newuser);

	return(TRUE);
}

static void*
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
	be_funcs->fcuser        = &monet5_create_user;
	be_funcs->fduser        = &monet5_drop_user;
	be_funcs->ffuser        = &monet5_find_user;
	be_funcs->fcrpriv       = &monet5_create_privileges;
	be_funcs->fshuser       = &monet5_schema_has_user;
	be_funcs->fauser        = &monet5_alter_user;
	be_funcs->fruser        = &monet5_rename_user;
	be_funcs->fschuserdep   = &monet5_schema_user_dependencies;
}

str
monet5_user_get_def_schema(mvc *m, oid user)
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

	void *p=0;

	str schema = NULL;
	str username = NULL;
	str err = NULL;

	if (m->debug&1)
		fprintf(stderr, "monet5_user_get_def_schema " OIDFMT "\n", user);

	if ((err = AUTHresolveUser(&username, &user)) != MAL_SUCCEED) {
		GDKfree(err);
		return(NULL);	/* don't reveal that the user doesn't exist */
	}

	mvc_trans(m);

	sys = find_sql_schema(m->session->tr, "sys");
	user_info = find_sql_table(sys, "db_user_info");
	users_name = find_sql_column(user_info, "name");
	users_schema = find_sql_column(user_info, "default_schema");

	if ((rid = table_funcs.column_find_row(m->session->tr, users_name, username, NULL)) != oid_nil)
		p = table_funcs.column_find_value(m->session->tr, users_schema, rid);

	assert(p);
	schema_id = *(sqlid*)p;
	_DELETE(p);

	schemas = find_sql_table(sys, "schemas");
	schemas_name = find_sql_column(schemas, "name");
	schemas_id = find_sql_column(schemas, "id");
	auths = find_sql_table(sys, "auths");
	auths_name = find_sql_column(auths, "name");

	if ((rid = table_funcs.column_find_row(m->session->tr, schemas_id, &schema_id, NULL)) != oid_nil)
		schema = table_funcs.column_find_value(m->session->tr, schemas_name, rid);

	/* only set schema if user is found */
	rid = table_funcs.column_find_row(m->session->tr, auths_name, username, NULL);
	if (rid != oid_nil) {
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
		return NULL;
	}
	/* reset the user and schema names */
	stack_set_string(m, "current_schema", schema);
	stack_set_string(m, "current_user", username);
	stack_set_string(m, "current_role", username);
	GDKfree(username);
	mvc_rollback(m, 0, NULL);
	return schema;
}


