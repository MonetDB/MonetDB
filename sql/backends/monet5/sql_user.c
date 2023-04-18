/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
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
#include "mal_interpreter.h"
#include "mal_authorize.h"
#include "mcrypt.h"


static inline sql_table*
getUsersTbl(mvc *m)
{
	sql_trans *tr = m->session->tr;
	sql_schema *sys = find_sql_schema(tr, "sys");
	return find_sql_table(tr, sys, USER_TABLE_NAME);
}


static oid
getUserOIDByName(mvc *m, const char *user)
{
	sql_trans *tr = m->session->tr;
	sqlstore *store = m->session->tr->store;
	sql_table *users = getUsersTbl(m);
	sql_column *users_name = find_sql_column(users, "name");
	return store->table_api.column_find_row(tr, users_name, user, NULL);
}


static str
getUserName(mvc *m, oid rid)
{
	if (is_oid_nil(rid))
		return NULL;
	sql_trans *tr = m->session->tr;
	sqlstore *store = m->session->tr->store;
	sql_table *users = getUsersTbl(m);
	return store->table_api.column_find_value(tr, find_sql_column(users, "name"), rid);
}


#if 0
static inline sql_table*
getSchemasTbl(mvc *m)
{
	sql_trans *tr = m->session->tr;
	sql_schema *sys = find_sql_schema(tr, "sys");
	return find_sql_table(tr, sys, SCHEMA_TABLE_NAME);
}

static str
getSchemaName(mvc *m, sqlid schema_id)
{
	if (schema_id > 0) {
		oid rid;
		sql_trans *tr = m->session->tr;
		sqlstore *store = m->session->tr->store;
		sql_table *tbl = getSchemasTbl(m);
		if (is_oid_nil(rid = store->table_api.column_find_row(tr, find_sql_column(tbl, "id"), &schema_id, NULL)))
			return NULL;
		return store->table_api.column_find_value(tr, find_sql_column(tbl, "name"), rid);
	}
	return NULL;
}
#endif

static str
getUserPassword(mvc *m, oid rid)
{
	if (is_oid_nil(rid)) {
		return NULL;
	}
	sql_trans *tr = m->session->tr;
	sqlstore *store = m->session->tr->store;
	sql_table *users = getUsersTbl(m);
	return store->table_api.column_find_value(tr, find_sql_column(users, USER_PASSWORD_COLUMN), rid);
}


static str
getUserNameCallback(Client c)
{
	str res = NULL;
	backend *be = (backend *) c->sqlcontext;
	if (be) {
		mvc *m = be->mvc;
		int active = m->session->tr->active;
		if (active || mvc_trans(m) == 0) {
			res = getUserName(m, c->user);
			if (!active)
				sql_trans_end(m->session, SQL_OK);
		}
	}
	return res;
}


static str
getUserPasswordCallback(Client c, const char *user)
{
	str res = NULL;
	backend *be = (backend *) c->sqlcontext;
	if (be) {
		mvc *m = be->mvc;
		int active = m->session->tr->active;
		// this starts new transaction
		if (active || mvc_trans(m) == 0) {
			oid rid = getUserOIDByName(m, user);
			res = getUserPassword(m, rid);
			if (!active)
				sql_trans_end(m->session, SQL_OK);
		}
	}
	return res;
}


static int
setUserPassword(mvc *m, oid rid, str value)
{
	str err = NULL;
	str hash = NULL;
	int res;
	if (is_oid_nil(rid)) {
		(void) sql_error(m, 02, SQLSTATE(42000) "setUserPassword: invalid user");
		return LOG_ERR;
	}
	if (strNil(value)) {
		(void) sql_error(m, 02, SQLSTATE(42000) "setUserPassword: password cannot be nil");
		return LOG_ERR;
	}
	if ((err = AUTHverifyPassword(value)) != MAL_SUCCEED) {
		(void) sql_error(m, 02, SQLSTATE(42000) "setUserPassword: %s", getExceptionMessage(err));
		freeException(err);
		return LOG_ERR;
	}
	if ((err = AUTHcypherValue(&hash, value)) != MAL_SUCCEED) {
		(void) sql_error(m, 02, SQLSTATE(42000) "setUserPassword: %s", getExceptionMessage(err));
		freeException(err);
		GDKfree(hash);
		return LOG_ERR;
	}

	sql_trans *tr = m->session->tr;
	sqlstore *store = m->session->tr->store;
	sql_table *users = getUsersTbl(m);
	res = store->table_api.column_update_value(tr, find_sql_column(users, USER_PASSWORD_COLUMN), rid, hash);
	GDKfree(hash);
	return res;
}


static int
changeUserPassword(mvc *m, oid rid, str oldpass, str newpass)
{
	str err = NULL;
	str hash = NULL;
	str passValue = NULL;
	if (is_oid_nil(rid)) {
		(void) sql_error(m, 02, SQLSTATE(42000) "changeUserPassword: invalid user");
		return LOG_ERR;
	}
	if (strNil(newpass)) {
		(void) sql_error(m, 02, SQLSTATE(42000) "changeUserPassword: password cannot be nil");
		return LOG_ERR;
	}
	if (oldpass) {
		// validate old password match
		if ((err = AUTHdecypherValue(&hash, passValue=getUserPassword(m, rid))) != MAL_SUCCEED) {
			(void) sql_error(m, 02, SQLSTATE(42000) "changeUserPassword: %s", getExceptionMessage(err));
			freeException(err);
			GDKfree(passValue);
			return LOG_ERR;
		}
		GDKfree(passValue);
		if (strcmp(oldpass, hash) != 0) {
			(void) sql_error(m, 02, SQLSTATE(42000) "changeUserPassword: password mismatch");
			GDKfree(hash);
			return LOG_ERR;
		}
		GDKfree(hash);
	}
	return setUserPassword(m, rid, newpass);
}


static oid
getUserOIDCallback(Client c, const char *user)
{
	oid res;
	backend *be = (backend *) c->sqlcontext;
	if (be) {
		mvc *m = be->mvc;
		int active = m->session->tr->active;
		if (active || mvc_trans(m) == 0) {
			res = getUserOIDByName(m, user);
			if (!active)
				sql_trans_end(m->session, SQL_OK);
			return res;
		}
	}
	return oid_nil;
}


static void
monet5_set_user_api_hooks(ptr mvc)
{
	(void) mvc;
	AUTHRegisterGetPasswordHandler(&getUserPasswordCallback);
	AUTHRegisterGetUserNameHandler(&getUserNameCallback);
	AUTHRegisterGetUserOIDHandler(&getUserOIDCallback);
}


static int
monet5_find_role(ptr _mvc, str role, sqlid *role_id)
{
	mvc *m = (mvc *) _mvc;
	sql_trans *tr = m->session->tr;
	sqlstore *store = m->session->tr->store;
	sql_schema *sys = find_sql_schema(tr, "sys");
	sql_table *auths = find_sql_table(tr, sys, "auths");
	sql_column *auth_name = find_sql_column(auths, "name");
	oid rid = store->table_api.column_find_row(tr, auth_name, role, NULL);
	if (is_oid_nil(rid))
		return -1;
	*role_id = store->table_api.column_find_sqlid(m->session->tr, find_sql_column(auths, "id"), rid);
	return 1;
}


static int
monet5_drop_user(ptr _mvc, str user)
{
	mvc *m = (mvc *) _mvc;
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *users = find_sql_table(m->session->tr, sys, "db_user_info");
	sql_column *users_name = find_sql_column(users, "name");
	sqlstore *store = m->session->tr->store;
	int log_res = LOG_OK;

	rid = store->table_api.column_find_row(m->session->tr, users_name, user, NULL);
	if (!is_oid_nil(rid) && (log_res = store->table_api.table_delete(m->session->tr, users, rid)) != LOG_OK) {
		(void) sql_error(m, 02, "DROP USER: failed%s", log_res == LOG_CONFLICT ? " due to conflict with another transaction" : "");
		return FALSE;
	}

	return TRUE;
}

#define outside_str 1
#define inside_str 2
#define default_schema_path "\"sys\"" /* "sys" will be the default schema path */
#define default_optimizer "default_pipe"
#define MAX_SCHEMA_SIZE 1024


static str
parse_schema_path_str(mvc *m, str schema_path, bool build) /* this function for both building and validating the schema path */
{
	list *l = m->schema_path;
	char next_schema[MAX_SCHEMA_SIZE]; /* needs one extra character for null terminator */
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
monet5_create_user(ptr _mvc, str user, str passwd, char enc, str fullname, sqlid schema_id, str schema_path, sqlid grantorid, lng max_memory, int max_workers, str optimizer, sqlid role_id)
{
	mvc *m = (mvc *) _mvc;
	oid rid;
	str ret, err, pwd, hash, schema_buf = NULL;
	sqlid user_id;
	sql_schema *s = find_sql_schema(m->session->tr, "sys");
	sql_table *db_user_info = find_sql_table(m->session->tr, s, "db_user_info"),
			  *auths = find_sql_table(m->session->tr, s, "auths"),
			  *schemas_tbl = find_sql_table(m->session->tr, s, "schemas");
	// Client c = MCgetClient(m->clientid);
	sqlstore *store = m->session->tr->store;
	int log_res = 0;
	bool new_schema = false;

	if (schema_id == 0) {
		// create default schema matching $user
		switch (sql_trans_create_schema(m->session->tr, user, m->role_id, m->user_id, &schema_id)) {
			case -1:
				throw(SQL,"sql.create_user",SQLSTATE(HY013) MAL_MALLOC_FAIL);
			case -2:
			case -3:
				throw(SQL,"sql.create_user",SQLSTATE(42000) "Create user schema failed due to transaction conflict");
			default:
				break;
		}
		new_schema = true;
	}
	assert(schema_id);

	if (is_oid_nil(rid = store->table_api.column_find_row(m->session->tr, find_sql_column(schemas_tbl, "id"), &schema_id, NULL)))
		throw(SQL,"sql.create_user",SQLSTATE(42000) "User schema not found");

	if (!schema_path) {
		// schema_name = store->table_api.column_find_value(m->session->tr, find_sql_column(schemas_tbl, "name"), rid);
		// if (schema_name) {
		// 	// "\"$schema_name\"\0"
		// 	if ((strlen(schema_name) + 4) > MAX_SCHEMA_SIZE) {
		// 		if (schema_name)
		// 			GDKfree(schema_name);
		// 		throw(SQL, "sql.schema_path", SQLSTATE(42000) "A schema has up to 1023 characters");
		// 	}
		// 	schema_buf = GDKmalloc(MAX_SCHEMA_SIZE);
		// 	snprintf(schema_buf, MAX_SCHEMA_SIZE, "\"%s\"", schema_name);
		// 	schema_path = schema_buf;
		// 	GDKfree(schema_name);
		// } else {
		// 	schema_path = default_schema_path;
		// }
		schema_path = default_schema_path;
	}

	if ((ret = parse_schema_path_str(m, schema_path, false)) != MAL_SUCCEED) {
		GDKfree(schema_buf);
		return ret;
	}

	if (!optimizer)
		optimizer = default_optimizer;

	if (!enc) {
		if (!(pwd = mcrypt_BackendSum(passwd, strlen(passwd)))) {
			GDKfree(schema_buf);
			throw(MAL, "sql.create_user", SQLSTATE(42000) "Crypt backend hash not found");
		}
	} else {
		pwd = passwd;
	}

	if ((err = AUTHGeneratePasswordHash(&hash, pwd)) != MAL_SUCCEED) {
		GDKfree(schema_buf);
		if (!enc)
			free(pwd);
		throw(MAL, "sql.create_user", SQLSTATE(42000) "create backend hash failure");
	}

	user_id = store_next_oid(m->session->tr->store);
	sqlid default_role_id = role_id > 0 ? role_id : user_id;
	if ((log_res = store->table_api.table_insert(m->session->tr, db_user_info, &user, &fullname, &schema_id, &schema_path, &max_memory, &max_workers, &optimizer, &default_role_id, &hash))) {
		if (!enc)
			free(pwd);
		GDKfree(schema_buf);
		GDKfree(hash);
		throw(SQL, "sql.create_user", SQLSTATE(42000) "Create user failed%s", log_res == LOG_CONFLICT ? " due to conflict with another transaction" : "");
	}
	// clean up
	GDKfree(schema_buf);
	GDKfree(hash);

	if ((log_res = store->table_api.table_insert(m->session->tr, auths, &user_id, &user, &grantorid))) {
		if (!enc)
			free(pwd);
		throw(SQL, "sql.create_user", SQLSTATE(42000) "Create user failed%s", log_res == LOG_CONFLICT ? " due to conflict with another transaction" : "");
	}

	if (new_schema) {
		// update schema authorization to be default_role_id
		switch (sql_trans_change_schema_authorization(m->session->tr, schema_id, default_role_id)) {
			case -1:
				if (!enc)
					free(pwd);
				throw(SQL,"sql.create_user",SQLSTATE(HY013) MAL_MALLOC_FAIL);
			case -2:
			case -3:
				if (!enc)
					free(pwd);
				throw(SQL,"sql.create_user",SQLSTATE(42000) "Update schema authorization failed due to transaction conflict");
			default:
				break;
		}

	}
	if (!enc)
		free(pwd);
	return ret;
}

static oid
monet5_find_user(ptr mp, str user)
{
	return getUserOIDByName((mvc *) mp, user);
}

str
monet5_password_hash(mvc *m, const char *username)
{
	str msg, hash = NULL;
	oid rid = getUserOIDByName(m, username);
	str password = getUserPassword(m, rid);
	if (password) {
		if ((msg = AUTHdecypherValue(&hash, password)) != MAL_SUCCEED) {
			(void) sql_error(m, 02, SQLSTATE(42000) "monet5_password_hash: %s", getExceptionMessage(msg));
			freeException(msg);
			GDKfree(password);
		}
	}
	GDKfree(password);
	return hash;
}

static void
monet5_create_privileges(ptr _mvc, sql_schema *s, const char *initpasswd)
{
	sql_schema *sys;
	sql_table *t = NULL;
	sql_table *uinfo = NULL;
	sql_column *col = NULL;
	mvc *m = (mvc *) _mvc;
	sqlid schema_id = 0;
	str err = NULL;

	/* create the authorisation related tables */
	mvc_create_table(&t, m, s, "db_user_info", tt_table, 1, SQL_PERSIST, 0, -1, 0);
	mvc_create_column_(&col, m, t, "name", "varchar", 1024);
	mvc_create_column_(&col, m, t, "fullname", "varchar", 2048);
	mvc_create_column_(&col, m, t, "default_schema", "int", 9);
	mvc_create_column_(&col, m, t, "schema_path", "clob", 0);
	mvc_create_column_(&col, m, t, "max_memory", "bigint", 64);
	mvc_create_column_(&col, m, t, "max_workers", "int", 32);
	mvc_create_column_(&col, m, t, "optimizer", "varchar", 1024);
	mvc_create_column_(&col, m, t, "default_role", "int", 32);
	mvc_create_column_(&col, m, t, "password", "varchar", 256);
	uinfo = t;

	sys = find_sql_schema(m->session->tr, "sys");
	schema_id = sys->base.id;
	assert(schema_id == 2000);

	sqlstore *store = m->session->tr->store;
	char *username = "monetdb";
	char *password = initpasswd ? mcrypt_BackendSum(initpasswd, strlen(initpasswd)) : mcrypt_BackendSum("monetdb", strlen("monetdb"));
	char *hash = NULL;
	if (password == NULL ||
		(err = AUTHGeneratePasswordHash(&hash, password)) != MAL_SUCCEED) {
		TRC_CRITICAL(SQL_TRANS, "generate password hash failure");
		freeException(err);
		free(password);
		return ;
	}
	free(password);

	char *fullname = "MonetDB Admin";
	char *schema_path = default_schema_path;
	// default values
	char *optimizer = default_optimizer;
	lng max_memory = 0;
	int max_workers = 0;
	sqlid default_role_id = USER_MONETDB;

	store->table_api.table_insert(m->session->tr, uinfo, &username, &fullname, &schema_id, &schema_path, &max_memory,
		&max_workers, &optimizer, &default_role_id, &hash);
	GDKfree(hash);
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
monet5_alter_user(ptr _mvc, str user, str passwd, char enc, sqlid schema_id, str schema_path, str oldpasswd, sqlid
		role_id)
{
	mvc *m = (mvc *) _mvc;
	Client c = MCgetClient(m->clientid);
	str err;
	int res = LOG_OK;
	oid rid = oid_nil;

	sqlstore *store = m->session->tr->store;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *info = find_sql_table(m->session->tr, sys, "db_user_info");
	sql_column *users_name = find_sql_column(info, "name");

	if (schema_id || schema_path || role_id) {
		rid = store->table_api.column_find_row(m->session->tr, users_name, user, NULL);
		// user should be checked here since the way `ALTER USER ident ...` stmt is
		if (is_oid_nil(rid)) {
			(void) sql_error(m, 02, "ALTER USER: local inconsistency, "
				 "your database is damaged, auth not found in SQL catalog");
			return FALSE;
		}
	}


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

		if (user) {
			// verify query user value is not the session user
			str username = NULL;
			if ((username = getUserName(m, c->user)) == NULL) {
				if (!enc) {
					free(pwd);
					free(opwd);
				}
				(void) sql_error(m, 02, "ALTER USER: invalid user");
				return (FALSE);
			}
			if (strcmp(username, user) == 0) {
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
			// verify current user is MAL_ADMIN ?
			if ((err = AUTHrequireAdmin(c)) != MAL_SUCCEED) {
				(void) sql_error(m, 02, "ALTER USER: %s", getExceptionMessage(err));
				freeException(err);
				if (!enc) {
					free(pwd);
					free(opwd);
				}
				return (FALSE);
			}
			if (setUserPassword(m, getUserOIDByName(m, user), pwd) != LOG_OK) {
				if (!enc) {
					free(pwd);
					free(opwd);
				}
				return (FALSE);
			}

		} else {
			if (changeUserPassword(m, c->user, opwd, pwd) != LOG_OK) {
				if (!enc) {
					free(pwd);
					free(opwd);
				}
				return (FALSE);
			}
		}
		if (!enc) {
			free(pwd);
			free(opwd);
		}
	}

	if (schema_id) {
		sql_column *users_schema = find_sql_column(info, "default_schema");

		if ((res = store->table_api.column_update_value(m->session->tr, users_schema, rid, &schema_id))) {
			(void) sql_error(m, 02, SQLSTATE(42000) "ALTER USER: failed%s",
							res == LOG_CONFLICT ? " due to conflict with another transaction" : "");
			return (FALSE);
		}
	}

	if (schema_path) {
		sql_column *sp = find_sql_column(info, "schema_path");

		if ((err = parse_schema_path_str(m, schema_path, false)) != MAL_SUCCEED) {
			(void) sql_error(m, 02, "ALTER USER: %s", getExceptionMessage(err));
			freeException(err);
			return (FALSE);
		}

		if ((res = store->table_api.column_update_value(m->session->tr, sp, rid, schema_path))) {
			(void) sql_error(m, 02, SQLSTATE(42000) "ALTER USER: failed%s",
							res == LOG_CONFLICT ? " due to conflict with another transaction" : "");
			return (FALSE);
		}
	}

	if (role_id) {
		sql_column *users_role = find_sql_column(info, "default_role");

		if ((res = store->table_api.column_update_value(m->session->tr, users_role, rid, &role_id))) {
			(void) sql_error(m, 02, SQLSTATE(42000) "ALTER USER: failed%s",
							res == LOG_CONFLICT ? " due to conflict with another transaction" : "");
			return (FALSE);
		}

	}

	return TRUE;
}

static int
monet5_rename_user(ptr _mvc, str olduser, str newuser)
{
	mvc *m = (mvc *) _mvc;
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *info = find_sql_table(m->session->tr, sys, "db_user_info");
	sql_column *users_name = find_sql_column(info, "name");
	sql_table *auths = find_sql_table(m->session->tr, sys, "auths");
	sql_column *auths_name = find_sql_column(auths, "name");
	int res = LOG_OK;

	sqlstore *store = m->session->tr->store;
	rid = store->table_api.column_find_row(m->session->tr, users_name, olduser, NULL);
	if (is_oid_nil(rid)) {
		(void) sql_error(m, 02, "ALTER USER: local inconsistency, "
				 "your database is damaged, user not found in SQL catalog");
		return (FALSE);
	}
	if ((res = store->table_api.column_update_value(m->session->tr, users_name, rid, newuser))) {
		(void) sql_error(m, 02, SQLSTATE(42000) "ALTER USER: failed%s",
						 res == LOG_CONFLICT ? " due to conflict with another transaction" : "");
		return (FALSE);
	}

	rid = store->table_api.column_find_row(m->session->tr, auths_name, olduser, NULL);
	if (is_oid_nil(rid)) {
		(void) sql_error(m, 02, "ALTER USER: local inconsistency, "
				 "your database is damaged, auth not found in SQL catalog");
		return (FALSE);
	}
	if ((res = store->table_api.column_update_value(m->session->tr, auths_name, rid, newuser))) {
		(void) sql_error(m, 02, SQLSTATE(42000) "ALTER USER: failed%s",
						 res == LOG_CONFLICT ? " due to conflict with another transaction" : "");
		return (FALSE);
	}
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
	if (A && U)
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
	be_funcs->ffrole = &monet5_find_role;
	be_funcs->fcrpriv = &monet5_create_privileges;
	be_funcs->fshuser = &monet5_schema_has_user;
	be_funcs->fauser = &monet5_alter_user;
	be_funcs->fruser = &monet5_rename_user;
	be_funcs->fschuserdep = &monet5_schema_user_dependencies;
	be_funcs->fset_user_api_hooks = &monet5_set_user_api_hooks;
}

int
monet5_user_get_def_schema(mvc *m, int user, str *schema)
{
	oid rid;
	sqlid schema_id = int_nil;
	sql_schema *sys = NULL;
	sql_table *user_info = NULL;
	sql_table *schemas = NULL;
	sql_table *auths = NULL;
	str username = NULL, sname = NULL;
	sqlstore *store = m->session->tr->store;

	sys = find_sql_schema(m->session->tr, "sys");
	auths = find_sql_table(m->session->tr, sys, "auths");
	user_info = find_sql_table(m->session->tr, sys, "db_user_info");
	schemas = find_sql_table(m->session->tr, sys, "schemas");

	rid = store->table_api.column_find_row(m->session->tr, find_sql_column(auths, "id"), &user, NULL);
	if (is_oid_nil(rid))
		return -2;
	if (!(username = store->table_api.column_find_value(m->session->tr, find_sql_column(auths, "name"), rid)))
		return -1;
	rid = store->table_api.column_find_row(m->session->tr, find_sql_column(user_info, "name"), username, NULL);
	_DELETE(username);

	if (!is_oid_nil(rid))
		schema_id = store->table_api.column_find_sqlid(m->session->tr, find_sql_column(user_info, "default_schema"), rid);
	if (is_int_nil(schema_id))
		return -3;
	rid = store->table_api.column_find_row(m->session->tr, find_sql_column(schemas, "id"), &schema_id, NULL);
	if (is_oid_nil(rid))
		return -3;

	if (!(sname = store->table_api.column_find_value(m->session->tr, find_sql_column(schemas, "name"), rid)))
		return -1;
	*schema = sa_strdup(m->session->sa, sname);
	_DELETE(sname);
	return *schema ? 0 : -1;
}

int
monet5_user_set_def_schema(mvc *m, oid user)
{
	oid rid;
	sqlid schema_id, default_role_id;
	sql_schema *sys = NULL;
	sql_table *user_info = NULL;
	sql_column *users_name = NULL;
	sql_column *users_schema = NULL;
	sql_column *users_schema_path = NULL;
	sql_column *users_default_role = NULL;
	sql_table *schemas = NULL;
	sql_column *schemas_name = NULL;
	sql_column *schemas_id = NULL;
	sql_table *auths = NULL;
	sql_column *auths_id = NULL;
	sql_column *auths_name = NULL;
	str path_err = NULL, other = NULL, schema = NULL, schema_cpy, schema_path = NULL, username = NULL, userrole = NULL;
	int ok = 1, res = 0;

	TRC_DEBUG(SQL_TRANS, OIDFMT "\n", user);

	if ((res = mvc_trans(m)) < 0) {
		// we have -1 here
		return res;
	}

	if ((username = getUserName(m, user)) == NULL) {
		return -1;
	}

	sys = find_sql_schema(m->session->tr, "sys");
	user_info = find_sql_table(m->session->tr, sys, "db_user_info");
	users_name = find_sql_column(user_info, "name");
	users_schema = find_sql_column(user_info, "default_schema");
	users_schema_path = find_sql_column(user_info, "schema_path");
	users_default_role = find_sql_column(user_info, "default_role");

	sqlstore *store = m->session->tr->store;
	rid = store->table_api.column_find_row(m->session->tr, users_name, username, NULL);
	if (is_oid_nil(rid)) {
		if (m->session->tr->active && (other = mvc_rollback(m, 0, NULL, false)) != MAL_SUCCEED)
			freeException(other);
		GDKfree(username);
		return -2;
	}
	schema_id = store->table_api.column_find_sqlid(m->session->tr, users_schema, rid);
	if (!(schema_path = store->table_api.column_find_value(m->session->tr, users_schema_path, rid))) {
		if (m->session->tr->active && (other = mvc_rollback(m, 0, NULL, false)) != MAL_SUCCEED)
			freeException(other);
		GDKfree(username);
		return -1;
	}

	default_role_id = store->table_api.column_find_sqlid(m->session->tr, users_default_role, rid);

	schemas = find_sql_table(m->session->tr, sys, "schemas");
	schemas_name = find_sql_column(schemas, "name");
	schemas_id = find_sql_column(schemas, "id");
	auths = find_sql_table(m->session->tr, sys, "auths");
	auths_id = find_sql_column(auths, "id");
	auths_name = find_sql_column(auths, "name");

	rid = store->table_api.column_find_row(m->session->tr, schemas_id, &schema_id, NULL);
	if (is_oid_nil(rid)) {
		if (m->session->tr->active && (other = mvc_rollback(m, 0, NULL, false)) != MAL_SUCCEED)
			freeException(other);
		GDKfree(username);
		_DELETE(schema_path);
		return -3;
	}
	if (!(schema = store->table_api.column_find_value(m->session->tr, schemas_name, rid))) {
		if (m->session->tr->active && (other = mvc_rollback(m, 0, NULL, false)) != MAL_SUCCEED)
			freeException(other);
		GDKfree(username);
		_DELETE(schema_path);
		return -1;
	}
	schema_cpy = schema;
	schema = sa_strdup(m->session->sa, schema);
	_DELETE(schema_cpy);

	/* check if username exists */
	rid = store->table_api.column_find_row(m->session->tr, auths_name, username, NULL);
	if (is_oid_nil(rid)) {
		if (m->session->tr->active && (other = mvc_rollback(m, 0, NULL, false)) != MAL_SUCCEED)
			freeException(other);
		GDKfree(username);
		_DELETE(schema_path);
		return -2;
	}

	m->user_id = store->table_api.column_find_sqlid(m->session->tr, auths_id, rid);

	/* check if role exists */
	rid = store->table_api.column_find_row(m->session->tr, auths_id, &default_role_id, NULL);
	if (is_oid_nil(rid)) {
		if (m->session->tr->active && (other = mvc_rollback(m, 0, NULL, false)) != MAL_SUCCEED)
			freeException(other);
		GDKfree(username);
		_DELETE(schema_path);
		return -4;
	}
	m->role_id = default_role_id;
	if (!(userrole = store->table_api.column_find_value(m->session->tr, auths_name, rid))) {
		if (m->session->tr->active && (other = mvc_rollback(m, 0, NULL, false)) != MAL_SUCCEED)
			freeException(other);
		GDKfree(username);
		_DELETE(schema_path);
		return -1;
	}

	/* while getting the session's schema, set the search path as well */
	if (!(ok = mvc_set_schema(m, schema)) || (path_err = parse_schema_path_str(m, schema_path, true)) != MAL_SUCCEED) {
		if (m->session->tr->active && (other = mvc_rollback(m, 0, NULL, false)) != MAL_SUCCEED)
			freeException(other);
		GDKfree(username);
		_DELETE(schema_path);
		_DELETE(userrole);
		freeException(path_err);
		return ok == 0 ? -3 : -1;
	}


	/* reset the user and schema names */
	if (!sqlvar_set_string(find_global_var(m, sys, "current_schema"), schema) ||
		!sqlvar_set_string(find_global_var(m, sys, "current_user"), username) ||
		!sqlvar_set_string(find_global_var(m, sys, "current_role"), userrole)) {
		res = -1;
	}
	GDKfree(username);
	_DELETE(schema_path);
	_DELETE(userrole);
	if ((other = mvc_rollback(m, 0, NULL, false)) != MAL_SUCCEED) {
		freeException(other);
		return -1;
	}
	return res;
}
