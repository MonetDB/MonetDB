/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * Privileges
 * ==========
 *
 * Sql has a simple access control schema. There are two types of authorization,
 * users and roles. Each user may be part of several roles.
 * For each authorization identity a set of privileges is administrated.
 * These are administrated on multiple levels where lower levels (ie.
 * table or column level) overwrite privileges on higher levels.
 *
 */

#include "monetdb_config.h"
#include "sql_privileges.h"
#include "sql_semantic.h"
#include "sql_parser.h"
#include "mal_exception.h"

#define PRIV_ROLE_ADMIN 0

#define GLOBAL_OBJID 0

static const char *
priv2string(int priv)
{
	switch (priv) {
	case PRIV_SELECT:
		return "SELECT";
	case PRIV_UPDATE:
		return "UPDATE";
	case PRIV_INSERT:
		return "INSERT";
	case PRIV_DELETE:
		return "DELETE";
	case PRIV_TRUNCATE:
		return "TRUNCATE";
	case PRIV_EXECUTE:
		return "EXECUTE";
	}
	return "UNKNOWN PRIV";
}

static void
sql_insert_priv(mvc *sql, sqlid auth_id, sqlid obj_id, int privilege, sqlid grantor, int grantable)
{
	sql_schema *ss = mvc_bind_schema(sql, "sys");
	sql_table *pt = find_sql_table(sql->session->tr, ss, "privileges");
	sqlstore *store = sql->session->tr->store;

	store->table_api.table_insert(sql->session->tr, pt, &obj_id, &auth_id, &privilege, &grantor, &grantable);
}

static void
sql_insert_all_privs(mvc *sql, sqlid auth_id, sqlid obj_id, int grantor, int grantable)
{
	sql_insert_priv(sql, auth_id, obj_id, PRIV_SELECT, grantor, grantable);
	sql_insert_priv(sql, auth_id, obj_id, PRIV_UPDATE, grantor, grantable);
	sql_insert_priv(sql, auth_id, obj_id, PRIV_INSERT, grantor, grantable);
	sql_insert_priv(sql, auth_id, obj_id, PRIV_DELETE, grantor, grantable);
	sql_insert_priv(sql, auth_id, obj_id, PRIV_TRUNCATE, grantor, grantable);
}

static bool
admin_privs(sqlid grantor)
{
	if (grantor == USER_MONETDB || grantor == ROLE_SYSADMIN) {
		return true;
	}
	return false;
}

int
mvc_schema_privs(mvc *m, sql_schema *s)
{
	if (admin_privs(m->user_id) || admin_privs(m->role_id))
		return 1;
	if (!s)
		return 0;
	if (m->user_id == s->auth_id || m->role_id == s->auth_id)
		return 1;
	return 0;
}

static bool
schema_privs(sqlid grantor, sql_schema *s)
{
	if (admin_privs(grantor))
		return true;
	if (!s)
		return false;
	if (grantor == s->auth_id)
		return true;
	return false;
}

str
sql_grant_global_privs( mvc *sql, char *grantee, int privs, int grant, sqlid grantor)
{
	bool allowed;
	sqlid grantee_id;

	allowed = admin_privs(grantor);

	if (!allowed)
		allowed = sql_grantable(sql, grantor, GLOBAL_OBJID, privs) == 1;

	if (!allowed)
		throw(SQL,"sql.grant_global",SQLSTATE(01007) "GRANT: Grantor '%s' is not allowed to grant global privileges", get_string_global_var(sql, "current_user"));

	grantee_id = sql_find_auth(sql, grantee);
	if (grantee_id <= 0)
		throw(SQL,"sql.grant_global",SQLSTATE(01007) "GRANT: User/role '%s' unknown", grantee);
	/* first check if privilege isn't already given */
	if ((sql_privilege(sql, grantee_id, GLOBAL_OBJID, privs) >= 0))
		throw(SQL,"sql.grant_global",SQLSTATE(01007) "GRANT: User/role '%s' already has this privilege", grantee);
	sql_insert_priv(sql, grantee_id, GLOBAL_OBJID, privs, grantor, grant);
	return MAL_SUCCEED;
}

char *
sql_grant_table_privs( mvc *sql, char *grantee, int privs, char *sname, char *tname, char *cname, int grant, sqlid grantor)
{
	sql_table *t = NULL;
	sql_column *c = NULL;
	bool allowed;
	sqlid grantee_id;
	int all = PRIV_SELECT | PRIV_UPDATE | PRIV_INSERT | PRIV_DELETE | PRIV_TRUNCATE;

	if (!(t = find_table_or_view_on_scope(sql, NULL, sname, tname, "GRANT", false)))
		throw(SQL,"sql.grant_table", "%s", sql->errstr);
	if (isDeclaredTable(t))
		throw(SQL,"sql.grant_table", SQLSTATE(42000) "GRANT: cannot grant on a declared table");

	allowed = schema_privs(grantor, t->s);

	if (!cname) {
		if (!allowed)
			allowed = sql_grantable(sql, grantor, t->base.id, privs) == 1;

		if (!allowed)
			throw(SQL,"sql.grant_table", SQLSTATE(01007) "GRANT: Grantor '%s' is not allowed to grant privileges for table '%s'", get_string_global_var(sql, "current_user"), tname);
	}
	if (cname) {
		c = mvc_bind_column(sql, t, cname);
		if (!c)
			throw(SQL,"sql.grant_table",SQLSTATE(42S22) "GRANT: Table '%s' has no column '%s'", tname, cname);
		/* allowed on column */
		if (!allowed)
			allowed = sql_grantable(sql, grantor, c->base.id, privs) == 1;

		if (!allowed)
			throw(SQL, "sql.grant_table", SQLSTATE(01007) "GRANT: Grantor '%s' is not allowed to grant privilege %s for table '%s'", get_string_global_var(sql, "current_user"), priv2string(privs), tname);
	}

	grantee_id = sql_find_auth(sql, grantee);
	if (grantee_id <= 0)
		throw(SQL,"sql.grant_table", SQLSTATE(01007) "GRANT: User/role '%s' unknown", grantee);
	/* first check if privilege isn't already given */
	if ((privs == all &&
	    (sql_privilege(sql, grantee_id, t->base.id, PRIV_SELECT) >= 0 ||
	     sql_privilege(sql, grantee_id, t->base.id, PRIV_UPDATE) >= 0 ||
	     sql_privilege(sql, grantee_id, t->base.id, PRIV_INSERT) >= 0 ||
	     sql_privilege(sql, grantee_id, t->base.id, PRIV_DELETE) >= 0 ||
	     sql_privilege(sql, grantee_id, t->base.id, PRIV_TRUNCATE) >= 0)) ||
	    (privs != all && !c && sql_privilege(sql, grantee_id, t->base.id, privs) >= 0) ||
	    (privs != all && c && sql_privilege(sql, grantee_id, c->base.id, privs) >= 0)) {
		throw(SQL, "sql.grant", SQLSTATE(01007) "GRANT: User/role '%s' already has this privilege", grantee);
	}
	if (privs == all) {
		sql_insert_all_privs(sql, grantee_id, t->base.id, grantor, grant);
	} else if (!c) {
		sql_insert_priv(sql, grantee_id, t->base.id, privs, grantor, grant);
	} else {
		sql_insert_priv(sql, grantee_id, c->base.id, privs, grantor, grant);
	}
	return NULL;
}

char *
sql_grant_func_privs( mvc *sql, char *grantee, int privs, char *sname, sqlid func_id, int grant, sqlid grantor)
{
	sql_schema *s = NULL;
	bool allowed;
	sqlid grantee_id;

	assert(sname);
	if (!(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.grant_func",SQLSTATE(3F000) "GRANT: no such schema '%s'", sname);
	sql_base *b = os_find_id(s->funcs, sql->session->tr, func_id);
	sql_func *f = (sql_func*)b;
	assert(f);
	allowed = schema_privs(grantor, f->s);

	if (!allowed)
		allowed = sql_grantable(sql, grantor, f->base.id, privs) == 1;

	if (!allowed)
		throw(SQL, "sql.grant_func", SQLSTATE(01007) "GRANT: Grantor '%s' is not allowed to grant privileges for function '%s'", get_string_global_var(sql, "current_user"), f->base.name);

	grantee_id = sql_find_auth(sql, grantee);
	if (grantee_id <= 0)
		throw(SQL, "sql.grant_func", SQLSTATE(01007) "GRANT: User/role '%s' unknown", grantee);
	/* first check if privilege isn't already given */
	if (sql_privilege(sql, grantee_id, f->base.id, privs) >= 0)
		throw(SQL,"sql.grant", SQLSTATE(01007) "GRANT: User/role '%s' already has this privilege", grantee);
	sql_insert_priv(sql, grantee_id, f->base.id, privs, grantor, grant);
	return NULL;
}

static void
sql_delete_priv(mvc *sql, sqlid auth_id, sqlid obj_id, int privilege, sqlid grantor, int grantable)
{
	sql_schema *ss = mvc_bind_schema(sql, "sys");
	sql_table *privs = find_sql_table(sql->session->tr, ss, "privileges");
	sql_column *priv_obj = find_sql_column(privs, "obj_id");
	sql_column *priv_auth = find_sql_column(privs, "auth_id");
	sql_column *priv_priv = find_sql_column(privs, "privileges");
	sql_trans *tr = sql->session->tr;
	sqlstore *store = tr->store;
	rids *A;
	oid rid = oid_nil;

	(void) grantor;
	(void) grantable;

	/* select privileges of this auth_id, privilege, obj_id */
	A = store->table_api.rids_select(tr, priv_auth, &auth_id, &auth_id, priv_priv, &privilege, &privilege, priv_obj, &obj_id, &obj_id, NULL );

	/* remove them */
	for(rid = store->table_api.rids_next(A); !is_oid_nil(rid); rid = store->table_api.rids_next(A))
		store->table_api.table_delete(tr, privs, rid);
	store->table_api.rids_destroy(A);
}

char *
sql_revoke_global_privs( mvc *sql, char *grantee, int privs, int grant, sqlid grantor)
{
	bool allowed;
	sqlid grantee_id;

	allowed = admin_privs(grantor);

	if (!allowed)
		allowed = sql_grantable(sql, grantor, GLOBAL_OBJID, privs) == 1;

	if (!allowed)
		throw(SQL, "sql.revoke_global", SQLSTATE(01006) "REVOKE: Grantor '%s' is not allowed to revoke global privileges", get_string_global_var(sql, "current_user"));

	grantee_id = sql_find_auth(sql, grantee);
	if (grantee_id <= 0)
		throw(SQL, "sql.revoke_global", SQLSTATE(01006) "REVOKE: User/role '%s' unknown", grantee);
	sql_delete_priv(sql, grantee_id, GLOBAL_OBJID, privs, grantor, grant);
	return NULL;
}

char *
sql_revoke_table_privs( mvc *sql, char *grantee, int privs, char *sname, char *tname, char *cname, int grant, sqlid grantor)
{
	sql_table *t = NULL;
	sql_column *c = NULL;
	bool allowed;
	sqlid grantee_id;
	int all = PRIV_SELECT | PRIV_UPDATE | PRIV_INSERT | PRIV_DELETE | PRIV_TRUNCATE;

	if (!(t = find_table_or_view_on_scope(sql, NULL, sname, tname, "REVOKE", false)))
		throw(SQL,"sql.revoke_table","%s", sql->errstr);
	if (isDeclaredTable(t))
		throw(SQL,"sql.revoke_table", SQLSTATE(42000) "REVOKE: cannot revoke on a declared table");

	allowed = schema_privs(grantor, t->s);
	if (!allowed)
		allowed = sql_grantable(sql, grantor, t->base.id, privs) == 1;

	if (!allowed)
		throw(SQL, "sql.revoke_table", SQLSTATE(01006) "REVOKE: Grantor '%s' is not allowed to revoke privileges for table '%s'", get_string_global_var(sql, "current_user"), tname);

	if (cname) {
		c = mvc_bind_column(sql, t, cname);
		if (!c)
			throw(SQL,"sql.revoke_table", SQLSTATE(42S22) "REVOKE: table '%s' has no column '%s'", tname, cname);
		/* allowed on column */
		if (!allowed)
			allowed = sql_grantable(sql, grantor, c->base.id, privs) == 1;

		if (!allowed)
			throw(SQL, "sql.revoke_table", SQLSTATE(01006) "REVOKE: Grantor '%s' is not allowed to revoke privilege %s for table '%s'", get_string_global_var(sql, "current_user"), priv2string(privs), tname);
	}

	grantee_id = sql_find_auth(sql, grantee);
	if (grantee_id <= 0)
		 throw(SQL,"sql.revoke_table", SQLSTATE(01006) "REVOKE: User/role '%s' unknown", grantee);
	if (privs == all) {
		sql_delete_priv(sql, grantee_id, t->base.id, PRIV_SELECT, grantor, grant);
		sql_delete_priv(sql, grantee_id, t->base.id, PRIV_UPDATE, grantor, grant);
		sql_delete_priv(sql, grantee_id, t->base.id, PRIV_INSERT, grantor, grant);
		sql_delete_priv(sql, grantee_id, t->base.id, PRIV_DELETE, grantor, grant);
		sql_delete_priv(sql, grantee_id, t->base.id, PRIV_TRUNCATE, grantor, grant);
	} else if (!c) {
		sql_delete_priv(sql, grantee_id, t->base.id, privs, grantor, grant);
	} else {
		sql_delete_priv(sql, grantee_id, c->base.id, privs, grantor, grant);
	}
	return NULL;
}

char *
sql_revoke_func_privs( mvc *sql, char *grantee, int privs, char *sname, sqlid func_id, int grant, sqlid grantor)
{
	sql_schema *s = NULL;
	bool allowed;
	sqlid grantee_id;

	assert(sname);
	if (!(s = mvc_bind_schema(sql, sname)))
		throw(SQL,"sql.revoke_func", SQLSTATE(3F000) "REVOKE: no such schema '%s'", sname);
	sql_base *b = os_find_id(s->funcs, sql->session->tr, func_id);
	sql_func *f = (sql_func*)b;
	assert(f);
	allowed = schema_privs(grantor, f->s);
	if (!allowed)
		allowed = sql_grantable(sql, grantor, f->base.id, privs) == 1;

	if (!allowed)
		throw(SQL, "sql.revoke_func", SQLSTATE(01006) "REVOKE: Grantor '%s' is not allowed to revoke privileges for function '%s'", get_string_global_var(sql, "current_user"), f->base.name);

	grantee_id = sql_find_auth(sql, grantee);
	if (grantee_id <= 0)
		throw(SQL, "sql.revoke_func", SQLSTATE(01006) "REVOKE: User/role '%s' unknown", grantee);
	sql_delete_priv(sql, grantee_id, f->base.id, privs, grantor, grant);
	return NULL;
}

static bool
sql_create_auth_id(mvc *m, sqlid id, str auth)
{
	int grantor = 0; /* no grantor */
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(m->session->tr, sys, "auths");
	sql_column *auth_name = find_sql_column(auths, "name");
	sqlstore *store = m->session->tr->store;

	if (!is_oid_nil(store->table_api.column_find_row(m->session->tr, auth_name, auth, NULL)))
		return false;

	store->table_api.table_insert(m->session->tr, auths, &id, &auth, &grantor);
	return true;
}

str
sql_create_role(mvc *m, str auth, sqlid grantor)
{
	if (!admin_privs(grantor))
		throw(SQL, "sql.create_role", SQLSTATE(0P000) "Insufficient privileges to create role '%s'", auth);

	if (sql_trans_create_role(m->session->tr, auth, grantor) < 0)
		throw(SQL, "sql.create_role", SQLSTATE(0P000) "Role '%s' already exists", auth);
	return NULL;
}

str
sql_drop_role(mvc *m, str auth)
{
	sqlid role_id = sql_find_auth(m, auth);
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(m->session->tr, sys, "auths");
	sql_table *user_roles = find_sql_table(m->session->tr, sys, "user_role");
	sql_trans *tr = m->session->tr;
	sqlstore *store = m->session->tr->store;
	rids *A;
	oid rid;

	rid = store->table_api.column_find_row(tr, find_sql_column(auths, "name"), auth, NULL);
	if (is_oid_nil(rid))
		throw(SQL, "sql.drop_role", SQLSTATE(0P000) "DROP ROLE: no such role '%s'", auth);
	store->table_api.table_delete(m->session->tr, auths, rid);

	/* select user roles of this role_id */
	A = store->table_api.rids_select(tr, find_sql_column(user_roles, "role_id"), &role_id, &role_id, NULL);
	/* remove them */
	for(rid = store->table_api.rids_next(A); !is_oid_nil(rid); rid = store->table_api.rids_next(A))
		store->table_api.table_delete(tr, user_roles, rid);
	store->table_api.rids_destroy(A);
	return NULL;
}

static oid
sql_privilege_rid(mvc *m, sqlid auth_id, sqlid obj_id, int priv)
{
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *privs = find_sql_table(m->session->tr, sys, "privileges");
	sql_column *priv_obj = find_sql_column(privs, "obj_id");
	sql_column *priv_auth = find_sql_column(privs, "auth_id");
	sql_column *priv_priv = find_sql_column(privs, "privileges");
	sqlstore *store = m->session->tr->store;

	return store->table_api.column_find_row(m->session->tr, priv_obj, &obj_id, priv_auth, &auth_id, priv_priv, &priv, NULL);
}

int
sql_privilege(mvc *m, sqlid auth_id, sqlid obj_id, int priv)
{
	oid rid = sql_privilege_rid(m, auth_id, obj_id, priv);
	int res = -1;

	if (!is_oid_nil(rid)) {
		/* found priv */
		res = priv;
	}
	return res;
}

int
global_privs(mvc *m, int priv)
{
	if (admin_privs(m->user_id) || admin_privs(m->role_id) ||
	    sql_privilege(m, m->user_id, GLOBAL_OBJID, priv) == priv ||
	    sql_privilege(m, m->role_id, GLOBAL_OBJID, priv) == priv ||
	    sql_privilege(m, ROLE_PUBLIC, GLOBAL_OBJID, priv) == priv) {
		return 1;
	}
	return 0;
}

int
table_privs(mvc *m, sql_table *t, int priv)
{
	/* temporary tables are owned by the session user */
	if (t->persistence == SQL_DECLARED_TABLE ||
	    (!t->system && t->persistence != SQL_PERSIST) ||
	    (priv == PRIV_SELECT && (t->persistence != SQL_PERSIST || t->commit_action)))
		return 1;
	if (admin_privs(m->user_id) || admin_privs(m->role_id) ||
	    (t->s && (m->user_id == t->s->auth_id || m->role_id == t->s->auth_id)) ||
	    sql_privilege(m, m->user_id, t->base.id, priv) == priv ||
	    sql_privilege(m, m->role_id, t->base.id, priv) == priv ||
	    sql_privilege(m, ROLE_PUBLIC, t->base.id, priv) == priv) {
		return 1;
	}
	return 0;
}

int
column_privs(mvc *m, sql_column *c, int priv)
{
	/* only SELECT and UPDATE privileges for columns are available */
	/* temporary tables are owned by the session user, so does it's columns */
	if (c->t->persistence == SQL_DECLARED_TABLE ||
	    (!c->t->system && c->t->persistence != SQL_PERSIST) ||
	    (priv == PRIV_SELECT && (c->t->persistence != SQL_PERSIST || c->t->commit_action)))
		return 1;
	if (admin_privs(m->user_id) || admin_privs(m->role_id) ||
	    (c->t->s && (m->user_id == c->t->s->auth_id || m->role_id == c->t->s->auth_id)) ||
	    sql_privilege(m, m->user_id, c->base.id, priv) == priv ||
	    sql_privilege(m, m->role_id, c->base.id, priv) == priv ||
	    sql_privilege(m, ROLE_PUBLIC, c->base.id, priv) == priv) {
		return 1;
	}
	return 0;
}

int
execute_priv(mvc *m, sql_func *f)
{
	int priv = PRIV_EXECUTE;

	if (!f->s || admin_privs(m->user_id) || admin_privs(m->role_id))
		return 1;
	if (m->user_id == f->s->auth_id || m->role_id == f->s->auth_id)
		return 1;
	if (sql_privilege(m, m->user_id, f->base.id, priv) == priv ||
	    sql_privilege(m, m->role_id, f->base.id, priv) == priv ||
	    sql_privilege(m, ROLE_PUBLIC, f->base.id, priv) == priv)
		return 1;
	return 0;
}

static bool
role_granting_privs(mvc *m, oid role_rid, sqlid role_id, sqlid grantor_id)
{
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(m->session->tr, sys, "auths");
	sql_column *auths_grantor = find_sql_column(auths, "grantor");
	sqlid owner_id;
	sqlstore *store = m->session->tr->store;

	owner_id = store->table_api.column_find_sqlid(m->session->tr, auths_grantor, role_rid);
	if (owner_id == grantor_id)
		return true;
	if (sql_privilege(m, grantor_id, role_id, PRIV_ROLE_ADMIN) == PRIV_ROLE_ADMIN)
		return true;
	/* check for grant rights in the privs table */
	return false;
}

char *
sql_grant_role(mvc *m, str grantee, str role, sqlid grantor, int admin)
{
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(m->session->tr, sys, "auths");
	sql_table *roles = find_sql_table(m->session->tr, sys, "user_role");
	sql_column *auths_name = find_sql_column(auths, "name");
	sql_column *auths_id = find_sql_column(auths, "id");
	sqlid role_id, grantee_id;
	sqlstore *store = m->session->tr->store;

	rid = store->table_api.column_find_row(m->session->tr, auths_name, role, NULL);
	if (is_oid_nil(rid))
		throw(SQL, "sql.grant_role", SQLSTATE(M1M05) "GRANT: no such role '%s' or grantee '%s'", role, grantee);
	role_id = store->table_api.column_find_sqlid(m->session->tr, auths_id, rid);
	if (backend_find_user(m, role) >= 0)
		throw(SQL,"sql.grant_role", SQLSTATE(M1M05) "GRANT: '%s' is a USER not a ROLE", role);
	if (!admin_privs(grantor) && !role_granting_privs(m, rid, role_id, grantor))
		throw(SQL,"sql.grant_role", SQLSTATE(0P000) "GRANT: Insufficient privileges to grant ROLE '%s'", role);
	rid = store->table_api.column_find_row(m->session->tr, auths_name, grantee, NULL);
	if (is_oid_nil(rid))
		throw(SQL,"sql.grant_role", SQLSTATE(M1M05) "GRANT: no such role '%s' or grantee '%s'", role, grantee);
	grantee_id = store->table_api.column_find_sqlid(m->session->tr, auths_id, rid);
	rid = store->table_api.column_find_row(m->session->tr, find_sql_column(roles, "login_id"), &grantee_id, find_sql_column(roles, "role_id"), &role_id, NULL);
	if (!is_oid_nil(rid))
		throw(SQL,"sql.grant_role", SQLSTATE(M1M05) "GRANT: User '%s' already has ROLE '%s'", grantee, role);

	store->table_api.table_insert(m->session->tr, roles, &grantee_id, &role_id);
	if (admin) {
		int priv = PRIV_ROLE_ADMIN, one = 1;
		sql_table *privs = find_sql_table(m->session->tr, sys, "privileges");

		store->table_api.table_insert(m->session->tr, privs, &role_id, &grantee_id, &priv, &grantor, &one);
	}
	return NULL;
}

char *
sql_revoke_role(mvc *m, str grantee, str role, sqlid grantor, int admin)
/* grantee no longer belongs the role (role) */
{
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(m->session->tr, sys, "auths");
	sql_table *roles = find_sql_table(m->session->tr, sys, "user_role");
	sql_table *privs = find_sql_table(m->session->tr, sys, "privileges");
	sql_column *auths_name = find_sql_column(auths, "name");
	sql_column *auths_id = find_sql_column(auths, "id");
	sql_column *roles_role_id = find_sql_column(roles, "role_id");
	sql_column *roles_login_id = find_sql_column(roles, "login_id");
	sqlid role_id, grantee_id;
	sqlstore *store = m->session->tr->store;

	rid = store->table_api.column_find_row(m->session->tr, auths_name, grantee, NULL);
	if (is_oid_nil(rid))
		throw(SQL,"sql.revoke_role", SQLSTATE(01006) "REVOKE: no such role '%s' or grantee '%s'", role, grantee);
	grantee_id = store->table_api.column_find_sqlid(m->session->tr, auths_id, rid);
	rid = store->table_api.column_find_row(m->session->tr, auths_name, role, NULL);
	if (is_oid_nil(rid))
		throw(SQL,"sql.revoke_role", SQLSTATE(01006) "REVOKE: no such role '%s' or grantee '%s'", role, grantee);
	role_id = store->table_api.column_find_sqlid(m->session->tr, auths_id, rid);
	if (!admin_privs(grantor) && !role_granting_privs(m, rid, role_id, grantor))
		throw(SQL,"sql.revoke_role", SQLSTATE(0P000) "REVOKE: insufficient privileges to revoke ROLE '%s'", role);

	if (!admin) {
		rid = store->table_api.column_find_row(m->session->tr, roles_login_id, &grantee_id, roles_role_id, &role_id, NULL);
		if (!is_oid_nil(rid))
			store->table_api.table_delete(m->session->tr, roles, rid);
		else
			throw(SQL,"sql.revoke_role", SQLSTATE(01006) "REVOKE: User '%s' does not have ROLE '%s'", grantee, role);
	}
	rid = sql_privilege_rid(m, grantee_id, role_id, PRIV_ROLE_ADMIN);
	if (!is_oid_nil(rid))
		store->table_api.table_delete(m->session->tr, privs, rid);
	else if (admin)
		throw(SQL,"sql.revoke_role", SQLSTATE(01006) "REVOKE: User '%s' does not have ROLE '%s'", grantee, role);
	return NULL;
}

sqlid
sql_find_auth(mvc *m, str auth)
{
	sqlid res = -1;
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(m->session->tr, sys, "auths");
	sql_column *auths_name = find_sql_column(auths, "name");
	sqlstore *store = m->session->tr->store;

	rid = store->table_api.column_find_row(m->session->tr, auths_name, auth, NULL);

	if (!is_oid_nil(rid)) {
		sql_column *auths_id = find_sql_column(auths, "id");
		sqlid p = store->table_api.column_find_sqlid(m->session->tr, auths_id, rid);

		if (p > -1)
			res = p;
	}
	return res;
}

sqlid
sql_find_schema(mvc *m, str schema)
{
	sqlid schema_id = -1;
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *schemas = find_sql_table(m->session->tr, sys, "schemas");
	sql_column *schemas_name = find_sql_column(schemas, "name");
	sqlstore *store = m->session->tr->store;

	rid = store->table_api.column_find_row(m->session->tr, schemas_name, schema, NULL);

	if (!is_oid_nil(rid)) {
		sql_column *schemas_id = find_sql_column(schemas, "id");
		sqlid p = store->table_api.column_find_sqlid(m->session->tr, schemas_id, rid);

		if (p > -1)
			schema_id = p;
	}
	return schema_id;
}

int
sql_schema_has_user(mvc *m, sql_schema *s)
{
	return(backend_schema_has_user(m, s));
}

static int
sql_grantable_(mvc *m, sqlid grantorid, sqlid obj_id, int privs)
{
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *prvs = find_sql_table(m->session->tr, sys, "privileges");
	sql_column *priv_obj = find_sql_column(prvs, "obj_id");
	sql_column *priv_auth = find_sql_column(prvs, "auth_id");
	sql_column *priv_priv = find_sql_column(prvs, "privileges");
	sql_column *priv_allowed = find_sql_column(prvs, "grantable");
	sqlstore *store = m->session->tr->store;
	int priv;

	for (priv = 1; priv <= privs; priv <<= 1) {
		if (!(priv & privs))
			continue;
		rid = store->table_api.column_find_row(m->session->tr, priv_obj, &obj_id, priv_auth, &grantorid, priv_priv, &priv, NULL);
		if (!is_oid_nil(rid)) {
			int allowed = store->table_api.column_find_int(m->session->tr, priv_allowed, rid);

			/* switch of priv bit */
			if (allowed)
				privs = (privs & ~priv);
		}
	}
	if (privs != 0)
		return 0;
	return 1;
}

int
sql_grantable(mvc *m, sqlid grantorid, sqlid obj_id, int privs)
{
	if (admin_privs(m->user_id) || admin_privs(m->role_id))
		return 1;
	return sql_grantable_(m, grantorid, obj_id, privs);
}

sqlid
mvc_set_role(mvc *m, char *role)
{
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(m->session->tr, sys, "auths");
	sql_column *auths_name = find_sql_column(auths, "name");
	sqlid res = 0;
	sqlstore *store = m->session->tr->store;

	TRC_DEBUG(SQL_TRANS, "Set role: %s\n", role);

	rid = store->table_api.column_find_row(m->session->tr, auths_name, role, NULL);
	if (!is_oid_nil(rid)) {
		sql_column *auths_id = find_sql_column(auths, "id");
		sqlid id = store->table_api.column_find_sqlid(m->session->tr, auths_id, rid);

		if (m->user_id == id) {
			m->role_id = id;
			res = 1;
		} else {
			sql_table *roles = find_sql_table(m->session->tr, sys, "user_role");
			sql_column *role_id = find_sql_column(roles, "role_id");
			sql_column *login_id = find_sql_column(roles, "login_id");

			rid = store->table_api.column_find_row(m->session->tr, login_id, &m->user_id, role_id, &id, NULL);
			if (!is_oid_nil(rid)) {
				m->role_id = id;
				res = 1;
			}
		}
	}
	return res;
}

int
mvc_set_schema(mvc *m, char *schema)
{
	int ret = 0;
	sql_schema *s = find_sql_schema(m->session->tr, schema);
	char* new_schema_name = sa_strdup(m->session->sa, schema);

	if (s && new_schema_name) {
		m->session->schema_name = new_schema_name;
		m->type = Q_SCHEMA;
		if (m->session->tr->active)
			m->session->schema = s;
		ret = 1;
	}
	return ret;
}

char *
sql_create_user(mvc *sql, char *user, char *passwd, char enc, char *fullname, char *schema, char *schema_path)
{
	char *err;
	sqlid schema_id = 0;

	if (!admin_privs(sql->user_id) && !admin_privs(sql->role_id))
		throw(SQL,"sql.create_user", SQLSTATE(42M31) "Insufficient privileges to create user '%s'", user);

	if (backend_find_user(sql, user) >= 0)
		throw(SQL,"sql.create_user", SQLSTATE(42M31) "CREATE USER: user '%s' already exists", user);
	if ((schema_id = sql_find_schema(sql, schema)) < 0)
		throw(SQL,"sql.create_user", SQLSTATE(3F000) "CREATE USER: no such schema '%s'", schema);
	if ((err = backend_create_user(sql, user, passwd, enc, fullname, schema_id, schema_path, sql->user_id)) != NULL)
	{
		/* strip off MAL exception decorations */
		char *r;
		char *e = err;
		if ((e = strchr(e, ':')) == NULL) {
			e = err;
		} else if ((e = strchr(++e, ':')) == NULL) {
			e = err;
		} else {
			e++;
		}
		r = createException(SQL,"sql.create_user", SQLSTATE(M0M27) "CREATE USER: %s", e);
		_DELETE(err);
		return r;
	}
	return NULL;
}

static int
id_cmp(sqlid *id1, sqlid *id2)
{
	return *id1 == *id2;
}

static char *
sql_drop_granted_users(mvc *sql, sqlid user_id, char *user, list *deleted_users)
{
	sql_schema *ss = mvc_bind_schema(sql, "sys");
	sql_table *privs = find_sql_table(sql->session->tr, ss, "privileges");
	sql_table *user_roles = find_sql_table(sql->session->tr, ss, "user_role");
	sql_table *auths = find_sql_table(sql->session->tr, ss, "auths");
	sql_trans *tr = sql->session->tr;
	sqlstore *store = tr->store;
	rids *A;
	oid rid;

	if (!list_find(deleted_users, &user_id, (fcmp) &id_cmp)) {
		if (mvc_check_dependency(sql, user_id, OWNER_DEPENDENCY, NULL))
			throw(SQL,"sql.drop_user",SQLSTATE(M1M05) "DROP USER: '%s' owns a schema", user);
		if (backend_drop_user(sql, user) == FALSE)
			throw(SQL,"sql.drop_user",SQLSTATE(M0M27) "%s", sql->errstr);

		/* select privileges of this user_id */
		A = store->table_api.rids_select(tr, find_sql_column(privs, "auth_id"), &user_id, &user_id, NULL);
		/* remove them */
		for(rid = store->table_api.rids_next(A); !is_oid_nil(rid); rid = store->table_api.rids_next(A))
			store->table_api.table_delete(tr, privs, rid);
		store->table_api.rids_destroy(A);

		/* select privileges granted by this user_id */
		A = store->table_api.rids_select(tr, find_sql_column(privs, "grantor"), &user_id, &user_id, NULL);
		/* remove them */
		for(rid = store->table_api.rids_next(A); !is_oid_nil(rid); rid = store->table_api.rids_next(A))
			store->table_api.table_delete(tr, privs, rid);
		store->table_api.rids_destroy(A);

		/* delete entry from auths table */
		rid = store->table_api.column_find_row(tr, find_sql_column(auths, "name"), user, NULL);
		if (is_oid_nil(rid))
			throw(SQL, "sql.drop_user", SQLSTATE(0P000) "DROP USER: no such user role '%s'", user);
		store->table_api.table_delete(tr, auths, rid);

		/* select user roles of this user_id */
		A = store->table_api.rids_select(tr, find_sql_column(user_roles, "login_id"), &user_id, &user_id, NULL);
		/* remove them */
		for(rid = store->table_api.rids_next(A); !is_oid_nil(rid); rid = store->table_api.rids_next(A))
			store->table_api.table_delete(tr, user_roles, rid);
		store->table_api.rids_destroy(A);

		list_append(deleted_users, &user_id);

		/* select users created by this user_id */
		A = store->table_api.rids_select(tr, find_sql_column(auths, "grantor"), &user_id, &user_id, NULL);
		/* remove them and continue the deletion */
		for(rid = store->table_api.rids_next(A); !is_oid_nil(rid); rid = store->table_api.rids_next(A)) {
			sqlid nuid = store->table_api.column_find_sqlid(tr, find_sql_column(auths, "id"), rid);
			char* nname = store->table_api.column_find_value(tr, find_sql_column(auths, "name"), rid);

			sql_drop_granted_users(sql, nuid, nname, deleted_users);
			store->table_api.table_delete(tr, auths, rid);
		}
		store->table_api.rids_destroy(A);
	}
	return NULL;
}

char *
sql_drop_user(mvc *sql, char *user)
{
	sqlid user_id = sql_find_auth(sql, user);
	list *deleted = list_create(NULL);
	str msg = NULL;

	if (!deleted)
		throw(SQL, "sql.drop_user", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	msg = sql_drop_granted_users(sql, user_id, user, deleted);
	list_destroy(deleted);
	return msg;
}

char *
sql_alter_user(mvc *sql, char *user, char *passwd, char enc, char *schema, char *schema_path, char *oldpasswd)
{
	sqlid schema_id = 0;
	/* we may be called from MAL (nil) */
	if (strNil(user))
		user = NULL;
	/* USER == NULL -> current_user */

	if (!admin_privs(sql->user_id) && !admin_privs(sql->role_id) && user != NULL && strcmp(user, get_string_global_var(sql, "current_user")) != 0)
		throw(SQL,"sql.alter_user", SQLSTATE(M1M05) "Insufficient privileges to change user '%s'", user);

	if (user != NULL && backend_find_user(sql, user) < 0)
		throw(SQL,"sql.alter_user", SQLSTATE(42M32) "ALTER USER: no such user '%s'", user);
	if (schema && (schema_id = sql_find_schema(sql, schema)) < 0)
		throw(SQL,"sql.alter_user", SQLSTATE(3F000) "ALTER USER: no such schema '%s'", schema);
	if (backend_alter_user(sql, user, passwd, enc, schema_id, schema_path, oldpasswd) == FALSE)
		throw(SQL,"sql.alter_user", SQLSTATE(M0M27) "%s", sql->errstr);
	return NULL;
}

char *
sql_rename_user(mvc *sql, char *olduser, char *newuser)
{
	if (!admin_privs(sql->user_id) && !admin_privs(sql->role_id))
		throw(SQL,"sql.rename_user", SQLSTATE(M1M05) "ALTER USER: insufficient privileges to rename user '%s'", olduser);

	if (backend_find_user(sql, olduser) < 0)
		throw(SQL,"sql.rename_user", SQLSTATE(42M32) "ALTER USER: no such user '%s'", olduser);
	if (backend_find_user(sql, newuser) >= 0)
		throw(SQL,"sql.rename_user", SQLSTATE(42M31) "ALTER USER: user '%s' already exists", newuser);
	if (backend_rename_user(sql, olduser, newuser) == FALSE)
		throw(SQL,"sql.rename_user", SQLSTATE(M1M05) "%s", sql->errstr);
	return NULL;
}

int
sql_create_privileges(mvc *m, sql_schema *s)
{
	int pub, p, zero = 0;
	sql_table *t, *privs;
	sql_subfunc *f;
	sql_trans *tr = m->session->tr;

	backend_create_privileges(m, s);

	t = mvc_create_table(m, s, "user_role", tt_table, 1, SQL_PERSIST, 0, -1, 0);
	mvc_create_column_(m, t, "login_id", "int", 32);
	mvc_create_column_(m, t, "role_id", "int", 32);

	/* all roles and users are in the auths table */
	t = mvc_create_table(m, s, "auths", tt_table, 1, SQL_PERSIST, 0, -1, 0);
	mvc_create_column_(m, t, "id", "int", 32);
	mvc_create_column_(m, t, "name", "varchar", 1024);
	mvc_create_column_(m, t, "grantor", "int", 32);

	t = mvc_create_table(m, s, "privileges", tt_table, 1, SQL_PERSIST, 0, -1, 0);
	mvc_create_column_(m, t, "obj_id", "int", 32);
	mvc_create_column_(m, t, "auth_id", "int", 32);
	mvc_create_column_(m, t, "privileges", "int", 32);
	mvc_create_column_(m, t, "grantor", "int", 32);
	mvc_create_column_(m, t, "grantable", "int", 32);

	/* add roles public and sysadmin and user monetdb */
	sql_create_auth_id(m, ROLE_PUBLIC, "public");
	sql_create_auth_id(m, ROLE_SYSADMIN, "sysadmin");
	sql_create_auth_id(m, USER_MONETDB, "monetdb");

	pub = ROLE_PUBLIC;
	p = PRIV_SELECT;
	privs = find_sql_table(tr, s, "privileges");

	sqlstore *store = m->session->tr->store;
	t = find_sql_table(tr, s, "schemas");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "types");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "functions");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "args");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "sequences");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "dependencies");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "_tables");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "_columns");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "keys");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "idxs");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "triggers");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "objects");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "tables");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "columns");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "comments");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "user_role");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "auths");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "privileges");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "table_partitions");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "range_partitions");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "value_partitions");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);

	p = PRIV_EXECUTE;
	f = sql_bind_func_(m, s->base.name, "env", NULL, F_UNION);
	store->table_api.table_insert(m->session->tr, privs, &f->func->base.id, &pub, &p, &zero, &zero);
	f = sql_bind_func_(m, s->base.name, "var", NULL, F_UNION);
	store->table_api.table_insert(m->session->tr, privs, &f->func->base.id, &pub, &p, &zero, &zero);

	/* owned by the users anyway
	s = mvc_bind_schema(m, "tmp");
	t = find_sql_table(tr, s, "profile");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "_tables");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "_columns");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "keys");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "idxs");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "triggers");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(tr, s, "objects");
	store->table_api.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	*/

	return 0;
}
