/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
sql_insert_priv(mvc *sql, int auth_id, int obj_id, int privilege, int grantor, int grantable)
{
	sql_schema *ss = mvc_bind_schema(sql, "sys");
	sql_table *pt = mvc_bind_table(sql, ss, "privileges");

	table_funcs.table_insert(sql->session->tr, pt, &obj_id, &auth_id, &privilege, &grantor, &grantable);
}

static void
sql_insert_all_privs(mvc *sql, int auth_id, int obj_id, int grantor, int grantable)
{
	sql_insert_priv(sql, auth_id, obj_id, PRIV_SELECT, grantor, grantable);
	sql_insert_priv(sql, auth_id, obj_id, PRIV_UPDATE, grantor, grantable);
	sql_insert_priv(sql, auth_id, obj_id, PRIV_INSERT, grantor, grantable);
	sql_insert_priv(sql, auth_id, obj_id, PRIV_DELETE, grantor, grantable);
	sql_insert_priv(sql, auth_id, obj_id, PRIV_TRUNCATE, grantor, grantable);
}

static int
admin_privs(int grantor)
{
	if (grantor == USER_MONETDB || grantor == ROLE_SYSADMIN) {
		return 1;
	}
	return 0;
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

static int
schema_privs(int grantor, sql_schema *s)
{
	if (admin_privs(grantor)) 
		return 1;
	if (!s)
		return 0;
	if (grantor == s->auth_id) 
		return 1;
	return 0;
}


str
sql_grant_global_privs( mvc *sql, char *grantee, int privs, int grant, int grantor)
{
	sql_trans *tr = sql->session->tr;
	int allowed, grantee_id;

	allowed = admin_privs(grantor);

	if (!allowed)
		allowed = sql_grantable(sql, grantor, GLOBAL_OBJID, privs, 0);

	if (!allowed) 
		throw(SQL,"sql.grant_global",SQLSTATE(0L000) "Grantor '%s' is not allowed to grant global privileges", stack_get_string(sql,"current_user"));

	grantee_id = sql_find_auth(sql, grantee);
	if (grantee_id <= 0) 
		throw(SQL,"sql.grant_global",SQLSTATE(42M32) "User/role '%s' unknown", grantee);
	/* first check if privilege isn't already given */
	if ((sql_privilege(sql, grantee_id, GLOBAL_OBJID, privs, 0))) 
		throw(SQL,"sql.grant_global",SQLSTATE(42M32) "User/role '%s' already has this privilege", grantee);
	sql_insert_priv(sql, grantee_id, GLOBAL_OBJID, privs, grantor, grant);
	tr->schema_updates++;
	return MAL_SUCCEED;
}

char *
sql_grant_table_privs( mvc *sql, char *grantee, int privs, char *sname, char *tname, char *cname, int grant, int grantor)
{
	sql_trans *tr = sql->session->tr;
	sql_schema *s = NULL;
	sql_table *t = NULL;
	sql_column *c = NULL;
	int allowed, grantee_id;
	int all = PRIV_SELECT | PRIV_UPDATE | PRIV_INSERT | PRIV_DELETE | PRIV_TRUNCATE;

 	if (sname)
		s = mvc_bind_schema(sql, sname);
	if (s)
 		t = mvc_bind_table(sql, s, tname);
	if (!t) 
		throw(SQL,"sql.grant_table",SQLSTATE(42S02) "GRANT no such table '%s'", tname);

	allowed = schema_privs(grantor, t->s);

	if (!cname) {
		if (!allowed)
			allowed = sql_grantable(sql, grantor, t->base.id, privs, 0);

		if (!allowed) 
			throw(SQL,"sql.grant_table", SQLSTATE(0L000) "Grantor '%s' is not allowed to grant privileges for table '%s'", stack_get_string(sql,"current_user"), tname);
	}
	if (cname) { 
		c = mvc_bind_column(sql, t, cname);
		if (!c) 
			throw(SQL,"sql.grant_table",SQLSTATE(42S22) "Table %s has no column %s", tname, cname);
		/* allowed on column */
		if (!allowed)
			allowed = sql_grantable(sql, grantor, c->base.id, privs, 0);

		if (!allowed) 
			throw(SQL, "sql.grant_table", SQLSTATE(0L000) "Grantor %s is not allowed to grant privilege %s for table %s", stack_get_string(sql, "current_user"), priv2string(privs), tname);
	}

	grantee_id = sql_find_auth(sql, grantee);
	if (grantee_id <= 0) 
		throw(SQL,"sql.grant_table", SQLSTATE(42M32) "User/role '%s' unknown", grantee);
	/* first check if privilege isn't already given */
	if ((privs == all && 
	    (sql_privilege(sql, grantee_id, t->base.id, PRIV_SELECT, 0) ||
	     sql_privilege(sql, grantee_id, t->base.id, PRIV_UPDATE, 0) ||
	     sql_privilege(sql, grantee_id, t->base.id, PRIV_INSERT, 0) ||
	     sql_privilege(sql, grantee_id, t->base.id, PRIV_DELETE, 0) ||
	     sql_privilege(sql, grantee_id, t->base.id, PRIV_TRUNCATE, 0))) ||
	    (privs != all && !c && sql_privilege(sql, grantee_id, t->base.id, privs, 0)) || 
	    (privs != all && c && sql_privilege(sql, grantee_id, c->base.id, privs, 0))) {
		throw(SQL, "sql.grant", SQLSTATE(42M32) "User/role '%s' already has this privilege", grantee);
	}
	if (privs == all) {
		sql_insert_all_privs(sql, grantee_id, t->base.id, grantor, grant);
	} else if (!c) {
		sql_insert_priv(sql, grantee_id, t->base.id, privs, grantor, grant);
	} else {
		sql_insert_priv(sql, grantee_id, c->base.id, privs, grantor, grant);
	}
	tr->schema_updates++;
	return NULL;
}

char *
sql_grant_func_privs( mvc *sql, char *grantee, int privs, char *sname, int func_id, int grant, int grantor)
{
	sql_trans *tr = sql->session->tr;
	sql_schema *s = NULL;
	sql_func *f = NULL;
	int allowed, grantee_id;

 	if (sname)
		s = mvc_bind_schema(sql, sname);
	if (s) {
		node *n = find_sql_func_node(s, func_id);
		if (n)
			f = n->data;
	}
	assert(f);
	allowed = schema_privs(grantor, f->s);

	if (!allowed)
		allowed = sql_grantable(sql, grantor, f->base.id, privs, 0);

	if (!allowed) 
		throw(SQL, "sql.grant_func", SQLSTATE(0L000) "Grantor '%s' is not allowed to grant privileges for function '%s'", stack_get_string(sql,"current_user"), f->base.name);

	grantee_id = sql_find_auth(sql, grantee);
	if (grantee_id <= 0) 
		throw(SQL, "sql.grant_func", SQLSTATE(42M32) "User/role '%s' unknown", grantee);
	/* first check if privilege isn't already given */
	if (sql_privilege(sql, grantee_id, f->base.id, privs, 0)) 
		throw(SQL,"sql.grant", SQLSTATE(42M32) "User/role '%s' already has this privilege", grantee);
	sql_insert_priv(sql, grantee_id, f->base.id, privs, grantor, grant);
	tr->schema_updates++;
	return NULL;
}

static void
sql_delete_priv(mvc *sql, int auth_id, int obj_id, int privilege, int grantor, int grantable)
{
	sql_schema *ss = mvc_bind_schema(sql, "sys");
	sql_table *privs = mvc_bind_table(sql, ss, "privileges");
	sql_column *priv_obj = find_sql_column(privs, "obj_id");
	sql_column *priv_auth = find_sql_column(privs, "auth_id");
	sql_column *priv_priv = find_sql_column(privs, "privileges");
	sql_trans *tr = sql->session->tr;
	rids *A;
	oid rid = oid_nil;

	(void) grantor;
	(void) grantable;

	/* select privileges of this auth_id, privilege, obj_id */
	A = table_funcs.rids_select(tr, priv_auth, &auth_id, &auth_id, priv_priv, &privilege, &privilege, priv_obj, &obj_id, &obj_id, NULL );

	/* remove them */
	for(rid = table_funcs.rids_next(A); !is_oid_nil(rid); rid = table_funcs.rids_next(A)) 
		table_funcs.table_delete(tr, privs, rid); 
	table_funcs.rids_destroy(A);
}

char *
sql_revoke_global_privs( mvc *sql, char *grantee, int privs, int grant, int grantor)
{
	int allowed, grantee_id;

	allowed = admin_privs(grantor);

	if (!allowed)
		allowed = sql_grantable(sql, grantor, GLOBAL_OBJID, privs, 0);

	if (!allowed) 
		throw(SQL, "sql.revoke_global", SQLSTATE(0L000) "Grantor '%s' is not allowed to revoke global privileges", stack_get_string(sql,"current_user"));

	grantee_id = sql_find_auth(sql, grantee);
	if (grantee_id <= 0) 
		throw(SQL, "sql.revoke_global", SQLSTATE(42M32) "REVOKE: user/role '%s' unknown", grantee);
	sql_delete_priv(sql, grantee_id, GLOBAL_OBJID, privs, grantor, grant);
	sql->session->tr->schema_updates++;
	return NULL;
}

char *
sql_revoke_table_privs( mvc *sql, char *grantee, int privs, char *sname, char *tname, char *cname, int grant, int grantor)
{
	sql_schema *s = NULL;
	sql_table *t = NULL;
	sql_column *c = NULL;
	int allowed, grantee_id;
	int all = PRIV_SELECT | PRIV_UPDATE | PRIV_INSERT | PRIV_DELETE | PRIV_TRUNCATE;

 	if (sname)
		s = mvc_bind_schema(sql, sname);
	if (s)
 		t = mvc_bind_table(sql, s, tname);
	if (!t) 
		throw(SQL,"sql.revoke_table", SQLSTATE(42S02) "Revoke: no such table '%s'", tname);

	allowed = schema_privs(grantor, t->s);
	if (!allowed)
		allowed = sql_grantable(sql, grantor, t->base.id, privs, 0);

	if (!allowed) 
		throw(SQL, "sql.revoke_table", SQLSTATE(0L000) "Grantor '%s' is not allowed to revoke privileges for table '%s'", stack_get_string(sql,"current_user"), tname);

	if (cname) { 
		c = mvc_bind_column(sql, t, cname);
		if (!c) 
			throw(SQL,"sql.revoke_table", SQLSTATE(42S22) "REVOKE: table %s has no column %s", tname, cname);
		/* allowed on column */
		if (!allowed)
			allowed = sql_grantable(sql, grantor, c->base.id, privs, 0);

		if (!allowed) 
			throw(SQL, "sql.revoke_table", SQLSTATE(0L000) "Grantor %s is not allowed to revoke privilege %s for table %s", stack_get_string(sql, "current_user"), priv2string(privs), tname);
	}

	grantee_id = sql_find_auth(sql, grantee);
	if (grantee_id <= 0) 
		 throw(SQL,"sql.revoke_table", SQLSTATE(42M32) "REVOKE: user/role '%s' unknown", grantee);
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
	sql->session->tr->schema_updates++;
	return NULL;
}

char *
sql_revoke_func_privs( mvc *sql, char *grantee, int privs, char *sname, int func_id, int grant, int grantor)
{
	sql_schema *s = NULL;
	sql_func *f = NULL;
	int allowed, grantee_id;

 	if (sname)
		s = mvc_bind_schema(sql, sname);
	if (s) {
		node *n = find_sql_func_node(s, func_id);
		if (n)
			f = n->data;
	}
	assert(f);
	allowed = schema_privs(grantor, f->s);
	if (!allowed)
		allowed = sql_grantable(sql, grantor, f->base.id, privs, 0);

	if (!allowed) 
		throw(SQL, "sql.revoke_func", SQLSTATE(0L000) "Grantor '%s' is not allowed to revoke privileges for function '%s'", stack_get_string(sql,"current_user"), f->base.name);

	grantee_id = sql_find_auth(sql, grantee);
	if (grantee_id <= 0) 
		throw(SQL, "sql.revoke_func", SQLSTATE(42M32) "REVOKE: user/role '%s' unknown", grantee);
	sql_delete_priv(sql, grantee_id, f->base.id, privs, grantor, grant);
	sql->session->tr->schema_updates++;
	return NULL;
}

static int
sql_create_auth_id(mvc *m, unsigned int id, str auth)
{
	int grantor = 0; /* no grantor */
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(sys, "auths");
	sql_column *auth_name = find_sql_column(auths, "name");

	if (!is_oid_nil(table_funcs.column_find_row(m->session->tr, auth_name, auth, NULL)))
		return FALSE;

	table_funcs.table_insert(m->session->tr, auths, &id, auth, &grantor);
	m->session->tr->schema_updates++;
	return TRUE;
}

str
sql_create_role(mvc *m, str auth, int grantor)
{
	int id;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(sys, "auths");
	sql_column *auth_name = find_sql_column(auths, "name");

	if (!admin_privs(grantor)) 
		throw(SQL, "sql.create_role", SQLSTATE(0P000) "Insufficient privileges to create role '%s'", auth);

	if (!is_oid_nil(table_funcs.column_find_row(m->session->tr, auth_name, auth, NULL)))
		throw(SQL, "sql.create_role", SQLSTATE(0P000) "Role '%s' already exists", auth);

	id = store_next_oid();
	table_funcs.table_insert(m->session->tr, auths, &id, auth, &grantor);
	m->session->tr->schema_updates++;
	return NULL;
}

str
sql_drop_role(mvc *m, str auth)
{
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(sys, "auths");
	sql_column *auth_name = find_sql_column(auths, "name");

	rid = table_funcs.column_find_row(m->session->tr, auth_name, auth, NULL);
	if (is_oid_nil(rid))
		throw(SQL, "sql.drop_role", SQLSTATE(0P000) "DROP ROLE: no such role '%s'", auth);
	table_funcs.table_delete(m->session->tr, auths, rid);
	m->session->tr->schema_updates++;
	return NULL;
}

static oid
sql_privilege_rid(mvc *m, int auth_id, int obj_id, int priv, int sub)
{
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *privs = find_sql_table(sys, "privileges");
	sql_column *priv_obj = find_sql_column(privs, "obj_id");
	sql_column *priv_auth = find_sql_column(privs, "auth_id");
	sql_column *priv_priv = find_sql_column(privs, "privileges");

	(void) sub;
	return table_funcs.column_find_row(m->session->tr, priv_obj, &obj_id, priv_auth, &auth_id, priv_priv, &priv, NULL);
}

int
sql_privilege(mvc *m, int auth_id, int obj_id, int priv, int sub)
{
	oid rid = sql_privilege_rid(m, auth_id, obj_id, priv, sub);
	int res = 0;

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
	    sql_privilege(m, m->user_id, GLOBAL_OBJID, priv, 0) == priv || 
	    sql_privilege(m, m->role_id, GLOBAL_OBJID, priv, 0) == priv || 
	    sql_privilege(m, ROLE_PUBLIC, GLOBAL_OBJID, priv, 0) == priv) {
		return 1;
	}
	return 0;
}

int
table_privs(mvc *m, sql_table *t, int priv)
{
	/* temporary tables are owned by the session user */
	if (t->persistence == SQL_DECLARED_TABLE || (priv == PRIV_SELECT && (t->persistence != SQL_PERSIST || t->commit_action)))
		return 1;
	if (admin_privs(m->user_id) || admin_privs(m->role_id) || (t->s && (m->user_id == t->s->auth_id || m->role_id == t->s->auth_id)) || sql_privilege(m, m->user_id, t->base.id, priv, 0) == priv || sql_privilege(m, m->role_id, t->base.id, priv, 0) == priv || sql_privilege(m, ROLE_PUBLIC, t->base.id, priv, 0) == priv) {
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
	if (sql_privilege(m, m->user_id, f->base.id, priv, 0) == priv || 
	    sql_privilege(m, m->role_id, f->base.id, priv, 0) == priv || 
	    sql_privilege(m, ROLE_PUBLIC, f->base.id, priv, 0) == priv) 
		return 1;
	return 0;
}

static int
role_granting_privs(mvc *m, oid role_rid, int role_id, int grantor_id)
{
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(sys, "auths");
	sql_column *auths_grantor = find_sql_column(auths, "grantor");
	int owner_id;
	void *val;

	val = table_funcs.column_find_value(m->session->tr, auths_grantor, role_rid);
	owner_id = *(int*)val;
	_DELETE(val);

	if (owner_id == grantor_id)
		return 1;
	if (sql_privilege(m, grantor_id, role_id, PRIV_ROLE_ADMIN, 0))
		return 1;
	/* check for grant rights in the privs table */
	return 0;
}

char *
sql_grant_role(mvc *m, str grantee, str role, int grantor, int admin)
{
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(sys, "auths");
	sql_table *roles = find_sql_table(sys, "user_role");
	sql_column *auths_name = find_sql_column(auths, "name");
	sql_column *auths_id = find_sql_column(auths, "id");
	int role_id, grantee_id;
	void *val;

	rid = table_funcs.column_find_row(m->session->tr, auths_name, role, NULL);
	if (is_oid_nil(rid)) 
		throw(SQL,  "sql.grant_role", SQLSTATE(M1M05) "Cannot grant ROLE '%s' to ROLE '%s'", role, grantee);
	val = table_funcs.column_find_value(m->session->tr, auths_id, rid);
	role_id = *(int*)val; 
	_DELETE(val);

	if (backend_find_user(m, role) >= 0) 
		throw(SQL,"sql.grant_role", SQLSTATE(M1M05) "GRANT: '%s' is a USER not a ROLE", role);
	if (!admin_privs(grantor) && !role_granting_privs(m, rid, role_id, grantor)) 
		throw(SQL,"sql.grant_role", SQLSTATE(0P000) "Insufficient privileges to grant ROLE '%s'", role);
	rid = table_funcs.column_find_row(m->session->tr, auths_name, grantee, NULL);
	if (is_oid_nil(rid))
		throw(SQL,"sql.grant_role", SQLSTATE(M1M05) "Cannot grant ROLE '%s' to ROLE '%s'", role, grantee);
	val = table_funcs.column_find_value(m->session->tr, auths_id, rid);
	grantee_id = *(int*)val; 
	_DELETE(val);

	table_funcs.table_insert(m->session->tr, roles, &grantee_id, &role_id);
	if (admin) { 
		int priv = PRIV_ROLE_ADMIN, one = 1;
		sql_table *privs = find_sql_table(sys, "privileges");

		table_funcs.table_insert(m->session->tr, privs, &role_id, &grantee_id, &priv, &grantor, &one);
	}
	m->session->tr->schema_updates++;
	return NULL;
}

char *
sql_revoke_role(mvc *m, str grantee, str role, int grantor, int admin)
/* grantee no longer belongs the role (role) */
{
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(sys, "auths");
	sql_table *roles = find_sql_table(sys, "user_role");
	sql_column *auths_name = find_sql_column(auths, "name");
	sql_column *auths_id = find_sql_column(auths, "id");
	sql_column *roles_role_id = find_sql_column(roles, "role_id");
	sql_column *roles_login_id = find_sql_column(roles, "login_id");
	int role_id, grantee_id;
	void *val;

	rid = table_funcs.column_find_row(m->session->tr, auths_name, grantee, NULL);
	if (is_oid_nil(rid))
		throw(SQL,"sql.revoke_role", SQLSTATE(42M32) "REVOKE: no such role '%s' or grantee '%s'", role, grantee);
	val = table_funcs.column_find_value(m->session->tr, auths_id, rid);
	grantee_id = *(int*)val; 
	_DELETE(val);

	rid = table_funcs.column_find_row(m->session->tr, auths_name, role, NULL);
	if (is_oid_nil(rid)) 
		throw(SQL,"sql.revoke_role", SQLSTATE(42M32) "REVOKE: no such role '%s' or grantee '%s'", role, grantee);
	val = table_funcs.column_find_value(m->session->tr, auths_id, rid);
	role_id = *(int*)val; 
	_DELETE(val);
	if (!admin_privs(grantor) && !role_granting_privs(m, rid, role_id, grantor))
		throw(SQL,"sql.revoke_role", SQLSTATE(0P000) "REVOKE: insufficient privileges to revoke ROLE '%s'", role);

	if (!admin) { 
		rid = table_funcs.column_find_row(m->session->tr, roles_login_id, &grantee_id, roles_role_id, &role_id, NULL);
		if (!is_oid_nil(rid)) 
			table_funcs.table_delete(m->session->tr, roles, rid);
	} else {
		rid = sql_privilege_rid(m, grantee_id, role_id, PRIV_ROLE_ADMIN, 0);
		if (!is_oid_nil(rid)) 
			table_funcs.table_delete(m->session->tr, roles, rid);
	}
	m->session->tr->schema_updates++;
	return NULL;
}

int
sql_find_auth(mvc *m, str auth)
{
	int res = -1;
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(sys, "auths");
	sql_column *auths_name = find_sql_column(auths, "name");

	rid = table_funcs.column_find_row(m->session->tr, auths_name, auth, NULL);

	if (!is_oid_nil(rid)) {
		sql_column *auths_id = find_sql_column(auths, "id");
		int *p = (int *) table_funcs.column_find_value(m->session->tr, auths_id, rid);

		if (p) {
			res = *p;
			_DELETE(p);
		}
	}
	return res;
}

int
sql_find_schema(mvc *m, str schema)
{
	int schema_id = -1;
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *schemas = find_sql_table(sys, "schemas");
	sql_column *schemas_name = find_sql_column(schemas, "name");

	rid = table_funcs.column_find_row(m->session->tr, schemas_name, schema, NULL);

	if (!is_oid_nil(rid)) {
		sql_column *schemas_id = find_sql_column(schemas, "id");
		int *p = (int *) table_funcs.column_find_value(m->session->tr, schemas_id, rid);

		if (p) {
			schema_id = *p;
			_DELETE(p);
		}
	}
	return schema_id;
}

int
sql_schema_has_user(mvc *m, sql_schema *s)
{
	return(backend_schema_has_user(m, s));
}

static int
sql_grantable_(mvc *m, int grantorid, int obj_id, int privs, int sub)
{
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *prvs = find_sql_table(sys, "privileges");
	sql_column *priv_obj = find_sql_column(prvs, "obj_id");
	sql_column *priv_auth = find_sql_column(prvs, "auth_id");
	sql_column *priv_priv = find_sql_column(prvs, "privileges");
	sql_column *priv_allowed = find_sql_column(prvs, "grantable");
	int priv;

	(void) sub;
	for (priv = 1; priv <= privs; priv <<= 1) {
		if (!(priv & privs))
			continue;
		rid = table_funcs.column_find_row(m->session->tr, priv_obj, &obj_id, priv_auth, &grantorid, priv_priv, &priv, NULL);
		if (!is_oid_nil(rid)) {
			void *p = table_funcs.column_find_value(m->session->tr, priv_allowed, rid);
			int allowed = *(int *)p;

			_DELETE(p);
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
sql_grantable(mvc *m, int grantorid, int obj_id, int privs, int sub)
{
	if (admin_privs(m->user_id) || admin_privs(m->role_id))
		return 1;
	return sql_grantable_(m, grantorid, obj_id, privs, sub);
}

int
mvc_set_role(mvc *m, char *role)
{
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(sys, "auths");
	sql_column *auths_name = find_sql_column(auths, "name");
	int res = 0;

	if (m->debug&1)
		fprintf(stderr, "mvc_set_role %s\n", role);

	rid = table_funcs.column_find_row(m->session->tr, auths_name, role, NULL);
	if (!is_oid_nil(rid)) {
		sql_column *auths_id = find_sql_column(auths, "id");
		void *p = table_funcs.column_find_value(m->session->tr, auths_id, rid);
		int id = *(int *)p;

		_DELETE(p);

		if (m->user_id == id) {
			m->role_id = id;
			res = 1;
		} else {
			sql_table *roles = find_sql_table(sys, "user_role");
			sql_column *role_id = find_sql_column(roles, "role_id");
			sql_column *login_id = find_sql_column(roles, "login_id");

			rid = table_funcs.column_find_row(m->session->tr, login_id, &m->user_id, role_id, &id, NULL);
		
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
	char* new_schema_name = _STRDUP(schema);

	if (s && new_schema_name) {
		if (m->session->schema_name)
			_DELETE(m->session->schema_name);
		m->session->schema_name = new_schema_name;
		m->type = Q_TRANS;
		if (m->session->active)
			m->session->schema = s;
		ret = 1;
	} else if(new_schema_name) {
		_DELETE(new_schema_name);
	}
	return ret;
}

char *
sql_create_user(mvc *sql, char *user, char *passwd, char enc, char *fullname, char *schema)
{
	char *err; 
	int schema_id = 0;

	if (!admin_privs(sql->user_id) && !admin_privs(sql->role_id)) 
		throw(SQL,"sql.create_user", SQLSTATE(42M31) "Insufficient privileges to create user '%s'", user);

	if (backend_find_user(sql, user) >= 0) {
		throw(SQL,"sql.create_user", SQLSTATE(42M31) "CREATE USER: user '%s' already exists", user);
	}
	if ((schema_id = sql_find_schema(sql, schema)) < 0) {
		throw(SQL,"sql.create_user", SQLSTATE(3F000) "CREATE USER: no such schema '%s'", schema);
	}
	if ((err = backend_create_user(sql, user, passwd, enc, fullname,
					schema_id, sql->user_id)) != NULL)
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

char *
sql_drop_user(mvc *sql, char *user)
{
	int user_id = sql_find_auth(sql, user);

	if (mvc_check_dependency(sql, user_id, OWNER_DEPENDENCY, NULL))
		throw(SQL,"sql.drop_user",SQLSTATE(M1M05) "DROP USER: '%s' owns a schema", user);
	if (backend_drop_user(sql,user) == FALSE)
		throw(SQL,"sql.drop_user",SQLSTATE(M0M27) "%s", sql->errstr);
	return sql_drop_role(sql, user);
}

char *
sql_alter_user(mvc *sql, char *user, char *passwd, char enc,
		char *schema, char *oldpasswd)
{
	sqlid schema_id = 0;
	/* we may be called from MAL (nil) */
	if (user != NULL && strcmp(user, str_nil) == 0)
		user = NULL;
	/* USER == NULL -> current_user */
	if (user != NULL && backend_find_user(sql, user) < 0)
		throw(SQL,"sql.alter_user", SQLSTATE(42M32) "ALTER USER: no such user '%s'", user);

	if (!admin_privs(sql->user_id) && !admin_privs(sql->role_id) && user != NULL && strcmp(user, stack_get_string(sql, "current_user")) != 0)
		throw(SQL,"sql.alter_user", SQLSTATE(M1M05) "Insufficient privileges to change user '%s'", user);
	if (schema && (schema_id = sql_find_schema(sql, schema)) < 0) {
		throw(SQL,"sql.alter_user", SQLSTATE(3F000) "ALTER USER: no such schema '%s'", schema);
	}
	if (backend_alter_user(sql, user, passwd, enc, schema_id, oldpasswd) == FALSE)
		throw(SQL,"sql.alter_user", SQLSTATE(M0M27) "%s", sql->errstr);
	return NULL;
}

char *
sql_rename_user(mvc *sql, char *olduser, char *newuser)
{
	if (backend_find_user(sql, olduser) < 0)
		throw(SQL,"sql.rename_user", SQLSTATE(42M32) "ALTER USER: no such user '%s'", olduser);
	if (backend_find_user(sql, newuser) >= 0)
		throw(SQL,"sql.rename_user", SQLSTATE(42M31) "ALTER USER: user '%s' already exists", newuser);
	if (!admin_privs(sql->user_id) && !admin_privs(sql->role_id))
		throw(SQL,"sql.rename_user", SQLSTATE(M1M05) "ALTER USER: insufficient privileges to "
				"rename user '%s'", olduser);

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

	backend_create_privileges(m, s);

	t = mvc_create_table(m, s, "user_role", tt_table, 1, SQL_PERSIST, 0, -1);
	mvc_create_column_(m, t, "login_id", "int", 32);
	mvc_create_column_(m, t, "role_id", "int", 32);

	/* all roles and users are in the auths table */
	t = mvc_create_table(m, s, "auths", tt_table, 1, SQL_PERSIST, 0, -1);
	mvc_create_column_(m, t, "id", "int", 32);
	mvc_create_column_(m, t, "name", "varchar", 1024);
	mvc_create_column_(m, t, "grantor", "int", 32);

	t = mvc_create_table(m, s, "privileges", tt_table, 1, SQL_PERSIST, 0, -1);
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
	privs = find_sql_table(s, "privileges");

	t = find_sql_table(s, "schemas");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "types");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "functions");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "args");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "sequences");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "dependencies");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "_tables");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "_columns");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "keys");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "idxs");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "triggers");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "objects");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "tables");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "columns");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "comments");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "user_role");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "auths");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "privileges");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);

	p = PRIV_EXECUTE;
	f = sql_bind_func_(m->sa, s, "env", NULL, F_UNION);

	table_funcs.table_insert(m->session->tr, privs, &f->func->base.id, &pub, &p, &zero, &zero);

	/* owned by the users anyway 
	s = mvc_bind_schema(m, "tmp");
	t = find_sql_table(s, "profile");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "_tables");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "_columns");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "keys");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "idxs");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "triggers");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "objects");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	*/

	return 0;
}
