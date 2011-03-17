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
#include "sql_statement.h"
#include "sql_semantic.h"
#include <sql_parser.h>

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
}

char *
sql_grant_table_privs( mvc *sql, char *grantee, int privs, char *tname, char *cname, int grant, int grantor)
{
	sql_schema *cur = cur_schema(sql);
	sql_table *t = mvc_bind_table(sql, cur, tname);
	sql_column *c = NULL;
	int allowed, grantee_id;
	int all = PRIV_SELECT | PRIV_UPDATE | PRIV_INSERT | PRIV_DELETE;

	if (!t) 
		return sql_message("GRANT no such table '%s'", tname);

	allowed = schema_privs(grantor, t->s);
	if (!allowed)
		allowed = sql_grantable(sql, grantor, t->base.id, all, 0);

	if (!allowed) 
		return sql_message("GRANTOR '%s' is not allowed to grant privileges for table '%s'", stack_get_string(sql,"current_user"), tname);

	if (cname) { 
		c = mvc_bind_column(sql, t, cname);
		if (!c) 
			return sql_message("GRANT: table %s has no column %s", tname, cname);
		/* allowed on column */
		if (!allowed)
			allowed = sql_grantable(sql, grantor, c->base.id, privs, 0);

		if (!allowed) 
			return sql_message("GRANTOR %s is not allowed to grant privilege %s for table %s", stack_get_string(sql, "current_user"), priv2string(privs), tname);
	}

	grantee_id = sql_find_auth(sql, grantee);
	if (grantee_id <= 0) 
		return sql_message("User/Role '%s' unknown", grantee);
	if (privs == all)
		sql_insert_all_privs(sql, grantee_id, t->base.id, grantor, grant);
	else if (!c)
		sql_insert_priv(sql, grantee_id, t->base.id, privs, grantor, grant);
	else
		sql_insert_priv(sql, grantee_id, c->base.id, privs, grantor, grant);
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
	for(rid = table_funcs.rids_next(A); rid != oid_nil; rid = table_funcs.rids_next(A)) 
		table_funcs.table_delete(tr, privs, rid); 
	table_funcs.rids_destroy(A);
}

char *
sql_revoke_table_privs( mvc *sql, char *grantee, int privs, char *tname, char *cname, int grant, int grantor)
{
	sql_schema *cur = cur_schema(sql);
	sql_table *t = mvc_bind_table(sql, cur, tname);
	sql_column *c = NULL;
	int allowed, grantee_id;
	int all = PRIV_SELECT | PRIV_UPDATE | PRIV_INSERT | PRIV_DELETE;

	if (!t) 
		return sql_message("REVOKE Table name %s doesn't exist", tname);

	allowed = schema_privs(grantor, t->s);
	if (!allowed)
		allowed = sql_grantable(sql, grantor, t->base.id, all, 0);

	if (!allowed) 
		return sql_message("GRANTOR '%s' is not allowed to revoke privileges for table '%s'", stack_get_string(sql,"current_user"), tname);

	if (cname) { 
		c = mvc_bind_column(sql, t, cname);
		if (!c) 
			return sql_message("REVOKE: table %s has no column %s", tname, cname);
		/* allowed on column */
		if (!allowed)
			allowed = sql_grantable(sql, grantor, c->base.id, privs, 0);

		if (!allowed) 
			return sql_message("GRANTOR %s is not allowed to revoke privilege %s for table %s", stack_get_string(sql, "current_user"), priv2string(privs), tname);
	}

	grantee_id = sql_find_auth(sql, grantee);
	if (grantee_id <= 0) 
		return sql_message("User/Role '%s' unknown", grantee);
	if (privs == all) {
		sql_delete_priv(sql, grantee_id, t->base.id, PRIV_SELECT, grantor, grant);
		sql_delete_priv(sql, grantee_id, t->base.id, PRIV_UPDATE, grantor, grant);
		sql_delete_priv(sql, grantee_id, t->base.id, PRIV_INSERT, grantor, grant);
		sql_delete_priv(sql, grantee_id, t->base.id, PRIV_DELETE, grantor, grant);
	} else if (!c)
		sql_insert_priv(sql, grantee_id, t->base.id, privs, grantor, grant);
	else
		sql_insert_priv(sql, grantee_id, c->base.id, privs, grantor, grant);
	return NULL;
}

int
sql_create_role_id(mvc *m, unsigned int id, str auth, int grantor)
{
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(sys, "auths");
	sql_column *auth_name = find_sql_column(auths, "name");

	if (table_funcs.column_find_row(m->session->tr, auth_name, auth, NULL) != oid_nil)
		return FALSE;

	table_funcs.table_insert(m->session->tr, auths, &id, auth, &grantor);
	return TRUE;
}

str
sql_create_role(mvc *m, str auth, int grantor)
{
	oid id;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(sys, "auths");
	sql_column *auth_name = find_sql_column(auths, "name");

	if (table_funcs.column_find_row(m->session->tr, auth_name, auth, NULL) != oid_nil)
		return sql_message("CREATE ROLE: Role '%s' allready exists\n", auth);

	id = store_next_oid();
	table_funcs.table_insert(m->session->tr, auths, &id, auth, &grantor);
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
	if (rid == oid_nil)
		return sql_message("DROP ROLE: Role '%s' does not exist\n", auth);
	table_funcs.table_delete(m->session->tr, auths, rid);
	return NULL;
}

char *
sql_grant_role(mvc *m, str grantee, str auth /*, grantor?, admin? */ )
{
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(sys, "auths");
	sql_table *roles = find_sql_table(sys, "user_role");
	sql_column *auths_name = find_sql_column(auths, "name");
	sql_column *auths_id = find_sql_column(auths, "id");

	void *auth_id, *grantee_id;

	rid = table_funcs.column_find_row(m->session->tr, auths_name, grantee, NULL);
	if (rid == oid_nil)
		return sql_message("GRANT: cannot grant ROLE '%s' to ROLE '%s'", grantee, auth );
	grantee_id = table_funcs.column_find_value(m->session->tr, auths_id, rid);

	rid = table_funcs.column_find_row(m->session->tr, auths_name, auth, NULL);
	if (rid == oid_nil) {
		_DELETE(grantee_id);
		return sql_message("GRANT: cannot grant ROLE '%s' to ROLE '%s'", grantee, auth );
	}
	auth_id = table_funcs.column_find_value(m->session->tr, auths_id, rid);

	table_funcs.table_insert(m->session->tr, roles, grantee_id, auth_id);
	_DELETE(grantee_id);
	_DELETE(auth_id);
	return NULL;
}

char *
sql_revoke_role(mvc *m, str grantee, str auth)
/* grantee no longer belongs the role (auth) */
{
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(sys, "auths");
	sql_table *roles = find_sql_table(sys, "user_role");
	sql_column *auths_name = find_sql_column(auths, "name");
	sql_column *auths_id = find_sql_column(auths, "id");
	sql_column *role_id = find_sql_column(roles, "role_id");
	sql_column *login_id = find_sql_column(roles, "login_id");

	void *auth_id, *grantee_id;

	rid = table_funcs.column_find_row(m->session->tr, auths_name, grantee, NULL);
	if (rid == oid_nil)
		return sql_message("REVOKE no such role '%s' or grantee '%s'", auth, grantee);
	grantee_id = table_funcs.column_find_value(m->session->tr, auths_id, rid);

	rid = table_funcs.column_find_row(m->session->tr, auths_name, auth, NULL);
	if (rid == oid_nil) {
		_DELETE(grantee_id);
		return sql_message("REVOKE no such role '%s' or grantee '%s'", auth, grantee);
	}
	auth_id = table_funcs.column_find_value(m->session->tr, auths_id, rid);

	rid = table_funcs.column_find_row(m->session->tr, login_id, grantee_id, role_id, auth_id, NULL);
	table_funcs.table_delete(m->session->tr, roles, rid);
	_DELETE(grantee_id);
	_DELETE(auth_id);
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

	if (rid != oid_nil) {
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

	if (rid != oid_nil) {
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

int
sql_privilege(mvc *m, int auth_id, int obj_id, int priv, int sub)
{
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *privs = find_sql_table(sys, "privileges");
	sql_column *priv_obj = find_sql_column(privs, "obj_id");
	sql_column *priv_auth = find_sql_column(privs, "auth_id");
	sql_column *priv_priv = find_sql_column(privs, "privileges");
	int res = 0;

	(void) sub;
	rid = table_funcs.column_find_row(m->session->tr, priv_obj, &obj_id, priv_auth, &auth_id, priv_priv, &priv, NULL);
	if (rid != oid_nil) {
		/* found priv */
		res = priv;
	}
	return res;
}

int
schema_privs(int grantor, sql_schema *s)
{
	if (grantor == USER_MONETDB || grantor == s->auth_id) {
		return 1;
	}
	return 0;
}

int
table_privs(mvc *m, sql_table *t, int priv)
{
	/* temporary tables are owned by the session user */
	if (t->persistence != SQL_PERSIST || t->commit_action)
		return 1;
	if (m->user_id == USER_MONETDB || m->role_id == t->s->auth_id || sql_privilege(m, m->user_id, t->base.id, priv, 0) == priv || sql_privilege(m, m->role_id, t->base.id, priv, 0) == priv || sql_privilege(m, ROLE_PUBLIC, t->base.id, priv, 0) == priv) {
		return 1;
	}
	return 0;
}

int
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
	for (priv = 1; priv < privs; priv <<= 1) {
		if (!(priv & privs))
			continue;
		rid = table_funcs.column_find_row(m->session->tr, priv_obj, &obj_id, priv_auth, &grantorid, priv_priv, &priv, NULL);
		if (rid != oid_nil) {
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
	if (m->user_id == USER_MONETDB)
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
	if (rid != oid_nil) {
		sql_table *roles = find_sql_table(sys, "user_role");
		sql_column *role_id = find_sql_column(roles, "role_id");
		sql_column *login_id = find_sql_column(roles, "login_id");

		sql_column *auths_id = find_sql_column(auths, "id");
		void *p = table_funcs.column_find_value(m->session->tr, auths_id, rid);
		int id = *(int *)p;

		_DELETE(p);
		rid = table_funcs.column_find_row(m->session->tr, login_id, &m->user_id, role_id, &id, NULL);
		
		if (rid != oid_nil) {
			m->role_id = id;
			res = 1;
		}
	}
	return res;
}

int
mvc_set_schema(mvc *m, char *schema)
{
	int ret = 0;
	sql_schema *s = find_sql_schema(m->session->tr, schema);

	if (s) {
		if (m->session->schema_name)
			_DELETE(m->session->schema_name);
		m->session->schema_name = _strdup(schema);
		m->type = Q_TRANS;
		if (m->session->active) 
			m->session->schema = s;
		ret = 1;
	}
	return ret;
}

char *
sql_create_user(mvc *sql, char *user, char *passwd, char enc, char *fullname, char *schema)
{
	char *err; 
	int schema_id = 0;

	if (backend_find_user(sql, user) >= 0) {
		return sql_message("CREATE USER: user '%s' already exists", user);
	}
	if ((schema_id = sql_find_schema(sql, schema)) < 0) {
		return sql_message("CREATE USER: no such schema '%s'", schema);
	}
	if ((err = backend_create_user(sql, user, passwd, enc, fullname,
					schema_id, sql->user_id)) != NULL)
	{
		char *r = sql_message("CREATE USER: %s", err);
		GDKfree(err);
		return r;
	}
	return NULL;
}

char *
sql_drop_user(mvc *sql, char *user)
{
	int user_id = sql_find_auth(sql, user);

	if (mvc_check_dependency(sql, user_id, OWNER_DEPENDENCY, NULL))
		return sql_message("DROP USER: '%s' owns a schema", user);
	if (backend_drop_user(sql,user) == FALSE)
		return sql_message("%s", sql->errstr);
	return sql_drop_role(sql, user);
}

char *
sql_alter_user(mvc *sql, char *user, char *passwd, char enc,
		char *schema, char *oldpasswd)
{
	sqlid schema_id = 0;
	/* USER == NULL -> current_user */
	if (user != NULL && backend_find_user(sql, user) < 0)
		return sql_message("ALTER USER: no such user '%s'", user);

	if (sql->user_id != USER_MONETDB && sql->role_id != ROLE_SYSADMIN && user != NULL && strcmp(user, stack_get_string(sql, "current_user")) != 0)
		return sql_message("ALTER USER: insufficient privileges to change user '%s'", user);
	if (schema && (schema_id = sql_find_schema(sql, schema)) < 0) {
		return sql_message("ALTER USER: no such schema '%s'", schema);
	}
	if (backend_alter_user(sql, user, passwd, enc, schema_id, oldpasswd) == FALSE)
		return sql_message("%s", sql->errstr);
	return NULL;
}

char *
sql_rename_user(mvc *sql, char *olduser, char *newuser)
{
	if (backend_find_user(sql, olduser) < 0)
		return sql_message("ALTER USER: no such user '%s'", olduser);
	if (backend_find_user(sql, newuser) >= 0)
		return sql_message("ALTER USER: user '%s' already exists", newuser);
	if (sql->user_id != USER_MONETDB && sql->role_id != ROLE_SYSADMIN)
		return sql_message("ALTER USER: insufficient privileges to "
				"rename user '%s'", olduser);

	if (backend_rename_user(sql, olduser, newuser) == FALSE)
		return sql_message("%s", sql->errstr);
	return NULL;

}

int
sql_create_privileges(mvc *m, sql_schema *s)
{
	int pub, p, zero = 0;
	sql_table *t, *privs;

	backend_create_privileges(m, s);

	t = mvc_create_table(m, s, "user_role", tt_table, 1, SQL_PERSIST, 0, -1);
	mvc_create_column_(m, t, "login_id", "int", 32);
	mvc_create_column_(m, t, "role_id", "int", 32);
	/*
	   mvc_create_column_(m, t, "grantor", "int", 32);
	   mvc_create_column_(m, t, "admin", "int", 32);
	 */

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

	/* add sysadmin roles */
	sql_create_role_id(m, ROLE_PUBLIC, "public", 0);
	sql_create_role_id(m, ROLE_SYSADMIN, "sysadmin", 0);
	sql_create_role_id(m, USER_MONETDB, "monetdb", 0);

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
	t = find_sql_table(s, "connections");
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
	t = find_sql_table(s, "user_role");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "auths");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);
	t = find_sql_table(s, "privileges");
	table_funcs.table_insert(m->session->tr, privs, &t->base.id, &pub, &p, &zero, &zero);

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
