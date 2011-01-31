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
#include <sql_parser.h>

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

int
sql_create_role(mvc *m, str auth, int grantor)
{
	oid id;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(sys, "auths");
	sql_column *auth_name = find_sql_column(auths, "name");

	if (table_funcs.column_find_row(m->session->tr, auth_name, auth, NULL) != oid_nil)
		return FALSE;

	id = store_next_oid();
	table_funcs.table_insert(m->session->tr, auths, &id, auth, &grantor);

	return TRUE;
}

int
sql_drop_role(mvc *m, str auth)
{
	oid rid;
	sql_schema *sys = find_sql_schema(m->session->tr, "sys");
	sql_table *auths = find_sql_table(sys, "auths");
	sql_column *auth_name = find_sql_column(auths, "name");

	rid = table_funcs.column_find_row(m->session->tr, auth_name, auth, NULL);
	if (rid != oid_nil)
		table_funcs.table_delete(m->session->tr, auths, rid);
	return TRUE;
}

int
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
		return FALSE;
	grantee_id = table_funcs.column_find_value(m->session->tr, auths_id, rid);

	rid = table_funcs.column_find_row(m->session->tr, auths_name, auth, NULL);
	if (rid == oid_nil) {
		_DELETE(grantee_id);
		return FALSE;
	}
	auth_id = table_funcs.column_find_value(m->session->tr, auths_id, rid);

	table_funcs.table_insert(m->session->tr, roles, grantee_id, auth_id);
	_DELETE(grantee_id);
	_DELETE(auth_id);
	return TRUE;
}

int
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
		return FALSE;
	grantee_id = table_funcs.column_find_value(m->session->tr, auths_id, rid);

	rid = table_funcs.column_find_row(m->session->tr, auths_name, auth, NULL);
	if (rid == oid_nil) {
		_DELETE(grantee_id);
		return FALSE;
	}
	auth_id = table_funcs.column_find_value(m->session->tr, auths_id, rid);

	rid = table_funcs.column_find_row(m->session->tr, login_id, grantee_id, role_id, auth_id, NULL);
	table_funcs.table_delete(m->session->tr, roles, rid);
	_DELETE(grantee_id);
	_DELETE(auth_id);
	return TRUE;
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
sql_alter_user(mvc *m, str user, str passwd, char enc,
		sqlid schema_id, str oldpasswd)
{
	return(backend_alter_user(m, user, passwd, enc, schema_id, oldpasswd));
}

int
sql_rename_user(mvc *m, str olduser, str newuser)
{
	return(backend_rename_user(m, olduser, newuser));
}

int
sql_drop_user(mvc *m, str user)
{
	if (backend_drop_user(m,user) == FALSE)
		return FALSE;
	return sql_drop_role(m, user);
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
