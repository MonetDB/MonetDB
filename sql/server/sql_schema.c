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


#include "monetdb_config.h"
#include "sql_schema.h"
#include "sql_parser.h"
#include "sql_semantic.h"
#include "sql_privileges.h"
#include "sql_psm.h"

#include "rel_exp.h"
#include "rel_select.h"
#include "rel_updates.h"
#include "rel_subquery.h"
#include "rel_optimizer.h"
#include "rel_bin.h"

static stmt *
create_type(mvc *sql, dlist *qname, char *impl)
{
	char *tname = qname_table(qname);

	if (!mvc_create_type(sql, sql->session->schema, tname, 0, 0, 0, impl)) {
		return sql_error(sql, 02, "CREATE TYPE: unknown external type '%s'", impl);
	}
	return stmt_none(sql->sa);
}

static void
stack_push_table(mvc *sql, char *tname, sql_table *t)
{
	sql_rel *r = rel_basetable(sql, t, tname );
		
	stack_push_rel_view(sql, tname, r);
}

static stmt *
create_trigger(mvc *sql, dlist *qname, int time, symbol *trigger_event, 
	char *table_name, dlist *opt_ref, dlist *triggered_action)
{
	stmt *sq = NULL;
	sql_trigger *trigger = NULL;
	char *tname = qname_table(qname);
	sql_schema *ss = cur_schema(sql);
	sql_table *t = NULL;
	int instantiate = (sql->emode == m_instantiate);
	int create = (!instantiate && sql->emode != m_deps);
	char emode = sql->emode;

	dlist *columns = trigger_event->data.lval;
	char *old_name = NULL, *new_name = NULL; 
	dlist *stmts = triggered_action->h->next->next->data.lval;
	
	if (opt_ref) {
		dnode *dl = opt_ref->h;
		for ( ; dl; dl = dl->next) {
			/* list (new(1)/old(0)), char */
			char *n = dl->data.lval->h->next->data.sval;

			assert(dl->data.lval->h->type == type_int);
			if (!dl->data.lval->h->data.i_val) /*?l_val?*/
				old_name = n;
			else
				new_name = n;
		}
	}
	if (create && !schema_privs(sql->role_id, ss)) 
		return sql_error(sql, 02, "CREATE TRIGGER: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), ss->base.name);
	if (create && (trigger = mvc_bind_trigger(sql, ss, tname )) != NULL) 
		return sql_error(sql, 02, "CREATE TRIGGER: name '%s' already in use", tname);
	
	if (create && !(t = mvc_bind_table(sql, ss, table_name)))
		return sql_error(sql, 02, "CREATE TRIGGER: unknown table '%s'", table_name);
	if (create && isView(t)) 
		return sql_error(sql, 02, "CREATE TRIGGER: cannot create trigger on view '%s'", tname);
	
	
	if (create) {
		int event = (trigger_event->token == SQL_INSERT)?0:
			    (trigger_event->token == SQL_DELETE)?1:2;
		int orientation = triggered_action->h->data.i_val;
		char *condition = triggered_action->h->next->data.sval;
		char *q = QUERY(sql->scanner);

		assert(triggered_action->h->type == type_int);
		trigger = mvc_create_trigger(sql, t, tname, time, orientation, 
			event, old_name, new_name, condition, q);
	}
	if (!create)
		t = mvc_bind_table(sql, ss, table_name);

	if (create) {
		stack_push_frame(sql, "OLD-NEW");
		/* we need to add the old and new tables */
		if (new_name)
			stack_push_table(sql, new_name, t);
		if (old_name)
			stack_push_table(sql, old_name, t);
	}
	if (create) /* for subtable we only need direct dependencies */
		sql->emode = m_deps;
	sq = sequential_block(sql, NULL, stmts, NULL, 1);
	sql->emode = emode;
	if (create)
		stack_pop_frame(sql);
	
	if (sq && create) {
		list *col_l = stmt_list_dependencies(sql->sa, sq, COLUMN_DEPENDENCY);
		list *func_l = stmt_list_dependencies(sql->sa, sq, FUNC_DEPENDENCY);
		list *view_id_l = stmt_list_dependencies(sql->sa, sq, VIEW_DEPENDENCY);

		mvc_create_dependencies(sql, col_l, trigger->base.id, TRIGGER_DEPENDENCY);
		mvc_create_dependencies(sql, func_l, trigger->base.id, TRIGGER_DEPENDENCY);
		mvc_create_dependencies(sql, view_id_l, trigger->base.id, TRIGGER_DEPENDENCY);

		sq = stmt_none(sql->sa);
	}
	
	/* todo trigger_columns */
	(void)columns;

	return sq;
}

static stmt *
drop_trigger(mvc *sql, dlist *qname)
{
	char *tname = qname_table(qname);
	sql_schema *ss = cur_schema(sql);
	sql_trigger *tri = NULL;

	if (!schema_privs(sql->role_id, ss)) 
		return sql_error(sql, 02, "DROP TRIGGER: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), ss->base.name);
	if ((tri = mvc_bind_trigger(sql, ss, tname )) == NULL)
		return sql_error(sql, 02, "DROP TRIGGER: unknown trigger %s\n", tname);
	mvc_drop_trigger(sql, ss, tri);
	return stmt_none(sql->sa);
}

static stmt *
connect_catalog(mvc *sql, dlist *qname)
{
	char *server, * db, *db_alias, *user, *lang, *passwd; 
	int *id = GDKmalloc(sizeof(int)), *port = GDKmalloc(sizeof(int));
	symbol *user_symb = NULL;
	char *default_dbalias;
	*id = -1;

	if (qname->cnt) {
		user_symb = qname->h->next->next->next->next->data.sym;
		assert(qname->h->next->type == type_int);
		*port = qname->h->next->data.i_val;
		server = GDKstrdup(qname->h->data.sval);
		db = GDKstrdup(qname->h->next->next->data.sval);
		user = GDKstrdup(user_symb->data.lval->h->data.sval);
		passwd = GDKstrdup(user_symb->data.lval->h->next->data.sval);
	} else {
		return sql_error(sql, 02, "CONNECT TO: DEFAULT is not supported!");
	}

	if (!qname->cnt || qname->h->next->next->next->data.sval == NULL){
		default_dbalias = sql_message( "%s_%s_%s", server, db, user);
		db_alias = default_dbalias;
	} else {
		db_alias = GDKstrdup(qname->h->next->next->next->data.sval);
	}

	if (!qname->cnt || qname->h->next->next->next->next->next->data.sval == NULL){
		lang = GDKstrdup("sql");
	}
	else {
		lang = GDKstrdup(qname->h->next->next->next->next->next->data.sval);
	}
	
	*id = mvc_connect_catalog(sql, server, *port, db, db_alias, user, passwd, lang);
	
	if (*id == 0)	
		return sql_error(sql, 02, "CONNECT TO: this connection already exists or the db_alias '%s' was already used!", db_alias);
	
	return stmt_connection(sql->sa, id, server, port, db, db_alias, user, passwd, lang);	
}

static stmt *
disconnect_catalog(mvc *sql, dlist *qname)
{
	int *id = GDKmalloc(sizeof(int)), *port = GDKmalloc(sizeof(int));
	char *db_alias = NULL;
	*id = 0, *port = -1;
	
	if (qname->cnt != 0) {
		db_alias = GDKstrdup(qname->h->data.sval);
	
		*id = mvc_disconnect_catalog(sql, db_alias);

		if (*id == 0)	
			return sql_error(sql, 02, "DISCONNECT CATALOG: no such db_alias '%s'", db_alias);
		else
			return stmt_connection(sql->sa, id, NULL, port, NULL, db_alias, NULL, NULL, NULL);	
	} else {
		mvc_disconnect_catalog_ALL(sql);	
		return stmt_connection(sql->sa, id, NULL, port, NULL, NULL, NULL, NULL, NULL);	
	}
}

static stmt *
create_index(mvc *sql, sql_schema *ss, char *iname, int itype, dlist *qname, dlist *column_list)
{

	char *name = qname_table(qname);
	char *sname = qname_schema(qname);
	sql_schema *s = NULL;
	sql_table *t = NULL;
	sql_idx *i = NULL;

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, "CREATE INDEX: no such schema '%s'", sname);
	if (s == NULL) 
		s = ss;

	i = mvc_bind_idx(sql, s, iname);
	t = mvc_bind_table(sql, s, name);
	if (i) {
		return sql_error(sql, 02, "CREATE INDEX: name '%s' already in use", iname);
	} else if (!t) {
		return sql_error(sql, 02, "CREATE INDEX: no such table '%s'", name);
	} else if (isView(t)) {
		return sql_error(sql, 02, "CREATE INDEX: cannot create index on view '%s'", name);
	} else {
		dnode *n = column_list->h;
		sql_idx *i = mvc_create_idx(sql, t, iname, (idx_type) itype);

		if (!i) {
			/* Fabian: I doubt whether this error can be triggered since
			 * the more generic case is already checked above. */
			return sql_error(sql, 02, "CREATE INDEX: index '%s' already exists", iname);
		}
		for (; n; n = n->next) {
			char *cname = n->data.sval;
			sql_column *c = mvc_bind_column(sql, t, cname);

			if (!c) {
				return sql_error(sql, 02, "CREATE INDEX: no such column '%s'", cname);
			} else {
				mvc_create_ic(sql, i, c);
				mvc_create_dependency(sql, c->base.id, i->base.id, INDEX_DEPENDENCY);
			}
		}
		return stmt_none(sql->sa);
	}
}

static stmt *
drop_index(mvc *sql, dlist *qname)
{
	stmt *res = NULL;
	char *iname = qname_table(qname);
	char *sname = qname_schema(qname);
	sql_schema *s = NULL;
	sql_idx *i = NULL;

	if (sname && !(s=mvc_bind_schema(sql, sname))) {
		(void) sql_error(sql, 02, "DROP INDEX: no such schema '%s'", sname);
		return NULL;
	}
	if (!s)
		s = cur_schema(sql);
 	i = mvc_bind_idx(sql, s, iname);
	if (!i) {
		return sql_error(sql, 02, "DROP INDEX: no such index '%s'", iname);
	} else if (!schema_privs(sql->role_id, s)) {
		return sql_error(sql, 02, "DROP INDEX: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), s->base.name);
	} else {
		mvc_drop_idx(sql, s, i);
		res = stmt_none(sql->sa);
	}
	return res;
}

static stmt *
create_user(mvc *sql, char *user, char *passwd, char enc, char *fullname, char *schema)
{
	char *err; 
	int schema_id = 0;

	if (backend_find_user(sql, user) >= 0) {
		return sql_error(sql, 02, "CREATE USER: user '%s' already exists", user);
	}
	if ((schema_id = sql_find_schema(sql, schema)) < 0) {
		return sql_error(sql, 02, "CREATE USER: no such schema '%s'", schema);
	}
	if ((err = backend_create_user(sql, user, passwd, enc, fullname,
					schema_id, sql->user_id)) != NULL)
	{
		(void) sql_error(sql, 02, "CREATE USER: %s", err);
		GDKfree(err);
		return FALSE;
	}
	return stmt_none(sql->sa);
}

static stmt *
drop_user(mvc *sql, char *user)
{
	int user_id = sql_find_auth(sql, user);
	if (mvc_check_dependency(sql, user_id, OWNER_DEPENDENCY, NULL))
		return sql_error(sql, 02, "DROP USER: '%s' owns a schema", user);

	if(sql_drop_user(sql, user) != NULL)
		return sql_error(sql, 02, "DROP USER: no such user '%s'", user);

	return stmt_none(sql->sa);
}

static stmt *
alter_user(mvc *sql, char *user, char *passwd, char enc,
		char *schema, char *oldpasswd)
{
	sqlid schema_id = 0;
	/* USER == NULL -> current_user */
	if (user != NULL && backend_find_user(sql, user) < 0)
		return sql_error(sql, 02, "ALTER USER: no such user '%s'", user);

	if (sql->user_id != USER_MONETDB && sql->role_id != ROLE_SYSADMIN && user != NULL && strcmp(user, stack_get_string(sql, "current_user")) != 0)
		return sql_error(sql, 02, "ALTER USER: insufficient privileges to change user '%s'", user);
	if (schema && (schema_id = sql_find_schema(sql, schema)) < 0) {
		return sql_error(sql, 02, "ALTER USER: no such schema '%s'", schema);
	}
	sql_alter_user(sql, user, passwd, enc, schema_id, oldpasswd);

	return stmt_none(sql->sa);
}

static stmt *
rename_user(mvc *sql, char *olduser, char *newuser)
{
	if (backend_find_user(sql, olduser) < 0)
		return sql_error(sql, 02, "ALTER USER: no such user '%s'", olduser);
	if (backend_find_user(sql, newuser) >= 0)
		return sql_error(sql, 02, "ALTER USER: user '%s' already exists",
				newuser);

	if (sql->user_id != USER_MONETDB && sql->role_id != ROLE_SYSADMIN)
		return sql_error(sql, 02, "ALTER USER: insufficient privileges to "
				"rename user '%s'", olduser);

	sql_rename_user(sql, olduser, newuser);

	return stmt_none(sql->sa);
}


#if 0
static stmt *
sql_update_add_idx(mvc *sql, sql_table *t, list *cols)
{
	list *exps;
	sql_kc *kc = cols->h->data;
	sql_rel *updates, *update;

 	exps = new_exp_list();
	exps = append(exps, 
		exp_column(t->base.name, "%TID%", sql_bind_localtype("oid"), CARD_MULTI, 0, 1));
	exps = append(exps, 
		exp_column(t->base.name, kc->c->base.name, &kc->c->type, CARD_MULTI, 0, 1 ));
	updates = rel_project(rel_basetable(sql, t, t->base.name), exps);
 	exps = new_exp_list();
	exps = append(exps, 
		exp_column(t->base.name, kc->c->base.name, &kc->c->type, CARD_MULTI, 0, 1 ));
	update = rel_update(rel_basetable(sql, t, t->base.name), updates, exps);
	update = rel_optimizer(sql, update);
	return rel_bin(sql, update);
}
#endif

stmt *
schemas(mvc *sql, symbol *s)
{
	stmt *ret = NULL;

	if (!QUERY_MODE(sql->emode))
		return sql_error(sql, 05, "schema statements are directly executed "
				"and therefore cannot be debugged, explained, profiled, "
				"traced or used in a prepared statement");

	if (s->token != SQL_CREATE_TABLE && STORE_READONLY(active_store_type)) 
		return sql_error(sql, 06, "schema statements cannot be executed on a readonly database.");

	switch (s->token) {
	case SQL_CREATE_INDEX:
	{
		dlist *l = s->data.lval;

		assert(l->h->next->type == type_int);
		ret = create_index(sql, cur_schema(sql), l->h->data.sval, l->h->next->data.i_val, l->h->next->next->data.lval, l->h->next->next->next->data.lval);
		sql->type = Q_SCHEMA;
	}
		break;
	case SQL_DROP_INDEX:
	{
		dlist *l = s->data.lval;

		ret = drop_index(sql, l);	/* index name */
		sql->type = Q_SCHEMA;
	}
		break;
	case SQL_CREATE_USER:
	{
		dlist *l = s->data.lval;

		ret = create_user(sql, l->h->data.sval,	/* user name */
				  l->h->next->data.sval,	/* password */
				  l->h->next->next->next->next->data.i_val == SQL_PW_ENCRYPTED, /* encrypted */
				  l->h->next->next->data.sval,	/* fullname */
				  l->h->next->next->next->data.sval);	/* dschema */
		sql->type = Q_SCHEMA;
	}
		break;
	case SQL_DROP_USER:
		ret = drop_user(sql, s->data.sval);	/* user name */
		sql->type = Q_SCHEMA;
		break;
	case SQL_ALTER_USER:
	{
		dlist *l = s->data.lval;

		ret = alter_user(sql, l->h->data.sval,	/* user */
				 l->h->next->data.lval->h->data.sval,	/* passwd */
				 l->h->next->data.lval->h->next->next->data.i_val == SQL_PW_ENCRYPTED, /* encrypted */
				 l->h->next->data.lval->h->next->data.sval,	/* schema */
				 l->h->next->data.lval->h->next->next->next->data.sval /* old passwd */
		    );
		sql->type = Q_SCHEMA;
	}
		break;
	case SQL_RENAME_USER:
	{
		dlist *l = s->data.lval;

		ret = rename_user(sql, l->h->data.sval, l->h->next->data.sval);
		sql->type = Q_SCHEMA;
	}
		break;

	case SQL_CREATE_TYPE:
	{
		dlist *l = s->data.lval;

		ret = create_type(sql, l->h->data.lval, l->h->next->data.sval);
		sql->type = Q_SCHEMA;
	}
		break;

	case SQL_CREATE_TRIGGER:
	{
		dlist *l = s->data.lval;

		assert(l->h->next->type == type_int);
		ret = create_trigger(sql, l->h->data.lval, l->h->next->data.i_val, l->h->next->next->data.sym, l->h->next->next->next->data.sval, l->h->next->next->next->next->data.lval, l->h->next->next->next->next->next->data.lval);
		sql->type = Q_SCHEMA;
	}
		break;

	case SQL_DROP_TRIGGER:
	{
		dlist *l = s->data.lval;

		ret = drop_trigger(sql, l);
		sql->type = Q_SCHEMA;
	}
		break;

	case SQL_CONNECT:
	{
		dlist *l = s->data.lval;

		ret = connect_catalog(sql, l);
		sql->type = Q_SCHEMA;
	}
		break;

	case SQL_DISCONNECT:
	{
		dlist *l = s->data.lval;

		ret = disconnect_catalog(sql, l);
		sql->type = Q_SCHEMA;
	}
		break;

	default:
		return sql_error(sql, 01, "schema statement unknown symbol(" PTRFMT ")->token = %s", PTRFMTCAST s, token2string(s->token));
	}
	sql->last = NULL;
	return ret;
}
