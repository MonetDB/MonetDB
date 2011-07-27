/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
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
