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

/* multi version catalog */
#ifndef _SQL_MVC_H
#define _SQL_MVC_H

#include <sql_mem.h>
#include <gdk.h>
#include <stdarg.h>
#include <sql_scan.h>
#include <sql_list.h>
#include <sql_types.h>
#include <sql_backend.h>
#include <sql_catalog.h>
#include <sql_relation.h>
#include <sql_storage.h>
#include <sql_keyword.h>
#include <sql_atom.h>
#ifdef HAVE_SYS_TIMES_H
#include <sys/times.h>
#endif

#include <mapi.h>

#define ERRSIZE 8192

/* different query execution modes (emode) */
#define m_normal 0
#define m_inplace 1 
#define m_execute 2
#define m_prepare 3
#define m_explain 4 
#define m_plan 5 

/* special modes for function/procedure and view instantiation and
   dependency generation */
#define m_instantiate 6
#define m_deps 7

#define QUERY_MODE(m) (m==m_normal || m==m_inplace || m==m_instantiate || m==m_deps)


/* different query execution modifiers (emod) */
#define mod_none 0
#define mod_debug 2
#define mod_trace 4
/* locked needs unlocking */
#define mod_locked 8 

typedef struct sql_var {
	void *s;	
	char *name;
	ValRecord value;
	sql_subtype type;
	int view;
} sql_var;

#define MAXSTATS 8

typedef struct mvc {
	char errstr[ERRSIZE];

	sql_allocator *sa;
	struct qc *qc;
	int clientid;		/* id of the owner */
	struct scanner scanner;

	list *params;
	sql_var *vars; 		/* stack of variables, frames are simply a
				   NULL in the var stack 
					(sometimes with name (label) ) */
	int topvars;
	int sizevars;
	int frame;
	atom **args;
	int argc;
	int argmax;
	struct symbol *sym;
	int point_query;	/* mark when a query is a point query */

	int user_id;
	int role_id;
	lng last_id;

	/* current session variables */
	int timezone;		/* minutes west of UTC */
	int cache;		/* some queries should not be cached ! */
	int caching;		/* cache current query ? */
	int history;		/* queries statistics are kept  */
	int reply_size;		/* reply size */
	int sizeheader;		/* print size header in result set */
	int debug;

	char emode;		/* execution mode */
	char emod;		/* execution modifier */

	sql_session *session;	

	int type;		/* query type */
	int label;		/* numbers for relational projection labels */
	list *cascade_action;  /* protection against recursive cascade actions */

	int opt_stats[MAXSTATS];/* keep statistics about optimizer rewrites */

	int result_id;
	res_table *results;
	sql_column *last; 	/* last accessed column */
#ifdef HAVE_TIMES
	struct tms times;
#endif	
	lng Tparse;
} mvc;

extern int mvc_init(char *dbname, int debug, store_type store, backend_stack stk);
extern void mvc_exit(void);
extern void mvc_logmanager(void);
extern void mvc_minmaxmanager(void);

extern mvc *mvc_create(int clientid, backend_stack stk, int debug, bstream *rs, stream *ws);
extern void mvc_reset(mvc *m, bstream *rs, stream *ws, int debug, int globalvars);
extern void mvc_destroy(mvc *c);

extern int mvc_status(mvc *c);
extern int mvc_type(mvc *c);

/* since Savepoints and transactions are related the 
 * commit function includes the savepoint creation.
 * Rollbacks can be either full or until a given savepoint. 
 * The special mvc_release can be used to release savepoints. 
 */
#define has_snapshots(tr) ((tr) && (tr)->parent && (tr)->parent->parent)

extern void mvc_trans(mvc *c);
extern int mvc_commit(mvc *c, int chain, char *name);
extern int mvc_rollback(mvc *c, int chain, char *name);
extern int mvc_release(mvc *c, char *name);

extern sql_type *mvc_bind_type(mvc *sql, char *name);
extern sql_type *schema_bind_type(mvc *sql, sql_schema * s, char *name);
extern sql_func *mvc_bind_func(mvc *sql, char *name);
extern list *schema_bind_func(mvc *sql, sql_schema * s, char *name, int type);

extern sql_schema *mvc_bind_schema(mvc *c, char *sname);
extern sql_table *mvc_bind_table(mvc *c, sql_schema *s, char *tname);
extern sql_column *mvc_bind_column(mvc *c, sql_table *t, char *cname);
extern sql_column *mvc_first_column(mvc *c, sql_table *t);
extern sql_idx *mvc_bind_idx(mvc *c, sql_schema *s, char *iname);
extern sql_key *mvc_bind_key(mvc *c, sql_schema *s, char *kname);
extern sql_key *mvc_bind_ukey(sql_table *t, list *cols);
extern sql_trigger *mvc_bind_trigger(mvc *c, sql_schema *s, char *tname);

extern sql_type *mvc_create_type(mvc *sql, sql_schema *s, char *sqlname, int digits, int scale, int radix, char *impl);
extern sql_func *mvc_create_func(mvc *sql, sql_schema *s, char *name, list *args, sql_subtype *res, int type, char *mod, char *impl, char *query);
extern void mvc_drop_func(mvc *c, sql_schema *s, sql_func * func, int drop_action);
extern void mvc_drop_all_func(mvc *c, sql_schema *s, list *list_func, int drop_action);

extern void mvc_drop_schema(mvc *c, sql_schema *s, int drop_action);
extern sql_schema *mvc_create_schema(mvc *m, char *name, int auth_id, int owner);
extern BUN mvc_clear_table(mvc *m, sql_table *t);
extern void mvc_drop_table(mvc *c, sql_schema *s, sql_table * t, int drop_action);
extern sql_table *mvc_create_table(mvc *c, sql_schema *s, char *name, int tt, bit system, int persistence, int commit_action, int sz);
extern sql_table *mvc_create_view(mvc *c, sql_schema *s, char *name, int persistence, char *sql, bit system);
extern sql_table *mvc_create_generated(mvc *c, sql_schema *s, char *name, char *sql, bit system);
extern sql_table *mvc_create_remote(mvc *c, sql_schema *s, char *name, int persistence, char *loc);

extern void mvc_drop_column(mvc *c, sql_table *t, sql_column *col, int drop_action);
extern sql_column *mvc_create_column(mvc *c, sql_table *t, char *name, sql_subtype *type);
extern sql_column *mvc_create_column_(mvc *c, sql_table *t, char *name, char *type, int digits);
extern sql_column *mvc_null(mvc *c, sql_column *col, int flag);
extern sql_column *mvc_default(mvc *c, sql_column *col, char *val);
extern sql_column *mvc_drop_default(mvc *c, sql_column *col);
extern sql_table * mvc_readonly(mvc *m, sql_table *t, int readonly);
extern int mvc_is_sorted(mvc *c, sql_column *col);

extern sql_ukey *mvc_create_ukey(mvc *m, sql_table *t, char *kname, key_type kt);
extern sql_key *mvc_create_ukey_done(mvc *m, sql_key *k);
extern sql_fkey *mvc_create_fkey(mvc *m, sql_table *t, char *kname, key_type kt, sql_key *rk, int on_delete, int on_update);
extern sql_key *mvc_create_kc(mvc *m, sql_key *k, sql_column *c);
extern sql_fkey *mvc_create_fkc(mvc *m, sql_fkey *fk, sql_column *c);

extern void mvc_drop_key(mvc *c, sql_schema *s, sql_key *key, int drop_action);

extern sql_idx *mvc_create_idx(mvc *m, sql_table *t, char *iname, idx_type it);
extern sql_idx *mvc_create_ic(mvc *m, sql_idx * i, sql_column *c);
extern void mvc_drop_idx(mvc *c, sql_schema *s, sql_idx * i);

extern sql_trigger * mvc_create_trigger(mvc *m, sql_table *t, char *name, sht time, sht orientation, sht event, char *old_name, char *new_name, char *condition, char *statement );
extern sql_trigger * mvc_create_tc(mvc *m, sql_trigger * i, sql_column *c /*, extra options such as trunc */ );
extern void mvc_drop_trigger(mvc *m, sql_schema *s, sql_trigger * tri);

/*dependency control*/
extern void mvc_create_dependency(mvc *m, int id, int depend_id, int depend_type);
extern void mvc_create_dependencies(mvc *m, list *id_l, sqlid depend_id, int dep_type);
extern int mvc_check_dependency(mvc * m, int id, int type, list *ignore_ids);
extern int mvc_connect_catalog(mvc *m, char *server, int port, char *db, char *db_alias, char *user, char *passwd, char *lng);
extern int mvc_disconnect_catalog(mvc *m, char *db_alias);
extern int mvc_disconnect_catalog_ALL(mvc *m);

/* variable management */
extern void stack_push_var(mvc *sql, char *name, sql_subtype *type);
extern void stack_push_rel_var(mvc *sql, char *name, sql_rel *var, sql_subtype *type);
extern void stack_push_rel_view(mvc *sql, char *name, sql_rel *view);

extern void stack_push_frame(mvc *sql, char *name);
extern void stack_pop_frame(mvc *sql);
extern void stack_pop_until(mvc *sql, int top);
extern sql_subtype *stack_find_type(mvc *sql, char *name);
extern sql_rel *stack_find_rel_view(mvc *sql, char *name);
extern int stack_find_var(mvc *sql, char *name);
extern sql_rel *stack_find_rel_var(mvc *sql, char *name);
/* find var in current frame */
extern int frame_find_var(mvc *sql, char *name);
/* find frame holding variable 'name' */
extern int stack_find_frame(mvc *sql, char *name);
/* find frame with given name */
extern int stack_has_frame(mvc *sql, char *name);
extern int stack_nr_of_declared_tables(mvc *sql);

extern ValRecord * stack_get_var(mvc *sql, char *name);
extern void stack_set_var(mvc *sql, char *name, ValRecord *v);

extern str stack_get_string(mvc *sql, char *name);
extern void stack_set_string(mvc *sql, char *name, str v);
extern lng stack_get_number(mvc *sql, char *name);
extern void stack_set_number(mvc *sql, char *name, lng v);

extern sql_column *mvc_copy_column(mvc *m, sql_table *t, sql_column *c);
extern sql_key *mvc_copy_key(mvc *m, sql_table *t, sql_key *k);
extern sql_idx *mvc_copy_idx(mvc *m, sql_table *t, sql_idx *i);

#endif /*_SQL_MVC_H*/
