/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/* multi version catalog */
#ifndef _SQL_MVC_H
#define _SQL_MVC_H

#include "sql_mem.h"
#include "gdk.h"
#include "sql_scan.h"
#include "sql_list.h"
#include "sql_types.h"
#include "sql_backend.h"
#include "sql_catalog.h"
#include "sql_relation.h"
#include "sql_storage.h"
#include "sql_keyword.h"
#include "mapi_querytype.h"
#include "sql_atom.h"
#include "sql_tokens.h"
#include "sql_symbol.h"

#define ERRSIZE 8192
#define ERR_AMBIGUOUS		050000
#define ERR_GROUPBY		060000

/* value vs predicate (boolean) */
#define type_value	0
#define type_relation	1

/* cardinality expected by enclosing operator */
#define card_none	-1	/* psm call doesn't return anything */
#define card_value	0
#define card_row 	1 /* needed for subqueries on single value tables (select (select 1))*/
#define card_column 	2
#define card_set	3 /* some operators require only a set (IN/EXISTS) */
#define card_exists	4
/* to be removed ie are in type (aka dimention) */
#define card_relation 	5
#define card_loader 	6

#define CARD_VALUE(card) (card == card_value || card == card_row || card == card_column || card == card_set || card == card_exists)

/* allowed to reduce (in the where and having parts we can reduce) */

/* different query execution modes (emode) */
#define m_normal 	0
#define m_deallocate 1
#define m_execute 	2
#define m_prepare 	3
#define m_plan 		4

/* special modes for function/procedure and view instantiation and
   dependency generation */
#define m_instantiate 	5
#define m_deps 		6

/* different query execution modifiers (emod) */
#define mod_none 	0
#define mod_debug 	1
#define mod_trace 	2
#define mod_explain 	4 
/* locked needs unlocking */
#define mod_locked 	16 

typedef struct sql_groupby_expression {
	symbol *sdef;
	tokens token;
	sql_exp *exp;
} sql_groupby_expression;

typedef struct sql_var {
	const char *name;
	atom a;
	sql_table *t;
	sql_rel *rel;
	dlist *wdef;
	sql_groupby_expression *exp;
	char view;
	char frame;
	char visited; //used for window definitions lookup
} sql_var;

#define MAXSTATS 8

typedef struct mvc {
	char errstr[ERRSIZE];

	sql_allocator *sa;
	struct qc *qc;
	int clientid;		/* id of the owner */
	struct scanner scanner;

	list *params;
	sql_func *forward;	/* forward definitions for recursive functions */
	sql_var *vars; 		/* stack of variables, frames are simply a
				   NULL in the var stack 
					(sometimes with name (label) ) */
	int topvars;
	int sizevars;
	int frame;
	int use_views;
	atom **args;
	int argc;
	int argmax;
	struct symbol *sym;
	int no_mitosis;		/* run query without mitosis */

	sqlid user_id;
	sqlid role_id;
	lng last_id;
	lng rowcnt;

	/* current session variables */
	int timezone;		/* milliseconds west of UTC */
	int cache;		/* some queries should not be cached ! */
	int caching;		/* cache current query ? */
	int reply_size;		/* reply size */
	bool sizeheader;	/* print size header in result set */
	int debug;

	lng Topt;		/* timer for optimizer phase */
	char emode;		/* execution mode */
	char emod;		/* execution modifier */

	sql_session *session;	

	mapi_query_t type;	/* query type */
	int pushdown;		/* AND or OR query handling */
	unsigned int label;	/* numbers for relational projection labels */
	int remote;
	list *cascade_action;  /* protection against recursive cascade actions */

	int opt_stats[MAXSTATS];/* keep statistics about optimizer rewrites */

	int result_id;
	res_table *results;
	char *query;		/* string, identify whatever we're working on */
} mvc;

/* NR_GLOBAL_VAR should match exactly the number of variables created in global_variables */
#define NR_GLOBAL_VARS 9

extern sql_table *mvc_init_create_view(mvc *sql, sql_schema *s, const char *name, const char *query);
extern int mvc_init(int debug, store_type store, int ro, int su, backend_stack stk);
extern void mvc_exit(void);
extern void mvc_logmanager(void);
extern void mvc_idlemanager(void);

extern mvc *mvc_create(int clientid, backend_stack stk, int debug, bstream *rs, stream *ws);
extern int mvc_reset(mvc *m, bstream *rs, stream *ws, int debug);
extern void mvc_destroy(mvc *c);

extern int mvc_status(mvc *c);
extern int mvc_error_retry(mvc *c); // error code on errors else 0, errors AMBIGUOUS and GROUPBY will also output 0
extern int mvc_type(mvc *c);
extern int mvc_debug_on(mvc *m, int flag);
extern void mvc_cancel_session(mvc *m);

/* since Savepoints and transactions are related the 
 * commit function includes the savepoint creation.
 * Rollbacks can be either full or until a given savepoint. 
 * The special mvc_release can be used to release savepoints. 
 */
#define has_snapshots(tr) ((tr) && (tr)->parent && (tr)->parent->parent)

extern int mvc_trans(mvc *c);
extern str mvc_commit(mvc *c, int chain, const char *name, bool enabling_auto_commit);
extern str mvc_rollback(mvc *c, int chain, const char *name, bool disabling_auto_commit);
extern str mvc_release(mvc *c, const char *name);

extern sql_type *mvc_bind_type(mvc *sql, const char *name);
extern sql_type *schema_bind_type(mvc *sql, sql_schema * s, const char *name);
extern sql_func *mvc_bind_func(mvc *sql, const char *name);
extern list *schema_bind_func(mvc *sql, sql_schema * s, const char *name, sql_ftype type);

extern sql_schema *mvc_bind_schema(mvc *c, const char *sname);
extern sql_table *mvc_bind_table(mvc *c, sql_schema *s, const char *tname);
extern sql_column *mvc_bind_column(mvc *c, sql_table *t, const char *cname);
extern sql_column *mvc_first_column(mvc *c, sql_table *t);
extern sql_idx *mvc_bind_idx(mvc *c, sql_schema *s, const char *iname);
extern sql_key *mvc_bind_key(mvc *c, sql_schema *s, const char *kname);
extern sql_key *mvc_bind_ukey(sql_table *t, list *cols);
extern sql_trigger *mvc_bind_trigger(mvc *c, sql_schema *s, const char *tname);

extern sql_type *mvc_create_type(mvc *sql, sql_schema *s, const char *sqlname, int digits, int scale, int radix, const char *impl);
extern int mvc_drop_type(mvc *sql, sql_schema *s, sql_type *t, int drop_action);

extern sql_func *mvc_create_func(mvc *sql, sql_allocator *sa, sql_schema *s, const char *name, list *args, list *res, sql_ftype type, sql_flang lang, const char *mod, const char *impl, const char *query, bit varres, bit vararg, bit system);
extern int mvc_drop_func(mvc *c, sql_schema *s, sql_func * func, int drop_action);
extern int mvc_drop_all_func(mvc *c, sql_schema *s, list *list_func, int drop_action);

extern int mvc_drop_schema(mvc *c, sql_schema *s, int drop_action);
extern sql_schema *mvc_create_schema(mvc *m, const char *name, sqlid auth_id, sqlid owner);
extern BUN mvc_clear_table(mvc *m, sql_table *t);
extern str mvc_drop_table(mvc *c, sql_schema *s, sql_table * t, int drop_action);
extern sql_table *mvc_create_table(mvc *c, sql_schema *s, const char *name, int tt, bit system, int persistence, int commit_action, int sz, bit properties);
extern sql_table *mvc_create_view(mvc *c, sql_schema *s, const char *name, int persistence, const char *sql, bit system);
extern sql_table *mvc_create_remote(mvc *c, sql_schema *s, const char *name, int persistence, const char *loc);

extern int mvc_drop_column(mvc *c, sql_table *t, sql_column *col, int drop_action);
extern sql_column *mvc_create_column(mvc *c, sql_table *t, const char *name, sql_subtype *type);
extern sql_column *mvc_create_column_(mvc *c, sql_table *t, const char *name, const char *type, int digits);
extern sql_column *mvc_null(mvc *c, sql_column *col, int flag);
extern sql_column *mvc_default(mvc *c, sql_column *col, char *val);
extern sql_column *mvc_drop_default(mvc *c, sql_column *col);
extern sql_column *mvc_storage(mvc *c, sql_column *col, char *storage);
extern sql_table * mvc_access(mvc *m, sql_table *t, sht access);
extern int mvc_is_sorted(mvc *c, sql_column *col);

extern sql_ukey *mvc_create_ukey(mvc *m, sql_table *t, const char *kname, key_type kt);
extern sql_key *mvc_create_ukey_done(mvc *m, sql_key *k);
extern sql_fkey *mvc_create_fkey(mvc *m, sql_table *t, const char *kname, key_type kt, sql_key *rk, int on_delete, int on_update);
extern sql_key *mvc_create_kc(mvc *m, sql_key *k, sql_column *c);
extern sql_fkey *mvc_create_fkc(mvc *m, sql_fkey *fk, sql_column *c);

extern int mvc_drop_key(mvc *c, sql_schema *s, sql_key *key, int drop_action);

extern sql_idx *mvc_create_idx(mvc *m, sql_table *t, const char *iname, idx_type it);
extern sql_idx *mvc_create_ic(mvc *m, sql_idx * i, sql_column *c);
extern int mvc_drop_idx(mvc *c, sql_schema *s, sql_idx * i);

extern sql_trigger * mvc_create_trigger(mvc *m, sql_table *t, const char *name, sht time, sht orientation, sht event, const char *old_name, const char *new_name, const char *condition, const char *statement );
extern sql_trigger * mvc_create_tc(mvc *m, sql_trigger * i, sql_column *c /*, extra options such as trunc */ );
extern int mvc_drop_trigger(mvc *m, sql_schema *s, sql_trigger * tri);

/*dependency control*/
extern void mvc_create_dependency(mvc *m, sqlid id, sqlid depend_id, sql_dependency depend_type);
extern void mvc_create_dependencies(mvc *m, list *id_l, sqlid depend_id, sql_dependency dep_type);
extern int mvc_check_dependency(mvc *m, sqlid id, sql_dependency type, list *ignore_ids);

/* variable management */
extern sql_var* stack_push_var(mvc *sql, const char *name, sql_subtype *type);
extern sql_var* stack_push_rel_var(mvc *sql, const char *name, sql_rel *var, sql_subtype *type);
extern sql_var* stack_push_table(mvc *sql, const char *name, sql_rel *var, sql_table *t);
extern sql_var* stack_push_rel_view(mvc *sql, const char *name, sql_rel *view);
extern sql_var* stack_push_window_def(mvc *sql, const char *name, dlist *sym);
extern dlist* stack_get_window_def(mvc *sql, const char *name, int *pos);
extern sql_var* stack_push_groupby_expression(mvc *sql, symbol *def, sql_exp *exp);
extern sql_exp* stack_get_groupby_expression(mvc *sql, symbol *def);
extern void stack_update_rel_view(mvc *sql, const char *name, sql_rel *view);

extern char stack_check_var_visited(mvc *sql, int i);
extern void stack_set_var_visited(mvc *sql, int i);
extern void stack_clear_frame_visited_flag(mvc *sql);

extern sql_var* stack_push_frame(mvc *sql, const char *name);
extern void stack_pop_frame(mvc *sql);
extern void stack_pop_until(mvc *sql, int top);
extern sql_subtype *stack_find_type(mvc *sql, const char *name);
extern sql_table *stack_find_table(mvc *sql, const char *name);
extern sql_rel *stack_find_rel_view(mvc *sql, const char *name);
extern int stack_find_var(mvc *sql, const char *name);
extern sql_rel *stack_find_rel_var(mvc *sql, const char *name);
/* find var in current frame */
extern int frame_find_var(mvc *sql, const char *name);
/* find frame holding variable 'name' */
extern int stack_find_frame(mvc *sql, const char *name);
/* find frame with given name */
extern int stack_has_frame(mvc *sql, const char *name);
extern int stack_nr_of_declared_tables(mvc *sql);

extern atom* stack_get_var(mvc *sql, const char *name);
extern atom* stack_set_var(mvc *sql, const char *name, ValRecord *v);

extern str stack_get_string(mvc *sql, const char *name);
extern str stack_set_string(mvc *sql, const char *name, const char *v);
#ifdef HAVE_HGE
extern hge val_get_number(ValRecord *val);
extern hge stack_get_number(mvc *sql, const char *name);
extern void stack_set_number(mvc *sql, const char *name, hge v);
#else
extern lng val_get_number(ValRecord *val);
extern lng stack_get_number(mvc *sql, const char *name);
extern void stack_set_number(mvc *sql, const char *name, lng v);
#endif

extern sql_column *mvc_copy_column(mvc *m, sql_table *t, sql_column *c);
extern sql_key *mvc_copy_key(mvc *m, sql_table *t, sql_key *k);
extern sql_idx *mvc_copy_idx(mvc *m, sql_table *t, sql_idx *i);
extern sql_trigger *mvc_copy_trigger(mvc *m, sql_table *t, sql_trigger *tr);
extern sql_part *mvc_copy_part(mvc *m, sql_table *t, sql_part *pt);

extern sql_rel *sql_processrelation(mvc *m, sql_rel* rel, int value_based_opt);

extern void *sql_error(mvc *sql, int error_code, _In_z_ _Printf_format_string_ char *format, ...)
	__attribute__((__format__(__printf__, 3, 4)));

extern int symbol_cmp(mvc* sql, symbol *s1, symbol *s2);

#endif /*_SQL_MVC_H*/
