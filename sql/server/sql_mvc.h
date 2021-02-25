/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#define ERR_AMBIGUOUS	050000
#define ERR_GROUPBY		060000
#define ERR_NOTFOUND	070000

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
#define m_prepare 	2
#define m_plan 		3

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

#define sql_shared_module_name "sql"
#define sql_private_module_name "user"

typedef struct sql_groupby_expression {
	symbol *sdef;
	tokens token;
	sql_exp *exp;
} sql_groupby_expression;

typedef struct sql_window_definition {
	char *name;
	dlist *wdef;
	bool visited; /* used for window definitions lookup */
} sql_window_definition;

typedef struct sql_local_table { /* declared tables during session */
	sql_table *table;
} sql_local_table;

typedef struct sql_rel_view { /* CTEs */
	char *name;
	sql_rel *rel_view;
} sql_rel_view;

typedef struct sql_var { /* Declared variables and parameters */
	char *sname; /* Global variables have a schema */
	char *name;
	atom var;
} sql_var;

typedef struct sql_frame {
	char *name; /* frame name */
	list *group_expressions;
	list *windows;
	list *tables;
	list *rel_views;
	list *vars;
	int frame_number;
} sql_frame;

typedef struct mvc {
	char errstr[ERRSIZE];

	sql_allocator *sa, *ta, *pa;

	struct scanner scanner;

	list *params;
	sql_func *forward;	/* forward definitions for recursive functions */
	list *global_vars; /* SQL declared variables on the global scope */
	sql_frame **frames;	/* stack of frames with variables */
	int topframes;
	int sizeframes;
	int frame;
	struct symbol *sym;

	int8_t use_views:1,
		   schema_path_has_sys:1, /* speed up object search */
		   schema_path_has_tmp:1;
	struct qc *qc;
	int clientid;		/* id of the owner */

	/* session variables */
	sqlid user_id;
	sqlid role_id;
	int timezone;		/* milliseconds west of UTC */
	int reply_size;		/* reply size */
	int debug;

	char emode;		/* execution mode */
	char emod;		/* execution modifier */

	sql_session *session;
	sql_store store;

	/* per query context */
	mapi_query_t type;	/* query type */

	/* during query needed flags */
	unsigned int label;	/* numbers for relational projection labels */
	list *cascade_action;  /* protection against recursive cascade actions */
	list *schema_path; /* schema search path for object lookup */
	uintptr_t sp;
} mvc;

extern sql_table *mvc_init_create_view(mvc *sql, sql_schema *s, const char *name, const char *query);

/* should return structure */
extern sql_store mvc_init(sql_allocator *pa, int debug, store_type store, int ro, int su);
extern void mvc_exit(sql_store store);

extern void mvc_logmanager(sql_store store);

extern mvc *mvc_create(sql_store *store, sql_allocator *pa, int clientid, int debug, bstream *rs, stream *ws);
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
sql_export str mvc_commit(mvc *c, int chain, const char *name, bool enabling_auto_commit);
sql_export str mvc_rollback(mvc *c, int chain, const char *name, bool disabling_auto_commit);
extern str mvc_release(mvc *c, const char *name);

extern sql_type *mvc_bind_type(mvc *sql, const char *name);
extern sql_type *schema_bind_type(mvc *sql, sql_schema * s, const char *name);

sql_export sql_schema *mvc_bind_schema(mvc *c, const char *sname);
sql_export sql_table *mvc_bind_table(mvc *c, sql_schema *s, const char *tname);
extern sql_column *mvc_bind_column(mvc *c, sql_table *t, const char *cname);
extern sql_column *mvc_first_column(mvc *c, sql_table *t);
extern sql_idx *mvc_bind_idx(mvc *c, sql_schema *s, const char *iname);
extern sql_key *mvc_bind_key(mvc *c, sql_schema *s, const char *kname);
extern sql_key *mvc_bind_ukey(sql_table *t, list *cols);
extern sql_trigger *mvc_bind_trigger(mvc *c, sql_schema *s, const char *tname);

extern sql_type *mvc_create_type(mvc *sql, sql_schema *s, const char *sqlname, unsigned int digits, unsigned int scale, int radix, const char *impl);
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
sql_export sql_column *mvc_create_column(mvc *c, sql_table *t, const char *name, sql_subtype *type);
extern sql_column *mvc_create_column_(mvc *c, sql_table *t, const char *name, const char *type, unsigned int digits);
extern sql_column *mvc_null(mvc *c, sql_column *col, int flag);
extern sql_column *mvc_default(mvc *c, sql_column *col, char *val);
extern sql_column *mvc_drop_default(mvc *c, sql_column *col);
extern sql_column *mvc_storage(mvc *c, sql_column *col, char *storage);
extern sql_table * mvc_access(mvc *m, sql_table *t, sht access);
extern int mvc_is_sorted(mvc *c, sql_column *col);
extern int mvc_is_unique(mvc *m, sql_column *col);
extern int mvc_is_duplicate_eliminated(mvc *c, sql_column *col);

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
extern int init_global_variables(mvc *sql);
extern sql_var *find_global_var(mvc *sql, sql_schema *s, const char *name);
extern sql_var *push_global_var(mvc *sql, const char *sname, const char *name, sql_subtype *type);

extern sql_var* frame_push_var(mvc *sql, const char *name, sql_subtype *type);
extern sql_local_table* frame_push_table(mvc *sql, sql_table *t);
extern sql_rel_view* stack_push_rel_view(mvc *sql, const char *name, sql_rel *var);
extern sql_window_definition* frame_push_window_def(mvc *sql, const char *name, dlist *wdef);
extern dlist* frame_get_window_def(mvc *sql, const char *name, int *pos);
extern sql_groupby_expression* frame_push_groupby_expression(mvc *sql, symbol *def, sql_exp *exp);
extern sql_exp* frame_get_groupby_expression(mvc *sql, symbol *def);
extern void stack_update_rel_view(mvc *sql, const char *name, sql_rel *view);

extern bool frame_check_var_visited(mvc *sql, int i);
extern void frame_set_var_visited(mvc *sql, int i);
extern void frame_clear_visited_flag(mvc *sql);

extern sql_frame *stack_push_frame(mvc *sql, const char *name);
extern void stack_pop_frame(mvc *sql);
extern void clear_frame(mvc *sql, sql_frame *frame);
extern void stack_pop_until(mvc *sql, int frame);

/* find variable in the stack */
extern sql_var *stack_find_var_frame(mvc *sql, const char *name, int *level);
extern sql_table *stack_find_table(mvc *sql, const char *name);
extern sql_table *frame_find_table(mvc *sq, const char *name);
extern sql_rel *stack_find_rel_view(mvc *sql, const char *name);
extern int stack_find_rel_view_projection_columns(mvc *sql, const char *name, sql_rel **res);

/* find variable in the current frame */
extern int frame_find_var(mvc *sql, const char *name);
extern sql_rel *frame_find_rel_view(mvc *sql, const char *name);

extern int stack_has_frame(mvc *sql, const char *name);
extern int stack_nr_of_declared_tables(mvc *sql);

extern atom *sqlvar_set(sql_var *var, ValRecord *v);
extern str sqlvar_get_string(sql_var *var);
extern str sqlvar_set_string(sql_var *var, const char *v);
#ifdef HAVE_HGE
extern hge val_get_number(ValRecord *val);
extern void sqlvar_set_number(sql_var *var, hge v);
#else
extern lng val_get_number(ValRecord *val);
extern void sqlvar_set_number(sql_var *var, lng v);
#endif

#define get_string_global_var(m, val) (sqlvar_get_string(find_global_var(m, mvc_bind_schema(m, "sys"), val)))

extern sql_column *mvc_copy_column(mvc *m, sql_table *t, sql_column *c);
extern sql_key *mvc_copy_key(mvc *m, sql_table *t, sql_key *k);
extern sql_idx *mvc_copy_idx(mvc *m, sql_table *t, sql_idx *i);
extern sql_trigger *mvc_copy_trigger(mvc *m, sql_table *t, sql_trigger *tr);

extern sql_rel *sql_processrelation(mvc *sql, sql_rel* rel, int value_based_opt, int storage_based_opt);

extern void *sql_error(mvc *sql, int error_code, _In_z_ _Printf_format_string_ char *format, ...)
	__attribute__((__format__(__printf__, 3, 4)));

extern int symbol_cmp(mvc* sql, symbol *s1, symbol *s2);

static inline int mvc_highwater(mvc *sql)
{
	int l = 0, rc = 0;
	uintptr_t c = (uintptr_t) (&l);

	size_t diff = c < sql->sp ? sql->sp - c : c - sql->sp;
	if (diff > THREAD_STACK_SIZE - 280 * 1024)
		rc = 1;
	return rc;
}

#endif /*_SQL_MVC_H*/
