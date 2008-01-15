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
 * Portions created by CWI are Copyright (C) 1997-2008 CWI.
 * All Rights Reserved.
 */

#ifndef SQL_STORAGE_H
#define SQL_STORAGE_H

#include "sql_catalog.h"

#define COLSIZE	1024

#define isNew(x)  (x->base.flag == TR_NEW)
#define isTemp(x) (isNew(x)||x->t->persistence!=SQL_PERSIST)
#define isTempTable(x)   (x->persistence!=SQL_PERSIST)
#define isGlobalTable(x) (x->persistence!=SQL_LOCAL_TEMP)
#define isGlobalTemp(x) (x->persistence==SQL_GLOBAL_TEMP)
#define isTempSchema(x)  (strcmp(x->base.name,"tmp") == 0)

typedef enum store_type {
	store_bat,	/* delta bats, ie multi user read/write */
	store_su,	/* single user, read/write */
	store_ro,	/* multi user, read only */
	store_suro,	/* single user, read only */
	store_bpm	/* bat partition manager */
} store_type;

extern sql_trans *gtrans;
extern int store_nr_active;
extern store_type active_store_type;

/* relational interface */
typedef ssize_t (*column_find_row_fptr)(sql_trans *tr, sql_column *c, void *value, ...);
typedef void *(*column_find_value_fptr)(sql_trans *tr, sql_column *c, ssize_t rid);
typedef int (*column_update_value_fptr)(sql_trans *tr, sql_column *c, ssize_t rid, void *value);
typedef int (*table_insert_fptr)(sql_trans *tr, sql_table *t, ...);
typedef int (*table_delete_fptr)(sql_trans *tr, sql_table *t, ssize_t rid);

typedef struct rids {
	size_t cur;
	void *data;
} rids;

/* returns table rids, for the given select ranges */
typedef rids *(*rids_select_fptr)( sql_trans *tr, sql_column *key, void *key_value_low, void *key_value_high, ...);

/* order rids by orderby_column values */
typedef rids *(*rids_orderby_fptr)( sql_trans *tr, rids *r, sql_column *orderby_col);

typedef rids *(*rids_join_fptr)( sql_trans *tr, rids *l, sql_column *lc, rids *r, sql_column *rc);

/* return table rids from result of table_select, return (-1) when done */
typedef ssize_t (*rids_next_fptr)(rids *r);

/* clean up the resources taken by the result of table_select */
typedef void (*rids_destroy_fptr)(rids *r);

typedef struct table_functions {
	column_find_row_fptr column_find_row;
	column_find_value_fptr column_find_value;
	column_update_value_fptr column_update_value;
	table_insert_fptr table_insert;
	table_delete_fptr table_delete;

	rids_select_fptr rids_select;
	rids_orderby_fptr rids_orderby;
	rids_join_fptr rids_join;
	rids_next_fptr rids_next;
	rids_destroy_fptr rids_destroy;
} table_functions; 

extern table_functions table_funcs;

/* delta table setup (ie readonly col + ins + upd + del)
-- binds for column,idx (rdonly, inserts, updates) and delets
*/
typedef void *(*bind_col_fptr) (sql_trans *tr, sql_column *c, int access);
typedef void *(*bind_idx_fptr) (sql_trans *tr, sql_idx *i, int access);
typedef void *(*bind_del_fptr) (sql_trans *tr, sql_table *t, int access);

/*
-- append/update to columns and indices 
*/
typedef void (*append_col_fptr) (sql_trans *tr, sql_column *c, void *d, int t);
typedef void (*append_idx_fptr) (sql_trans *tr, sql_idx *i, void *d, int t);
typedef void (*update_col_fptr) (sql_trans *tr, sql_column *c, void *d, int t, ssize_t rid);
typedef void (*update_idx_fptr) (sql_trans *tr, sql_idx *i, void *d, int t);
typedef void (*delete_tab_fptr) (sql_trans *tr, sql_table *t, void *d, int tpe);

/*
-- create the necessary storage resources for columns, indices and tables
-- returns LOG_OK, LOG_ERR
*/
typedef int (*create_col_fptr) (sql_trans *tr, sql_column *c); 
typedef int (*create_idx_fptr) (sql_trans *tr, sql_idx *i); 
typedef int (*create_del_fptr) (sql_trans *tr, sql_table *t); 

/*
-- duplicate the necessary storage resources for columns, indices and tables
-- returns LOG_OK, LOG_ERR
*/
typedef int (*dup_col_fptr) (sql_trans *tr, sql_column *oc, sql_column *c);
typedef int (*dup_idx_fptr) (sql_trans *tr, sql_idx *oi, sql_idx *i ); 
typedef int (*dup_del_fptr) (sql_trans *tr, sql_table *ot, sql_table *t); 

/*
-- free the storage resources for columns, indices and tables
-- returns LOG_OK, LOG_ERR
*/
typedef int (*destroy_col_fptr) (sql_trans *tr, sql_column *c); 
typedef int (*destroy_idx_fptr) (sql_trans *tr, sql_idx *i); 
typedef int (*destroy_del_fptr) (sql_trans *tr, sql_table *t); 

/*
-- clear any storage resources for columns, indices and tables
-- returns number of removed tuples
*/
typedef size_t (*clear_col_fptr) (sql_trans *tr, sql_column *c); 
typedef size_t (*clear_idx_fptr) (sql_trans *tr, sql_idx *i); 
typedef size_t (*clear_del_fptr) (sql_trans *tr, sql_table *t); 

/*
-- update_table rollforward the changes made from table ft to table tt 
-- returns LOG_OK, LOG_ERR
*/
typedef int (*update_table_fptr) (sql_trans *tr, sql_table *ft, sql_table *tt); 

/*
-- handle inserts and updates of columns and indices
-- returns LOG_OK, LOG_ERR
*/
typedef int (*col_ins_fptr) (sql_trans *tr, sql_column *c, void *data);
typedef int (*col_upd_fptr) (sql_trans *tr, sql_column *c, void *rows, void *data);
typedef int (*idx_ins_fptr) (sql_trans *tr, sql_idx *c, void *data);
typedef int (*idx_upd_fptr) (sql_trans *tr, sql_idx *c, void *rows, void *data);
/*
-- handle deletes
-- returns LOG_OK, LOG_ERR
*/
typedef int (*del_fptr) (sql_trans *tr, sql_table *c, void *rows);

/* backing struct for this interface */
typedef struct store_functions {

	bind_col_fptr bind_col;
	bind_idx_fptr bind_idx;
	bind_del_fptr bind_del;

	append_col_fptr append_col;
	append_idx_fptr append_idx;
	update_col_fptr update_col;
	update_idx_fptr update_idx;
	delete_tab_fptr delete_tab;

	create_col_fptr create_col;
	create_idx_fptr create_idx;
	create_del_fptr create_del;
	
	dup_col_fptr dup_col;
	dup_idx_fptr dup_idx;
	dup_del_fptr dup_del;

	destroy_col_fptr destroy_col;
	destroy_idx_fptr destroy_idx;
	destroy_del_fptr destroy_del;

	clear_col_fptr clear_col;
	clear_idx_fptr clear_idx;
	clear_del_fptr clear_del;

	update_table_fptr update_table;

	col_ins_fptr col_ins;
	col_upd_fptr col_upd;

	idx_ins_fptr idx_ins;
	idx_upd_fptr idx_upd;

	del_fptr del;
} store_functions;

extern store_functions store_funcs;

typedef int (*logger_create_fptr) (char *logdir, char *dbname, int catalog_version);

typedef void (*logger_destroy_fptr) (void);
typedef int (*logger_restart_fptr) (void);
typedef int (*logger_cleanup_fptr) (void);

typedef int (*logger_changes_fptr)(void);
typedef int (*logger_get_sequence_fptr) (int seq, lng *id);

typedef int (*log_isnew_fptr)(void);
typedef int (*log_tstart_fptr) (void);
typedef int (*log_tend_fptr) (void);
typedef int (*log_sequence_fptr) (int seq, lng id);

typedef struct logger_functions {
	logger_create_fptr create;
	logger_destroy_fptr destroy;
	logger_restart_fptr restart;
	logger_cleanup_fptr cleanup;

	logger_changes_fptr changes;
	logger_get_sequence_fptr get_sequence;

	log_isnew_fptr log_isnew;
	log_tstart_fptr log_tstart;
	log_tend_fptr log_tend;
	log_sequence_fptr log_sequence;
} logger_functions;

extern logger_functions logger_funcs;

/* we need to add an interface for result_tables later */

extern res_table *res_table_create(sql_trans *tr, int res_id, int nr_cols, int querytype, res_table *next, void *order);
extern res_col *res_col_create(sql_trans *tr, res_table *t, char *tn, char *name, char *typename, int digits, int scale, int mtype, void *v);

extern void res_table_destroy(res_table *t);

extern res_table *res_tables_remove(res_table *results, res_table *t);
extern void res_tables_destroy(res_table *results);
extern res_table *res_tables_find(res_table *results, int res_id);

extern int
 store_init(int debug, store_type store, char *logdir, char *dbname, backend_stack stk);
extern void
 store_exit(void);

extern void
 store_manager(void);

extern void store_lock(void);
extern void store_unlock(void);
extern int store_next_oid(void);

extern sql_trans *sql_trans_create(backend_stack stk, sql_trans *parent, char *name);
extern sql_trans *sql_trans_destroy(sql_trans *tr);
extern int sql_trans_validate(sql_trans *tr);
extern int sql_trans_commit(sql_trans *tr);

extern sql_type *sql_trans_create_type(sql_trans *tr, sql_schema * s, char *sqlname, int digits, int scale, int radix, char *impl);

extern sql_func *sql_trans_create_func(sql_trans *tr, sql_schema * s, char *func, list *args, sql_subtype *res, bit sql, bit aggr, char *mod, char *impl, int is_func);

extern void sql_trans_drop_func(sql_trans *tr, sql_schema *s, int id, int drop_action);
extern void sql_trans_drop_all_func(sql_trans *tr, sql_schema *s, list *list_func, int drop_action);

extern void reset_functions(sql_trans *tr);

extern sql_schema *sql_trans_create_schema(sql_trans *tr, char *name, int auth_id, int owner);
extern void sql_trans_drop_schema(sql_trans *tr, int id, int drop_action);

extern sql_table *create_sql_table(char *name, sht type, bit system, int persistence, int commit_action);
extern sql_table *sql_trans_create_table(sql_trans *tr, sql_schema *s, char *name, bit system, int persistence, int commit_action, int sz);
extern sql_table *sql_trans_create_view(sql_trans *tr, sql_schema *s, char *name, char *sql, bit system);
extern sql_table *sql_trans_create_generated(sql_trans *tr, sql_schema *s, char *name, char *sql, bit system);

extern void sql_trans_drop_table(sql_trans *tr, sql_schema *s, int id, int drop_action);
extern size_t sql_trans_clear_table(sql_trans *tr, sql_table *t);

extern sql_column *create_sql_column(sql_table *t, char *nme, sql_subtype *tpe);
extern sql_column *sql_trans_create_column(sql_trans *tr, sql_table *t, char *name, sql_subtype *tpe);
extern void sql_trans_drop_column(sql_trans *tr, sql_table *t, int id, int drop_action);
extern sql_column *sql_trans_alter_null(sql_trans *tr, sql_column *col, int isnull);
extern sql_column *sql_trans_alter_default(sql_trans *tr, sql_column *col, char *val);

extern sql_key *sql_trans_create_ukey(sql_trans *tr, sql_table *t, char *name, key_type kt);
extern sql_fkey *sql_trans_create_fkey(sql_trans *tr, sql_table *t, char *name, key_type kt, sql_key *rkey, int on_delete, int on_update);
extern sql_key *sql_trans_create_kc(sql_trans *tr, sql_key *k, sql_column *c /*, extra options such as trunc */ );
extern sql_fkey *sql_trans_create_fkc(sql_trans *tr, sql_fkey *k, sql_column *c /*, extra options such as trunc */ );
extern void sql_trans_drop_key(sql_trans *tr, sql_schema *s, int id, int drop_action);

extern sql_idx *sql_trans_create_idx(sql_trans *tr, sql_table *t, char *name, idx_type it);
extern sql_idx *sql_trans_create_ic(sql_trans *tr, sql_idx * i, sql_column *c /*, extra options such as trunc */ );
extern void sql_trans_drop_idx(sql_trans *tr, sql_schema *s, int id, int drop_action);

extern sql_trigger * sql_trans_create_trigger(sql_trans *tr, sql_table *t, char *name, sht time, sht orientation, sht event, char *old_name, char *new_name, char *condition, char *statement );
extern sql_trigger * sql_trans_create_tc(sql_trans *tr, sql_trigger * i, sql_column *c /*, extra options such as trunc */ );
extern void sql_trans_drop_trigger(sql_trans *tr, sql_schema *s, int id, int drop_action);

extern sql_sequence * sql_trans_create_sequence(sql_trans *tr, sql_schema *s, char *name, lng start, lng min, lng max, lng inc, lng cacheinc, bit cycle );
extern void sql_trans_drop_sequence(sql_trans *tr, sql_schema *s, char *name, int drop_action);
extern sql_sequence *sql_trans_alter_sequence(sql_trans *tr, sql_sequence *seq, lng min, lng max, lng inc, lng cache, lng cycle);
extern lng sql_trans_sequence_restart(sql_trans *tr, sql_sequence *seq, lng start);

extern sql_session * sql_session_create(backend_stack stk, int autocommit);
extern void sql_session_destroy(sql_session *s);
extern void sql_session_reset(sql_session *s, int autocommit);
extern int sql_trans_begin(sql_session *s);
extern void sql_trans_end(sql_session *s);

extern list* sql_trans_schema_user_dependencies(sql_trans *tr, int schema_id);
extern void sql_trans_create_dependency(sql_trans *tr, int id, int depend_id, short depend_type);
extern void sql_trans_drop_dependencies(sql_trans *tr, int depend_id);
extern list* sql_trans_get_dependencies(sql_trans *tr, int id, short depend_type, list *ignore_ids);
extern int sql_trans_get_dependency_type(sql_trans *tr, int depend_id, short depend_type);
extern int sql_trans_check_dependency(sql_trans *tr, int id, int depend_id, short depend_type);
extern list* sql_trans_owner_schema_dependencies(sql_trans *tr, int id);

extern int sql_trans_connect_catalog(sql_trans *tr, char *server, int port, char *db, char *db_alias, char *user, char *passwd, char *lng);
extern int sql_trans_disconnect_catalog(sql_trans *tr, char *db_alias);
extern int sql_trans_disconnect_catalog_ALL(sql_trans *tr);
extern list *sql_trans_get_connection(sql_trans *tr,int id, char *server, char *db, char *db_alias, char *user);

#endif /*SQL_STORAGE_H */
