/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef SQL_STORAGE_H
#define SQL_STORAGE_H

#include "sql_catalog.h"
#include "store_sequence.h"

#define COLSIZE	1024

#define LOG_OK		0
#define LOG_ERR		(-1)
#define LOG_CONFLICT	(-2)

#define isTempTable(x)   ((x)->persistence!=SQL_PERSIST)
#define isGlobal(x)      ((x)->persistence!=SQL_LOCAL_TEMP && (x)->persistence!=SQL_DECLARED_TABLE)
#define isGlobalTemp(x)  ((x)->persistence==SQL_GLOBAL_TEMP)
#define isLocalTemp(x)   ((x)->persistence==SQL_LOCAL_TEMP)
#define isTempSchema(x)  (strcmp((x)->base.name, "tmp") == 0)
#define isDeclaredTable(x)  ((x)->persistence==SQL_DECLARED_TABLE)

typedef enum store_type {
	store_bat,	/* delta bats, ie multi user read/write */
	store_tst,
	store_mem
} store_type;


struct sqlstore;

/* builtin functions have ids less than this */
#define FUNC_OIDS 2000

/* relational interface */
typedef oid (*column_find_row_fptr)(sql_trans *tr, sql_column *c, const void *value, ...);
typedef void *(*column_find_value_fptr)(sql_trans *tr, sql_column *c, oid rid);
typedef sqlid (*column_find_sqlid_fptr)(sql_trans *tr, sql_column *c, oid rid);
typedef bte (*column_find_bte_fptr)(sql_trans *tr, sql_column *c, oid rid);
typedef sht (*column_find_sht_fptr)(sql_trans *tr, sql_column *c, oid rid);
typedef int (*column_find_int_fptr)(sql_trans *tr, sql_column *c, oid rid);
typedef lng (*column_find_lng_fptr)(sql_trans *tr, sql_column *c, oid rid);
typedef const char * (*column_find_string_start_fptr)(sql_trans *tr, sql_column *c, oid rid, ptr *cbat);
typedef void (*column_find_string_end_fptr)(ptr cbat);
typedef int (*column_update_value_fptr)(sql_trans *tr, sql_column *c, oid rid, void *value);
typedef int (*table_insert_fptr)(sql_trans *tr, sql_table *t, ...);
typedef int (*table_delete_fptr)(sql_trans *tr, sql_table *t, oid rid);

typedef res_table *(*table_orderby_fptr)(sql_trans *tr, sql_table *t,
		sql_column *jl, sql_column *jr,
		sql_column *jl2, sql_column *jr2 /* optional join(jl,jr,(jl2,jr2)) */, sql_column *o, ...);
typedef void *(*table_fetch_value_fptr)(res_table *rt, sql_column *c);
typedef void (*table_result_destroy_fptr)(res_table *rt);

typedef struct rids {
	BUN cur;
	void *data;
} rids;

typedef struct subrids {
	BUN pos;
	sqlid id;
	void *ids;
	void *rids;
} subrids;

/* returns table rids, for the given select ranges */
typedef rids *(*rids_select_fptr)( sql_trans *tr, sql_column *key, const void *key_value_low, const void *key_value_high, ...);

/* order rids by orderby_column values */
typedef rids *(*rids_orderby_fptr)( sql_trans *tr, rids *r, sql_column *orderby_col);

typedef rids *(*rids_join_fptr)( sql_trans *tr, rids *l, sql_column *lc, rids *r, sql_column *rc);
typedef rids *(*rids_semijoin_fptr)( sql_trans *tr, rids *l, sql_column *lc, rids *r, sql_column *rc);
typedef rids *(*rids_diff_fptr)( sql_trans *tr, rids *l, sql_column *lc, subrids *r, sql_column *rc);

/* return table rids from result of table_select, return (-1) when done */
typedef oid (*rids_next_fptr)(rids *r);

/* clean up the resources taken by the result of table_select */
typedef void (*rids_destroy_fptr)(rids *r);
typedef int (*rids_empty_fptr)(rids *r);

typedef subrids *(*subrids_create_fptr)( sql_trans *tr, rids *l, sql_column *jc1, sql_column *jc2, sql_column *obc);

/* return table rids from result of table_select, return (-1) when done */
typedef oid (*subrids_next_fptr)(subrids *r);
typedef sqlid (*subrids_nextid_fptr)(subrids *r);

/* clean up the resources taken by the result of table_select */
typedef void (*subrids_destroy_fptr)(subrids *r);

typedef struct table_functions {
	column_find_row_fptr column_find_row;
	column_find_value_fptr column_find_value;
	column_find_sqlid_fptr column_find_sqlid;
	column_find_bte_fptr column_find_bte;
	column_find_int_fptr column_find_int;
	column_find_lng_fptr column_find_lng;
	column_find_string_start_fptr column_find_string_start; /* this function returns a pointer to the heap, use it with care! */
	column_find_string_end_fptr column_find_string_end; /* don't forget to call this function to unfix the bat descriptor! */

	column_update_value_fptr column_update_value;
	table_insert_fptr table_insert;
	table_delete_fptr table_delete;
	table_orderby_fptr table_orderby;
	table_fetch_value_fptr table_fetch_value;
	table_result_destroy_fptr table_result_destroy;

	rids_select_fptr rids_select;
	rids_orderby_fptr rids_orderby;
	rids_join_fptr rids_join;
	rids_semijoin_fptr rids_semijoin;
	rids_next_fptr rids_next;
	rids_destroy_fptr rids_destroy;
	rids_empty_fptr rids_empty;

	subrids_create_fptr subrids_create;
	subrids_next_fptr subrids_next;
	subrids_nextid_fptr subrids_nextid;
	subrids_destroy_fptr subrids_destroy;
	rids_diff_fptr rids_diff;
} table_functions;

/* delta table setup (ie readonly col + ins + upd + del)
-- binds for column,idx (rdonly, inserts, updates) and delets
*/
typedef void *(*bind_col_fptr) (sql_trans *tr, sql_column *c, int access);
typedef int (*bind_updates_fptr) (sql_trans *tr, sql_column *c, BAT **ui, BAT **uv);
typedef int (*bind_updates_idx_fptr) (sql_trans *tr, sql_idx *c, BAT **ui, BAT **uv);
typedef void *(*bind_idx_fptr) (sql_trans *tr, sql_idx *i, int access);
typedef void *(*bind_cands_fptr) (sql_trans *tr, sql_table *t, int nr_of_parts, int part_nr);

typedef int (*append_col_fptr) (sql_trans *tr, sql_column *c, BUN offset, BAT *offsets, void *d, BUN cnt, int t);
typedef int (*append_idx_fptr) (sql_trans *tr, sql_idx *i, BUN offset, BAT *offsets, void *d, BUN cnt, int t);

typedef int (*update_col_fptr) (sql_trans *tr, sql_column *c, void *tids, void *d, int t);
typedef int (*update_idx_fptr) (sql_trans *tr, sql_idx *i, void *tids, void *d, int t);

typedef int (*delete_tab_fptr) (sql_trans *tr, sql_table *t, void *d, int tpe);
typedef int (*claim_tab_fptr) (sql_trans *tr, sql_table *t, size_t cnt, BUN *offset, BAT **offsets);
typedef int (*tab_validate_fptr) (sql_trans *tr, sql_table *t, int uncommitted);

/*
-- count number of rows in column (excluding the deletes)
-- check for sortedness
 */
typedef size_t (*count_del_fptr) (sql_trans *tr, sql_table *t, int access);
typedef size_t (*count_col_fptr) (sql_trans *tr, sql_column *c, int access);
typedef size_t (*count_idx_fptr) (sql_trans *tr, sql_idx *i, int access);
typedef size_t (*dcount_col_fptr) (sql_trans *tr, sql_column *c);
typedef int (*min_max_col_fptr) (sql_trans *tr, sql_column *c);
typedef int (*prop_col_fptr) (sql_trans *tr, sql_column *c);

/*
-- create the necessary storage resources for columns, indices and tables
-- returns LOG_OK, LOG_ERR
*/
typedef int (*create_col_fptr) (sql_trans *tr, sql_column *c);
typedef int (*create_idx_fptr) (sql_trans *tr, sql_idx *i);
typedef int (*create_del_fptr) (sql_trans *tr, sql_table *t);

typedef void *(*col_dup_fptr) (sql_column *c);
typedef void *(*idx_dup_fptr) (sql_idx *i);
typedef void *(*del_dup_fptr) (sql_table *t);

/*
-- upgrade the necessary storage resources for columns, indices and tables
-- needed for the upgrade of the logger structure (ie user tables are
-- now stored using the object ids, no longer the names). This allows
-- renames.
-- returns LOG_OK, LOG_ERR
*/
typedef int (*upgrade_col_fptr) (sql_trans *tr, sql_column *c);
typedef int (*upgrade_idx_fptr) (sql_trans *tr, sql_idx *i);
typedef int (*upgrade_del_fptr) (sql_trans *tr, sql_table *t);
typedef int (*swap_bats_fptr) (sql_trans *tr, sql_column *c, BAT *b);

/*
-- free the storage resources for columns, indices and tables
-- returns LOG_OK, LOG_ERR
*/
typedef int (*destroy_col_fptr) (struct sqlstore *store, sql_column *c);
typedef int (*destroy_idx_fptr) (struct sqlstore *store, sql_idx *i);
typedef int (*destroy_del_fptr) (struct sqlstore *store, sql_table *t);

/*
-- drop a persistent table, ie add to the list of changes
-- returns LOG_OK, LOG_ERR
*/
typedef int (*drop_col_fptr) (sql_trans *tr, sql_column *c);
typedef int (*drop_idx_fptr) (sql_trans *tr, sql_idx *i);
typedef int (*drop_del_fptr) (sql_trans *tr, sql_table *t);

typedef BUN (*clear_table_fptr) (sql_trans *tr, sql_table *t);

typedef enum storage_type {
	ST_DEFAULT = 0,
	ST_DICT,
	ST_FOR,
} storage_type;

typedef int (*col_compress_fptr) (sql_trans *tr, sql_column *c, storage_type st, BAT *offsets, BAT *vals);

/*
-- update_table rollforward the changes made from table ft to table tt
-- returns LOG_OK, LOG_ERR
*/
typedef int (*update_table_fptr) (sql_trans *tr, sql_table *ft, sql_table *tt);

/* backing struct for this interface */
typedef struct store_functions {

	bind_col_fptr bind_col;
	bind_updates_fptr bind_updates;
	bind_updates_idx_fptr bind_updates_idx;
	bind_idx_fptr bind_idx;
	bind_cands_fptr bind_cands;

	append_col_fptr append_col;
	append_idx_fptr append_idx;

	update_col_fptr update_col;
	update_idx_fptr update_idx;

	delete_tab_fptr delete_tab;
	claim_tab_fptr claim_tab;
	claim_tab_fptr key_claim_tab;
	tab_validate_fptr tab_validate;

	count_del_fptr count_del;
	count_col_fptr count_col;
	count_idx_fptr count_idx;
	dcount_col_fptr dcount_col;
	min_max_col_fptr min_max_col;
	prop_col_fptr sorted_col;
	prop_col_fptr unique_col;
	prop_col_fptr double_elim_col; /* varsize col with double elimination */

	col_dup_fptr col_dup;
	idx_dup_fptr idx_dup;
	del_dup_fptr del_dup;

	create_col_fptr create_col;
	create_idx_fptr create_idx;
	create_del_fptr create_del;

	destroy_col_fptr destroy_col;
	destroy_idx_fptr destroy_idx;
	destroy_del_fptr destroy_del;

	drop_col_fptr drop_col;
	drop_idx_fptr drop_idx;
	drop_del_fptr drop_del;

	clear_table_fptr clear_table;
	col_compress_fptr col_compress;

	upgrade_col_fptr upgrade_col;
	upgrade_idx_fptr upgrade_idx;
	upgrade_del_fptr upgrade_del;
	swap_bats_fptr swap_bats;
} store_functions;

typedef int (*logger_create_fptr) (struct sqlstore *store, int debug, const char *logdir, int catalog_version);

typedef void (*logger_destroy_fptr) (struct sqlstore *store);
typedef int (*logger_flush_fptr) (struct sqlstore *store, lng save_id);
typedef int (*logger_activate_fptr) (struct sqlstore *store);
typedef int (*logger_cleanup_fptr) (struct sqlstore *store);

typedef int (*logger_changes_fptr)(struct sqlstore *store);
typedef int (*logger_get_sequence_fptr) (struct sqlstore *store, int seq, lng *id);

typedef int (*log_isnew_fptr)(struct sqlstore *store);
typedef int (*log_tstart_fptr) (struct sqlstore *store, bool flush);
typedef int (*log_tend_fptr) (struct sqlstore *store);
typedef int (*log_tdone_fptr) (struct sqlstore *store, ulng commit_ts);
typedef lng (*log_save_id_fptr) (struct sqlstore *store);
typedef int (*log_sequence_fptr) (struct sqlstore *store, int seq, lng id);

/*
-- List which parts of which files must be included in a hot snapshot.
-- This is written to the given stream in the following format:
-- - The first line is the absolute path of the db dir. All other paths
--   are relative to this.
-- - Every other line is either "c %ld %s\n" or "w %ld %s\n". The long
--   is always the number of bytes to write. The %s is the relative path of the
--   destination. For "c" lines (copy), it is also the relative path of the
--   source file. For "w" lines (write), the %ld bytes to write follow directly
--   after the newline.
-- Using a stream (buffer) instead of a list data structure simplifies debugging
-- and avoids a lot of tiny allocations and pointer manipulations.
*/
typedef gdk_return (*logger_get_snapshot_files_fptr)(struct sqlstore *store, stream *plan);

typedef struct logger_functions {
	logger_create_fptr create;
	logger_destroy_fptr destroy;
	logger_flush_fptr flush;
	logger_activate_fptr activate;

	logger_changes_fptr changes;
	logger_get_sequence_fptr get_sequence;

	logger_get_snapshot_files_fptr get_snapshot_files;

	log_isnew_fptr log_isnew;
	log_tstart_fptr log_tstart;
	log_tend_fptr log_tend;
	log_tdone_fptr log_tdone;
	log_save_id_fptr log_save_id;
	log_sequence_fptr log_sequence;
} logger_functions;

/* we need to add an interface for result_tables later */

extern res_table *res_table_create(sql_trans *tr, int res_id, oid query_id, int nr_cols, mapi_query_t querytype, res_table *next, void *order);
extern res_col *res_col_create(sql_trans *tr, res_table *t, const char *tn, const char *name, const char *typename, int digits, int scale, char mtype, void *v, bool cache);

extern void res_table_destroy(res_table *t);

extern res_table *res_tables_remove(res_table *results, res_table *t);
sql_export void res_tables_destroy(res_table *results);
extern res_table *res_tables_find(res_table *results, int res_id);

extern struct sqlstore *store_init(int debug, store_type store, int readonly, int singleuser);
extern void store_exit(struct sqlstore *store);

extern void store_suspend_log(struct sqlstore *store);
extern void store_resume_log(struct sqlstore *store);
extern lng store_hot_snapshot(struct sqlstore *store, str tarfile);
extern lng store_hot_snapshot_to_stream(struct sqlstore *store, stream *s);

extern ulng store_function_counter(struct sqlstore *store);

extern ulng store_oldest(struct sqlstore *store);
extern ulng store_get_timestamp(struct sqlstore *store);
extern void store_manager(struct sqlstore *store);

extern void store_lock(struct sqlstore *store);
extern void store_unlock(struct sqlstore *store);
extern sqlid store_next_oid(struct sqlstore *store);

extern int store_readonly(struct sqlstore *store);

extern sql_trans *sql_trans_create(struct sqlstore *store, sql_trans *parent, const char *name);
extern sql_trans *sql_trans_destroy(sql_trans *tr);
//extern bool sql_trans_validate(sql_trans *tr);
extern int sql_trans_commit(sql_trans *tr);

extern int sql_trans_create_type(sql_trans *tr, sql_schema *s, const char *sqlname, unsigned int digits, unsigned int scale, int radix, const char *impl);
extern int sql_trans_drop_type(sql_trans *tr, sql_schema * s, sqlid id, int drop_action);

extern int sql_trans_create_func(sql_func **fres, sql_trans *tr, sql_schema *s, const char *func, list *args, list *res, sql_ftype type, sql_flang lang,
								 const char *mod, const char *impl, const char *query, bit varres, bit vararg, bit system, bit side_effect);

extern int sql_trans_drop_func(sql_trans *tr, sql_schema *s, sqlid id, int drop_action);
extern int sql_trans_drop_all_func(sql_trans *tr, sql_schema *s, list *list_func, int drop_action);

extern void sql_trans_update_tables(sql_trans *tr, sql_schema *s);
extern void sql_trans_update_schemas(sql_trans *tr);

extern int sql_trans_create_schema(sql_trans *tr, const char *name, sqlid auth_id, sqlid owner);
extern int sql_trans_rename_schema(sql_trans *tr, sqlid id, const char *new_name);
extern int sql_trans_drop_schema(sql_trans *tr, sqlid id, int drop_action);

sql_export int sql_trans_create_table(sql_table **tres, sql_trans *tr, sql_schema *s, const char *name, const char *sql, int tt, bit system, int persistence, int commit_action, int sz, bte properties);

extern int sql_trans_set_partition_table(sql_trans *tr, sql_table *t);
extern int sql_trans_add_table(sql_trans *tr, sql_table *mt, sql_table *pt);
extern int sql_trans_add_range_partition(sql_trans *tr, sql_table *mt, sql_table *pt, sql_subtype tpe, ptr min, ptr max, bit with_nills, int update, sql_part** err);
extern int sql_trans_add_value_partition(sql_trans *tr, sql_table *mt, sql_table *pt, sql_subtype tpe, list* vals, bit with_nills, int update, sql_part **err);

extern int sql_trans_rename_table(sql_trans *tr, sql_schema *s, sqlid id, const char *new_name);
extern int sql_trans_set_table_schema(sql_trans *tr, sqlid id, sql_schema *os, sql_schema *ns);
extern int sql_trans_del_table(sql_trans *tr, sql_table *mt, sql_table *pt, int drop_action);

extern int sql_trans_drop_table(sql_trans *tr, sql_schema *s, const char *name, int drop_action);
extern int sql_trans_drop_table_id(sql_trans *tr, sql_schema *s, sqlid id, int drop_action);
extern BUN sql_trans_clear_table(sql_trans *tr, sql_table *t);
extern int sql_trans_alter_access(sql_trans *tr, sql_table *t, sht access);

extern int sql_trans_create_column(sql_column **rcol, sql_trans *tr, sql_table *t, const char *name, sql_subtype *tpe);
extern int sql_trans_rename_column(sql_trans *tr, sql_table *t, sqlid id, const char *old_name, const char *new_name);
extern int sql_trans_drop_column(sql_trans *tr, sql_table *t, sqlid id, int drop_action);
extern int sql_trans_alter_null(sql_trans *tr, sql_column *col, int isnull);
extern int sql_trans_alter_default(sql_trans *tr, sql_column *col, char *val);
extern int sql_trans_alter_storage(sql_trans *tr, sql_column *col, char *storage);
extern int sql_trans_is_sorted(sql_trans *tr, sql_column *col);
extern int sql_trans_is_unique(sql_trans *tr, sql_column *col);
extern int sql_trans_is_duplicate_eliminated(sql_trans *tr, sql_column *col);
extern size_t sql_trans_dist_count(sql_trans *tr, sql_column *col);
extern int sql_trans_ranges(sql_trans *tr, sql_column *col, void **min, void **max);

extern void column_destroy(struct sqlstore *store, sql_column *c);
extern void idx_destroy(struct sqlstore *store, sql_idx * i);
extern void table_destroy(struct sqlstore *store, sql_table *t);

extern int sql_trans_create_ukey(sql_key **res, sql_trans *tr, sql_table *t, const char *name, key_type kt);
extern int sql_trans_key_done(sql_trans *tr, sql_key *k);

extern int sql_trans_create_fkey(sql_fkey **res, sql_trans *tr, sql_table *t, const char *name, key_type kt, sql_key *rkey, int on_delete, int on_update);
extern int sql_trans_create_kc(sql_trans *tr, sql_key *k, sql_column *c);
extern int sql_trans_create_fkc(sql_trans *tr, sql_fkey *fk, sql_column *c);
extern int sql_trans_drop_key(sql_trans *tr, sql_schema *s, sqlid id, int drop_action);

extern int sql_trans_create_idx(sql_idx **i, sql_trans *tr, sql_table *t, const char *name, idx_type it);
extern int sql_trans_create_ic(sql_trans *tr, sql_idx *i, sql_column *c);
extern int sql_trans_drop_idx(sql_trans *tr, sql_schema *s, sqlid id, int drop_action);

extern int sql_trans_create_trigger(sql_trigger **tres, sql_trans *tr, sql_table *t, const char *name,sht time, sht orientation, sht event, const char *old_name, const char *new_name,const char *condition, const char *statement);
extern int sql_trans_drop_trigger(sql_trans *tr, sql_schema *s, sqlid id, int drop_action);

extern sql_sequence *create_sql_sequence(struct sqlstore *store, sql_allocator *sa, sql_schema *s, const char *name, lng start, lng min, lng max, lng inc, lng cacheinc, bit cycle);
extern int sql_trans_create_sequence(sql_trans *tr, sql_schema *s, const char *name, lng start, lng min, lng max, lng inc, lng cacheinc, bit cycle, bit bedropped);
extern int sql_trans_drop_sequence(sql_trans *tr, sql_schema *s, sql_sequence *seq, int drop_action);
extern int sql_trans_alter_sequence(sql_trans *tr, sql_sequence *seq, lng min, lng max, lng inc, lng cache, bit cycle);
extern int sql_trans_sequence_restart(sql_trans *tr, sql_sequence *seq, lng start);

extern sql_session * sql_session_create(struct sqlstore *store, sql_allocator *sa, int autocommit);
extern void sql_session_destroy(sql_session *s);
extern int sql_session_reset(sql_session *s, int autocommit);
extern int sql_trans_begin(sql_session *s);
extern int sql_trans_end(sql_session *s, int commit /* rollback=0, or commit=1 temporaries */);

extern list* sql_trans_schema_user_dependencies(sql_trans *tr, sqlid schema_id);
extern int sql_trans_create_dependency(sql_trans *tr, sqlid id, sqlid depend_id, sql_dependency depend_type);
extern int sql_trans_drop_dependencies(sql_trans *tr, sqlid depend_id);
extern int sql_trans_drop_dependency(sql_trans *tr, sqlid id, sqlid depend_id, sql_dependency depend_type);
extern list* sql_trans_get_dependencies(sql_trans *tr, sqlid id, sql_dependency depend_type, list *ignore_ids);
extern int sql_trans_get_dependency_type(sql_trans *tr, sqlid depend_id, sql_dependency depend_type);
extern int sql_trans_check_dependency(sql_trans *tr, sqlid id, sqlid depend_id, sql_dependency depend_type);
extern list* sql_trans_owner_schema_dependencies(sql_trans *tr, sqlid id);

extern sql_table *create_sql_table(struct sqlstore *store, sql_allocator *sa, const char *name, sht type, bit system, int persistence, int commit_action, bit properties);
extern sql_column *create_sql_column(struct sqlstore *store, sql_allocator *sa, sql_table *t, const char *name, sql_subtype *tpe);
extern sql_key *create_sql_ukey(struct sqlstore *store, sql_allocator *sa, sql_table *t, const char *nme, key_type kt);
extern sql_fkey *create_sql_fkey(struct sqlstore *store, sql_allocator *sa, sql_table *t, const char *nme, key_type kt, sql_key *rkey, int on_delete, int on_update );
extern sql_key *create_sql_kc(struct sqlstore *store, sql_allocator *sa, sql_key *k, sql_column *c);
extern sql_key * key_create_done(sql_trans *tr, sql_allocator *sa, sql_key *k);

extern sql_idx *create_sql_idx(struct sqlstore *store, sql_allocator *sa, sql_table *t, const char *nme, idx_type it);
extern sql_idx *create_sql_ic(struct sqlstore *store, sql_allocator *sa, sql_idx *i, sql_column *c);
extern sql_idx *create_sql_idx_done(sql_trans *tr, sql_idx *i);
extern sql_func *create_sql_func(struct sqlstore *store, sql_allocator *sa, const char *func, list *args, list *res, sql_ftype type, sql_flang lang, const char *mod,
								 const char *impl, const char *query, bit varres, bit vararg, bit system, bit side_effect);

/* for alter we need to duplicate a table */
extern sql_table *dup_sql_table(sql_allocator *sa, sql_table *t);
extern void drop_sql_column(sql_table *t, sqlid id, int drop_action);
extern void drop_sql_idx(sql_table *t, sqlid id);
extern void drop_sql_key(sql_table *t, sqlid id, int drop_action);

extern int sql_trans_copy_column(sql_trans *tr, sql_table *t, sql_column *c, sql_column **cres);
extern int sql_trans_copy_key(sql_trans *tr, sql_table *t, sql_key *k, sql_key **kres);
extern int sql_trans_copy_idx(sql_trans *tr, sql_table *t, sql_idx *i, sql_idx **ires);
extern int sql_trans_copy_trigger(sql_trans *tr, sql_table *t, sql_trigger *tri, sql_trigger **tres);
extern sql_table *globaltmp_instantiate(sql_trans *tr, sql_table *t);

#define NR_TABLE_LOCKS 64
#define NR_COLUMN_LOCKS 512
#define TRANSACTION_ID_BASE	(1ULL<<63)

typedef struct sqlstore {
	int catalog_version;	/* software version of the catalog */
	sql_catalog *cat;		/* the catalog of persistent tables (what to do with tmp tables ?) */
	sql_schema *tmp;		/* keep pointer to default (empty) tmp schema */
	MT_Lock lock;			/* lock protecting concurrent writes (not reads, ie use rcu) */
	MT_Lock commit;			/* protect transactions, only single commit (one wal writer) */
	MT_Lock flush;			/* flush lock protecting concurrent writes (not reads, ie use rcu) */
	MT_Lock table_locks[NR_TABLE_LOCKS];		/* protecting concurrent writes to tables (storage) */
	MT_Lock column_locks[NR_COLUMN_LOCKS];		/* protecting concurrent writes to columns (storage) */
	list *active;			/* list of running transactions */

	ATOMIC_TYPE nr_active;	/* count number of transactions */
	ATOMIC_TYPE lastactive;	/* timestamp of last active client */
	ATOMIC_TYPE timestamp;	/* timestamp counter */
	ATOMIC_TYPE transaction;/* transaction id counter */
	ATOMIC_TYPE function_counter;/* function counter used during function instantiation */
	ulng oldest;
	ulng oldest_pending;
	int readonly;			/* store is readonly */
	int singleuser;			/* store is for a single user only (==1 enable, ==2 single user session running) */
	int first;				/* just created the db */
	int initialized;		/* used during bootstrap only */
	int debug;				/* debug mask */
	store_type active_type;
	list *changes;			/* pending changes to cleanup */
	sql_hash *dependencies; /* pending dependencies created to cleanup */
	sql_hash *depchanges;	/* pending dependencies changes to cleanup */
	list *seqchanges;		/* pending sequence number changes to be add to the first commiting transaction */
	sql_hash *sequences;	/* loaded store sequence numbers */

	sql_allocator *sa;		/* for now a store allocator, needs a special version with free operations (with reuse) */
	sqlid obj_id, prev_oid;

	store_functions storage_api;
	table_functions table_api;
	logger_functions logger_api;
	void *logger;			/* space to keep logging structure of storage backend */
} sqlstore;

typedef enum sql_dependency_change_type {
	ddl = 1,
	dml = 2
} sql_dependency_change_type;

typedef struct sql_dependency_change {
	sqlid objid; /* id of the object where the dependency was created */
	sql_dependency_change_type type; /* type of dependency */
	ulng ts; /* transaction timestamp of the dependency */
} sql_dependency_change;

typedef struct sql_change {
	sql_base *obj;
	ulng ts;        /* commit/rollback timestamp */
	void *data;	/* data changes */
	bool committed;	/* commit or rollback */
	bool handled;	/* handled in commit */
	tc_log_fptr log;		/* callback to log changes */
	tc_commit_fptr commit;	/* callback to commit or rollback the changes */
	tc_cleanup_fptr cleanup;/* callback to cleanup changes */
} sql_change;

extern void trans_add(sql_trans *tr, sql_base *b, void *data, tc_cleanup_fptr cleanup, tc_commit_fptr commit, tc_log_fptr log);
extern int tr_version_of_parent(sql_trans *tr, ulng ts);

extern int sql_trans_add_predicate(sql_trans* tr, sql_column *c, unsigned int cmp, atom *r, atom *f, bool anti, bool semantics);
extern int sql_trans_add_dependency(sql_trans* tr, sqlid id, sql_dependency_change_type tp);
sql_export int sql_trans_add_dependency_change(sql_trans *tr, sqlid id, sql_dependency_change_type tp);

/* later move intop dict.h on this level */
extern BAT *DICTenlarge(BAT *offsets, BUN cnt, BUN sz, role_t role);
extern BAT *DICTdecompress_(BAT *o, BAT *u, role_t role);
extern int DICTprepare4append(BAT **noffsets, BAT *vals, BAT *dict);
extern int DICTprepare4append_vals(void **noffsets, void *vals, BUN cnt, BAT *dict);

extern BAT *FORdecompress_(BAT *o, lng minval, int tt, role_t role);
extern int FORprepare4append(BAT **noffsets, BAT *vals, lng minval, int tt);
extern int FORprepare4append_vals(void **noffsets, void *vals, BUN cnt, lng minval, int vtype, int ft);

#endif /*SQL_STORAGE_H */
