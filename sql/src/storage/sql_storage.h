#ifndef SQL_STORAGE_H
#define SQL_STORAGE_H

#include "sql_catalog.h"

sqlbat_export ssize_t
 column_find_row(sql_trans *tr, sql_column *c, void *value, ...);

sqlbat_export void *column_find_value(sql_trans *tr, sql_column *c, oid rid);

sqlbat_export int
 column_update_value(sql_trans *tr, sql_column *c, oid rid, void *value);

sqlbat_export int
 table_insert(sql_trans *tr, sql_table *t, ...);

sqlbat_export int
 table_delete(sql_trans *tr, sql_table *t, oid rid);

sqlbat_export int
 store_init(int debug, char *logdir, char *dbname, backend_stack stk);
sqlbat_export void
 store_exit();

sqlbat_export void
 store_manager();

sqlbat_export void store_lock();
sqlbat_export void store_unlock();

sqlbat_export sql_trans *sql_trans_create(backend_stack stk, sql_trans *parent, char *name);
sqlbat_export sql_trans *sql_trans_destroy(sql_trans *tr);
sqlbat_export int sql_trans_validate(sql_trans *tr);
sqlbat_export int sql_trans_commit(sql_trans *tr);

sqlbat_export sql_module *sql_trans_create_module(sql_trans *tr, char *name);
sqlbat_export void sql_trans_drop_module(sql_trans *tr, char *name);

sqlbat_export sql_type *sql_trans_create_type(sql_trans *tr, sql_module * m, char *sqlname, int digits, int scale, int radix, char *impl);

sqlbat_export sql_schema *sql_trans_create_schema(sql_trans *tr, char *name, int auth_id);
sqlbat_export void sql_trans_drop_schema(sql_trans *tr, char *sname);

sqlbat_export sql_table *sql_trans_create_table(sql_trans *tr, sql_schema *s, char *name, bit system, bit persists, bit clear, int sz);
sqlbat_export sql_table *sql_trans_create_view(sql_trans *tr, sql_schema *s, char *name, char *sql, bit system, bit persists);
sqlbat_export void sql_trans_drop_table(sql_trans *tr, sql_schema *s, char *name, int cascade);
sqlbat_export size_t sql_trans_clear_table(sql_trans *tr, sql_table *t);

sqlbat_export sql_column *sql_trans_create_column(sql_trans *tr, sql_table *t, char *name, sql_subtype *tpe);
sqlbat_export void sql_trans_drop_column(sql_trans *tr, sql_table *t, char *name);
sqlbat_export sql_column *sql_trans_alter_null(sql_trans *tr, sql_column *col, int isnull);
sqlbat_export sql_column *sql_trans_alter_default(sql_trans *tr, sql_column *col, char *val);

sqlbat_export sql_key *sql_trans_create_key(sql_trans *tr, sql_table *t, char *name, key_type kt, sql_key *rkey);
sqlbat_export sql_key *sql_trans_create_kc(sql_trans *tr, sql_key *k, sql_column *c /*, extra options such as trunc */ );
sqlbat_export void sql_trans_drop_key(sql_trans *tr, sql_schema *s, char *name);

sqlbat_export sql_idx *sql_trans_create_idx(sql_trans *tr, sql_table *t, char *name, idx_type it);
sqlbat_export sql_idx *sql_trans_create_ic(sql_trans *tr, sql_idx * i, sql_column *c /*, extra options such as trunc */ );
sqlbat_export void sql_trans_drop_idx(sql_trans *tr, sql_schema *s, char *name);

#endif /*SQL_STORAGE_H */
