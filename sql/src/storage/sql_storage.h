#ifndef SQL_STORAGE_H
#define SQL_STORAGE_H

#include "sql_catalog.h"

extern ssize_t
 column_find_row(sql_trans *tr, sql_column *c, void *value, ...);

extern void *column_find_value(sql_trans *tr, sql_column *c, oid rid);

extern int
 column_update_value(sql_trans *tr, sql_column *c, oid rid, void *value);

extern int
 table_insert(sql_trans *tr, sql_table *t, ...);

extern int
 table_delete(sql_trans *tr, sql_table *t, oid rid);

extern int
 store_init(int debug, char *logdir, char *dbname, backend_stack stk);
extern void
 store_exit();

extern void
 store_manager();

extern void store_lock();
extern void store_unlock();

extern sql_trans *sql_trans_create(backend_stack stk, sql_trans *parent, char *name);
extern sql_trans *sql_trans_destroy(sql_trans *tr);
extern int sql_trans_validate(sql_trans *tr);
extern int sql_trans_commit(sql_trans *tr);

extern sql_module *sql_trans_create_module(sql_trans *tr, char *name);
extern void sql_trans_drop_module(sql_trans *tr, char *name);

extern sql_type *sql_trans_create_type(sql_trans *tr, sql_module * m, char *sqlname, int digits, int scale, int radix, char *impl);

extern sql_schema *sql_trans_create_schema(sql_trans *tr, char *name, int auth_id);
extern void sql_trans_drop_schema(sql_trans *tr, char *sname);

extern sql_table *sql_trans_create_table(sql_trans *tr, sql_schema *s, char *name, bit system, bit persists, bit clear, int sz);
extern sql_table *sql_trans_create_view(sql_trans *tr, sql_schema *s, char *name, char *sql, bit system, bit persists);
extern void sql_trans_drop_table(sql_trans *tr, sql_schema *s, char *name, int cascade);
extern size_t sql_trans_clear_table(sql_trans *tr, sql_table *t);

extern sql_column *sql_trans_create_column(sql_trans *tr, sql_table *t, char *name, sql_subtype *tpe);
extern void sql_trans_drop_column(sql_trans *tr, sql_table *t, char *name);
extern sql_column *sql_trans_alter_null(sql_trans *tr, sql_column *col, int isnull);
extern sql_column *sql_trans_alter_default(sql_trans *tr, sql_column *col, char *val);

extern sql_key *sql_trans_create_key(sql_trans *tr, sql_table *t, char *name, key_type kt, sql_key *rkey);
extern sql_key *sql_trans_create_kc(sql_trans *tr, sql_key *k, sql_column *c /*, extra options such as trunc */ );
extern void sql_trans_drop_key(sql_trans *tr, sql_schema *s, char *name);

extern sql_idx *sql_trans_create_idx(sql_trans *tr, sql_table *t, char *name, idx_type it);
extern sql_idx *sql_trans_create_ic(sql_trans *tr, sql_idx * i, sql_column *c /*, extra options such as trunc */ );
extern void sql_trans_drop_idx(sql_trans *tr, sql_schema *s, char *name);

#endif /*SQL_STORAGE_H */
