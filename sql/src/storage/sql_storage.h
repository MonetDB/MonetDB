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
 * Portions created by CWI are Copyright (C) 1997-2007 CWI.
 * All Rights Reserved.
 */

#ifndef SQL_STORAGE_H
#define SQL_STORAGE_H

#include "sql_catalog.h"

extern list* table_select_column( sql_trans *tr, sql_column *val, sql_column *key, void *key_value, ...);
extern list* table_select_column_multi_values( sql_trans *tr, sql_column *val, sql_column *key1 , void *key_value1, sql_column *key2,list *values);

extern ssize_t
 column_find_row(sql_trans *tr, sql_column *c, void *value, ...);

extern void *column_find_value(sql_trans *tr, sql_column *c, ssize_t rid);

extern int
 column_update_value(sql_trans *tr, sql_column *c, ssize_t rid, void *value);

extern int
 table_insert(sql_trans *tr, sql_table *t, ...);

extern int
 table_delete(sql_trans *tr, sql_table *t, ssize_t rid);

extern int
 table_dump(sql_trans *tr, sql_table *t);

extern int
 table_check(sql_trans *tr, sql_table *t);

extern int
 store_init(int debug, char *logdir, char *dbname, backend_stack stk);
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

extern sql_func *sql_trans_create_func(sql_trans *tr, sql_schema * s, char *func, list *args, sql_subtype *res, bit sql, bit aggr, char *mod, char *impl);

extern void sql_trans_drop_func(sql_trans *tr, sql_schema *s, char *name, int cascade);

extern sql_schema *sql_trans_create_schema(sql_trans *tr, char *name, int auth_id, int owner);
extern void sql_trans_drop_schema(sql_trans *tr, char *sname);

extern sql_table *create_sql_table(char *name, sht type, bit system, int persistence, int commit_action);
extern sql_table *sql_trans_create_table(sql_trans *tr, sql_schema *s, char *name, bit system, int persistence, int commit_action, int sz);
extern sql_table *sql_trans_create_view(sql_trans *tr, sql_schema *s, char *name, char *sql, bit system);
extern sql_table *sql_trans_create_generated(sql_trans *tr, sql_schema *s, char *name, char *sql, bit system);

extern void sql_trans_drop_table(sql_trans *tr, sql_schema *s, char *name, int cascade);
extern size_t sql_trans_clear_table(sql_trans *tr, sql_table *t);

extern sql_column *create_sql_column(sql_table *t, char *nme, sql_subtype *tpe);
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

extern sql_trigger * sql_trans_create_trigger(sql_trans *tr, sql_table *t, char *name, sht time, sht orientation, sht event, char *old_name, char *new_name, char *condition, char *statement );
extern sql_trigger * sql_trans_create_tc(sql_trans *tr, sql_trigger * i, sql_column *c /*, extra options such as trunc */ );
extern void sql_trans_drop_trigger(sql_trans *tr, sql_schema *s, char *name);

extern sql_sequence * sql_trans_create_sequence(sql_trans *tr, sql_schema *s, char *name, lng start, lng min, lng max, lng inc, lng cacheinc, bit cycle );
extern void sql_trans_drop_sequence(sql_trans *tr, sql_schema *s, char *name);
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
extern list* sql_trans_get_dependencies(sql_trans *tr, int id, short depend_type);
extern int sql_trans_check_dependency(sql_trans *tr, int id, int depend_id, short depend_type);
extern list* sql_trans_owner_schema_dependencies(sql_trans *tr, int id);

extern int sql_trans_connect_catalog(sql_trans *tr, char *server, int port, char *db, char *db_alias, char *user, char *passwd, char *lng);
extern int sql_trans_disconnect_catalog(sql_trans *tr, char *db_alias);
extern int sql_trans_disconnect_catalog_ALL(sql_trans *tr);
extern list *sql_trans_get_connection(sql_trans *tr,int id, char *server, char *db, char *db_alias, char *user);

#endif /*SQL_STORAGE_H */
