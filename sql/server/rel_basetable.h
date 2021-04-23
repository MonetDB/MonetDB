/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _REL_BASETABLE_H_
#define _REL_BASETABLE_H_

#include "rel_rel.h"

#define is_updateble(rel) \
	(rel->op == op_basetable || \
	(rel->op == op_ddl && (rel->flag == ddl_create_table || rel->flag == ddl_alter_table)))

#define rel_base_table(r) ((sql_table*)r->l)

extern sql_table *rel_ddl_table_get(sql_rel *r);
extern sql_rel *rel_ddl_basetable_get(sql_rel *r);

extern sql_rel *rel_basetable(mvc *sql, sql_table *t, const char *tname);
extern void rel_base_disallow(sql_rel *r);		/* set flag too check per column access */
extern int rel_base_use(mvc *ql, sql_rel *rt, int nr);	/* return error on (read) access violation */
extern void rel_base_use_tid(mvc *sql, sql_rel *rt);
extern void rel_base_use_all(mvc *sql, sql_rel *rel);
extern char *rel_base_name(sql_rel *r);
extern char *rel_base_rename(sql_rel *r, char *name);

extern sql_exp * rel_base_bind_colnr( mvc *sql, sql_rel *rel, int nr);
extern sql_rel *rel_base_bind_column_( sql_rel *rel, const char *cname);
extern sql_exp *rel_base_bind_column( mvc *sql, sql_rel *rel, const char *cname, int no_tname);
extern sql_rel *rel_base_bind_column2_( sql_rel *rel, const char *tname, const char *cname);
extern sql_exp *rel_base_bind_column2( mvc *sql, sql_rel *rel, const char *tname, const char *cname);

extern list *rel_base_projection( mvc *sql, sql_rel *rel, int intern);
extern list *rel_base_project_all( mvc *sql, sql_rel *rel, char *tname); /* select * from t */
extern sql_rel *rel_base_add_columns( mvc *sql, sql_rel *r);
extern sql_rel *rewrite_basetable(mvc *sql, sql_rel *rel);

extern void rel_base_dump_exps( stream *fout, sql_rel *rel);
extern int rel_base_has_column_privileges( mvc *sql, sql_rel *rel);

extern void rel_base_set_mergetable( sql_rel *rel, sql_table *mt); /* keep parent merge table */
extern sql_table *rel_base_get_mergetable( sql_rel *rel);

#endif /* _REL_BASETABLE_H_ */
