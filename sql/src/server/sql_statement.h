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
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2010 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _SQL_STATEMENT_H_
#define _SQL_STATEMENT_H_

#include "sql_mem.h"
#include "sql_types.h"
#include "sql_atom.h"
#include "sql_string.h"
#include "sql_mvc.h"

#define create_stmt_list() list_create((fdestroy)&stmt_destroy)

typedef union stmtdata {
	int ival;
	char *sval;
	struct atom *aval;
	struct list *lval;
	struct stmt *stval;
	struct group *gval;
	struct sql_column *cval;
	struct sql_key *kval;
	struct sql_idx *idxval;
	struct sql_table *tval;
	struct sql_schema *schema;
	sql_subtype typeval;
	struct sql_subaggr *aggrval;
	struct sql_subfunc *funcval;
} stmtdata;

typedef enum stmt_type {
	st_none,
	st_var,			/* use and/or declare variable */

	st_basetable,	
	st_table,		/* some functions return a table */
	st_temp,		/* temporal bat */
	st_single,		/* single value bat */
	st_rs_column,
	st_column,		/* relational column result */
	st_bat,
	st_dbat,
	st_idxbat,
	st_const,
	st_mark,
	st_gen_group,
	st_reverse,
	st_mirror,

	st_limit,
	st_order,
	st_reorder,

	st_ordered,
	st_output,
	st_affected_rows,

	st_atom,
	st_select,
	st_select2,
	st_selectN,
	st_uselect,
	st_uselect2,
	st_uselectN,
	st_semijoin,
	st_relselect,

	st_releqjoin,
	st_join,
	st_join2,
	st_joinN,
	st_outerjoin,
	st_diff,
	st_union,
	st_reljoin,

	st_export,
	st_append,
	st_table_clear,
	st_exception,
	st_trans,
	st_catalog,

	st_append_col,
	st_append_idx,
	st_update_col,
	st_update_idx,
	st_delete,

	st_group_ext,
	st_group,
	st_derive,
	st_unique,
	st_convert,
	st_unop,
	st_binop,
	st_Nop,
	st_aggr,

	st_alias,

	st_connection,		/*To handle support sql connections*/

	/* used internally only */
	st_list,

	/* flow control statements */
	st_while,
	st_if,
	st_return,
	st_assign
} st_type;

typedef enum comp_type {
	cmp_gt = 0,
	cmp_gte = 1,
	cmp_lte = 2,
	cmp_lt = 3,
	cmp_equal = 4,
	cmp_notequal = 5,
	cmp_notlike = 6,
	cmp_like = 7,
	cmp_notilike = 8,
	cmp_ilike = 9,
	cmp_all = 10,
	cmp_or = 11,
	cmp_project = 12
} comp_type;

/* flag to indicate anti join/select */
#define ANTI 16
#define GRP_DONE 32

typedef struct stmt {
	sql_ref ref;

	st_type type;
	stmtdata op1;
	stmtdata op2;
	stmtdata op3;
	stmtdata op4;		/* only op4 will hold other types */

	char nrcols;
	char key;		/* key (aka all values are unique) */
	char aggr;		/* aggregated */

	int flag;

	int nr;			/* variable assignment */
	int nr2;		/* usage count */

	struct stmt *h;
	struct stmt *t;
	int optimized;
	struct stmt *rewritten;
} stmt;

typedef struct group {
	sql_ref ref;

	stmt *grp;
	stmt *ext;
} group;

extern const char *st_type2string(st_type type);

extern int stmt2dot(stmt *s, int i, char *fn);
extern int print_stmt(stmt *s, int *nr);
extern void Sprint(stmt *s);

extern stmt *stmt_none(void);

#define VAR_DECLARE 1
#define VAR_GLOBAL(f) ((f>>1)==1)
extern stmt *stmt_var(char *varname, sql_subtype *t, int declare, int level);
extern stmt *stmt_varnr(int nr, sql_subtype *t);

extern stmt *stmt_table(stmt *cols, int temp);
extern stmt *stmt_basetable(sql_table *t, char *tname);

#define isbasetable(s) (s->type == st_basetable && isTable(s->op1.tval))
#define basetable_table(s) s->op1.tval

extern stmt *stmt_column(stmt *i, stmt *basetable, sql_table *t);	/* relational column */
extern stmt *stmt_rs_column(stmt *result_set, stmt *v, sql_subtype *tpe);

extern stmt *stmt_bat(sql_column *c, stmt *basetable, int access );
extern stmt *stmt_delta_table_bat(sql_column *c, stmt *basetable, int access );
extern stmt *stmt_idxbat(sql_idx * i, int access);
extern stmt *stmt_delta_table_idxbat(sql_idx * i, int access);

extern stmt *stmt_append_col(sql_column *c, stmt *b);
extern stmt *stmt_append_idx(sql_idx *i, stmt *b);
extern stmt *stmt_update_col(sql_column *c, stmt *b);
extern stmt *stmt_update_idx(sql_idx *i, stmt *b);
extern stmt *stmt_delete(sql_table *t, stmt *b);

extern stmt *stmt_append(stmt *c, stmt *values);
extern stmt *stmt_table_clear(sql_table *t);
extern stmt *stmt_export(stmt *t, char *sep, char *rsep, char *ssep, char *null_string, stmt *file);
extern stmt *stmt_trans(int type, stmt *chain, stmt *name);
extern stmt *stmt_catalog(int type, stmt *name, stmt *auth, stmt *action);

extern stmt *stmt_temp(sql_subtype *t);
extern stmt *stmt_atom(atom *op1);
extern stmt *stmt_atom_string(char *s);
extern stmt *stmt_atom_clob(char *S);
extern stmt *stmt_atom_int(int i);
extern stmt *stmt_atom_wrd(wrd i);
extern stmt *stmt_atom_wrd_nil(void);
extern stmt *stmt_atom_lng(lng l);
extern stmt *stmt_bool(int b);
extern stmt *stmt_select(stmt *op1, stmt *op2, comp_type cmptype);
extern stmt *stmt_uselect(stmt *op1, stmt *op2, comp_type cmptype);
/* cmp
       0 ==   l <  x <  h
       1 ==   l <  x <= h
       2 ==   l <= x <  h
       3 ==   l <= x <= h
       */
extern stmt *stmt_select2(stmt *op1, stmt *op2, stmt *op3, int cmp);
extern stmt *stmt_uselect2(stmt *op1, stmt *op2, stmt *op3, int cmp);
extern stmt *stmt_selectN(stmt *l, stmt *r, sql_subfunc *op);
extern stmt *stmt_uselectN(stmt *l, stmt *r, sql_subfunc *op);
extern stmt *stmt_likeselect(stmt *op1, stmt *op2, stmt *op3, comp_type cmptype);

#define isEqJoin(j) \
	(j->type == st_join && (j->flag == cmp_equal || j->flag == cmp_project))

extern stmt *stmt_semijoin(stmt *op1, stmt *op2);

extern stmt *stmt_relselect_init(void);
extern void stmt_relselect_fill(stmt *relselect, stmt *select);

extern stmt *stmt_releqjoin_init(void);
extern void stmt_releqjoin_fill(stmt *releqjoin, stmt *lc, stmt *rc);
extern stmt *stmt_releqjoin1(list *joins);
extern stmt *stmt_releqjoin2(list *l1, list *l2);
extern stmt *stmt_join(stmt *op1, stmt *op2, comp_type cmptype);

/* generic join operator, with a left and right statement list */
extern stmt *stmt_joinN(stmt *l, stmt *r, sql_subfunc *op);

extern stmt *stmt_join2(stmt *l, stmt *ra, stmt *rb, int cmp);
extern stmt *stmt_project(stmt *op1, stmt *op2);
extern stmt *stmt_outerjoin(stmt *op1, stmt *op2, comp_type cmptype);
extern stmt *stmt_reljoin(stmt *op1, list *neqjoins);

extern stmt *stmt_diff(stmt *op1, stmt *op2);
extern stmt *stmt_union(stmt *op1, stmt *op2);
extern stmt *stmt_list(list *l);
extern void stmt_set_nrcols(stmt *s);
extern stmt *stmt_connection(int *id, char *server, int *port, char *db, char *db_alias, char *user, char *passwd, char *lang);

/* raise exception incase the condition (cond) holds, continue with stmt res */
extern stmt *stmt_exception(stmt *cond, char *errstr, int errcode);

extern stmt *stmt_const(stmt *s, stmt *val);

extern stmt *stmt_mark(stmt *s, int id);
extern stmt *stmt_mark_tail(stmt *s, int id);
extern stmt *stmt_gen_group(stmt *s); /* given a gid,cnt blowup to full groups */
extern stmt *stmt_reverse(stmt *s);
extern stmt *stmt_mirror(stmt *s);

#define LIMIT_DIRECTION(dir,order,before_project) \
		(dir<<2)+(before_project<<1)+(order)
extern stmt *stmt_limit(stmt *s, stmt *offset, stmt *limit, int direction);
extern stmt *stmt_limit2(stmt *sa, stmt *sb, stmt *offset, stmt *limit, int direction);
extern stmt *stmt_order(stmt *s, int direction);
extern stmt *stmt_reorder(stmt *s, stmt *t, int direction);

extern stmt *stmt_convert(stmt *v, sql_subtype *from, sql_subtype *to);
extern stmt *stmt_unop(stmt *op1, sql_subfunc *op);
extern stmt *stmt_binop(stmt *op1, stmt *op2, sql_subfunc *op);
extern stmt *stmt_Nop(stmt *ops, sql_subfunc *op);
extern stmt *stmt_aggr(stmt *op1, group *grp, sql_subaggr *op, int reduce);
extern stmt *stmt_aggr2(stmt *op1, stmt *op2, sql_subaggr *op);
extern stmt *stmt_unique(stmt *s, group *grp);

extern stmt *stmt_alias(stmt *op1, char *tname, char *name);

extern stmt *stmt_ordered(stmt *order, stmt *res);
extern stmt *stmt_output(stmt *l);
extern stmt *stmt_affected_rows(stmt *l);

/* flow control statements */
extern stmt *stmt_while(stmt *cond, stmt *whilestmts );
extern stmt *stmt_if(stmt *cond, stmt *ifstmts, stmt *elsestmts);
extern stmt *stmt_return(stmt *val, int nr_of_declared_tables);
extern stmt *stmt_assign(char *varname, stmt *val, int level);

extern sql_subtype *head_type(stmt *st);
extern sql_subtype *tail_type(stmt *st);
extern int stmt_has_null( stmt *s );

extern char *column_name(stmt *st);
extern char *table_name(stmt *st);
extern char *schema_name(stmt *st);

extern void cond_stmt_destroy(stmt *s);
extern void stmt_destroy(stmt *s);
extern stmt *stmt_dup(stmt *s);

/*Dependency control*/
extern list* stmt_list_dependencies(stmt *s, int depend_type);

extern group *grp_create(stmt *s, group *og);
extern void grp_done(group *g);
extern void grp_destroy(group *g);
extern group *grp_dup(group *g);

extern stmt *stmt_group(stmt *s);
extern stmt *stmt_derive(stmt *g, stmt *s);

extern stmt *const_column(stmt *val );

#endif /* _SQL_STATEMENT_H_ */

