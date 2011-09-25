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

#ifndef _SQL_STATEMENT_H_
#define _SQL_STATEMENT_H_

#include "sql_mem.h"
#include "sql_types.h"
#include "sql_atom.h"
#include "sql_string.h"
#include "sql_mvc.h"

typedef union stmtdata {
	struct atom *aval;
	struct list *lval;

	struct sql_column *cval;
	struct sql_idx *idxval;
	struct sql_table *tval;

	sql_subtype typeval;
	struct sql_subaggr *aggrval;
	struct sql_subfunc *funcval;
	struct group *grp;
} stmtdata;

typedef enum stmt_type {
	st_none,
	st_var,			/* use and/or declare variable */

	st_basetable,	
	st_table,		/* some functions return a table */
	st_temp,		/* temporal bat */
	st_single,		/* single value bat */
	st_rs_column,
	st_bat,
	st_dbat,
	st_idxbat,
	st_const,
	st_mark,
	st_gen_group,
	st_reverse,
	st_mirror,

	st_limit,
	st_limit2,
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
	st_cond,
	st_control_end,
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
	cmp_or = 10,
	cmp_in = 11,
	cmp_notin = 12,

	cmp_all = 13,		/* special case for crossproducts */
	cmp_project = 14	/* special case for projection joins */
} comp_type;

#define is_theta_exp(e) (e == cmp_gt || e == cmp_gte || e == cmp_lte ||\
		         e == cmp_lt || e == cmp_equal || e == cmp_notequal)
#define is_complex_exp(e) (e == cmp_or || e == cmp_in || e == cmp_notin)

/* flag to indicate anti join/select */
#define ANTI 16
#define GRP_DONE 32

typedef struct stmt {
	st_type type;
	struct stmt* op1;
	struct stmt* op2;
	struct stmt* op3;
	stmtdata op4;		/* only op4 will hold other types */

	char nrcols;
	char key;		/* key (aka all values are unique) */
	char aggr;		/* aggregated */

	int flag;

	int nr;			/* variable assignment */

	struct stmt *h;
	struct stmt *t;
	int optimized;
	struct stmt *rewritten;
} stmt;

typedef struct group {
	stmt *grp;
	stmt *ext;
} group;

extern const char *st_type2string(st_type type);

extern stmt **stmt_array(sql_allocator *sa, stmt *s);
extern void print_stmts( sql_allocator *sa, stmt ** stmts );
extern void print_tree( sql_allocator *sa, stmt * stmts );
extern void clear_stmts( stmt ** stmts );

extern stmt *stmt_none(sql_allocator *sa);

#define VAR_DECLARE 1
#define VAR_GLOBAL(f) ((f>>1)==1)
extern stmt *stmt_var(sql_allocator *sa, char *varname, sql_subtype *t, int declare, int level);
extern stmt *stmt_varnr(sql_allocator *sa, int nr, sql_subtype *t);

extern stmt *stmt_table(sql_allocator *sa, stmt *cols, int temp);
extern stmt *stmt_basetable(sql_allocator *sa, sql_table *t, char *tname);

#define isbasetable(s) (s->type == st_basetable && isTable(s->op1.tval))
#define basetable_table(s) s->op1.tval

extern stmt *stmt_rs_column(sql_allocator *sa, stmt *result_set, stmt *v, sql_subtype *tpe);

extern stmt *stmt_bat(sql_allocator *sa, sql_column *c, stmt *basetable, int access );
extern stmt *stmt_delta_table_bat(sql_allocator *sa, sql_column *c, stmt *basetable, int access );
extern stmt *stmt_idxbat(sql_allocator *sa, sql_idx * i, int access);
extern stmt *stmt_delta_table_idxbat(sql_allocator *sa, sql_idx * i, int access);

extern stmt *stmt_append_col(sql_allocator *sa, sql_column *c, stmt *b);
extern stmt *stmt_append_idx(sql_allocator *sa, sql_idx *i, stmt *b);
extern stmt *stmt_update_col(sql_allocator *sa, sql_column *c, stmt *b);
extern stmt *stmt_update_idx(sql_allocator *sa, sql_idx *i, stmt *b);
extern stmt *stmt_delete(sql_allocator *sa, sql_table *t, stmt *b);

extern stmt *stmt_append(sql_allocator *sa, stmt *c, stmt *values);
extern stmt *stmt_table_clear(sql_allocator *sa, sql_table *t);
extern stmt *stmt_export(sql_allocator *sa, stmt *t, char *sep, char *rsep, char *ssep, char *null_string, stmt *file);
extern stmt *stmt_trans(sql_allocator *sa, int type, stmt *chain, stmt *name);
extern stmt *stmt_catalog(sql_allocator *sa, int type, stmt *args);

extern stmt *stmt_temp(sql_allocator *sa, sql_subtype *t);
extern stmt *stmt_atom(sql_allocator *sa, atom *op1);
extern stmt *stmt_atom_string(sql_allocator *sa, char *s);
extern stmt *stmt_atom_string_nil(sql_allocator *sa);
extern stmt *stmt_atom_clob(sql_allocator *sa, char *S);
extern stmt *stmt_atom_int(sql_allocator *sa, int i);
extern stmt *stmt_atom_wrd(sql_allocator *sa, wrd i);
extern stmt *stmt_atom_wrd_nil(sql_allocator *sa);
extern stmt *stmt_atom_lng(sql_allocator *sa, lng l);
extern stmt *stmt_bool(sql_allocator *sa, int b);
extern stmt *stmt_select(sql_allocator *sa, stmt *op1, stmt *op2, comp_type cmptype);
extern stmt *stmt_uselect(sql_allocator *sa, stmt *op1, stmt *op2, comp_type cmptype);
/* cmp
       0 ==   l <  x <  h
       1 ==   l <  x <= h
       2 ==   l <= x <  h
       3 ==   l <= x <= h
       */
extern stmt *stmt_select2(sql_allocator *sa, stmt *op1, stmt *op2, stmt *op3, int cmp);
extern stmt *stmt_uselect2(sql_allocator *sa, stmt *op1, stmt *op2, stmt *op3, int cmp);
extern stmt *stmt_selectN(sql_allocator *sa, stmt *l, stmt *r, sql_subfunc *op);
extern stmt *stmt_uselectN(sql_allocator *sa, stmt *l, stmt *r, sql_subfunc *op);
extern stmt *stmt_likeselect(sql_allocator *sa, stmt *op1, stmt *op2, stmt *op3, comp_type cmptype);

#define isEqJoin(j) \
	(j->type == st_join && (j->flag == cmp_equal || j->flag == cmp_project))

extern stmt *stmt_semijoin(sql_allocator *sa, stmt *op1, stmt *op2);

extern stmt *stmt_relselect_init(sql_allocator *sa);
extern void stmt_relselect_fill(stmt *relselect, stmt *select);

extern stmt *stmt_releqjoin_init(sql_allocator *sa);
extern void stmt_releqjoin_fill(stmt *releqjoin, stmt *lc, stmt *rc);
extern stmt *stmt_releqjoin(sql_allocator *sa, list *joins);
extern stmt *stmt_join(sql_allocator *sa, stmt *op1, stmt *op2, comp_type cmptype);

/* generic join operator, with a left and right statement list */
extern stmt *stmt_joinN(sql_allocator *sa, stmt *l, stmt *r, sql_subfunc *op);

extern stmt *stmt_join2(sql_allocator *sa, stmt *l, stmt *ra, stmt *rb, int cmp);
extern stmt *stmt_project(sql_allocator *sa, stmt *op1, stmt *op2);
extern stmt *stmt_outerjoin(sql_allocator *sa, stmt *op1, stmt *op2, comp_type cmptype);

extern stmt *stmt_diff(sql_allocator *sa, stmt *op1, stmt *op2);
extern stmt *stmt_union(sql_allocator *sa, stmt *op1, stmt *op2);
extern stmt *stmt_list(sql_allocator *sa, list *l);
extern void stmt_set_nrcols(stmt *s);
extern stmt *stmt_connection(sql_allocator *sa, int *id, char *server, int *port, char *db, char *db_alias, char *user, char *passwd, char *lang);

/* raise exception incase the condition (cond) holds, continue with stmt res */
extern stmt *stmt_exception(sql_allocator *sa, stmt *cond, char *errstr, int errcode);

extern stmt *stmt_const(sql_allocator *sa, stmt *s, stmt *val);

extern stmt *stmt_mark(sql_allocator *sa, stmt *s, int id);
extern stmt *stmt_mark_tail(sql_allocator *sa, stmt *s, int id);
extern stmt *stmt_gen_group(sql_allocator *sa, stmt *s); /* given a gid,cnt blowup to full groups */
extern stmt *stmt_reverse(sql_allocator *sa, stmt *s);
extern stmt *stmt_mirror(sql_allocator *sa, stmt *s);

#define LIMIT_DIRECTION(dir,order,before_project) \
		(dir<<2)+(before_project<<1)+(order)
extern stmt *stmt_limit(sql_allocator *sa, stmt *s, stmt *offset, stmt *limit, int direction);
extern stmt *stmt_limit2(sql_allocator *sa, stmt *s, stmt *sb, stmt *offset, stmt *limit, int direction);
extern stmt *stmt_order(sql_allocator *sa, stmt *s, int direction);
extern stmt *stmt_reorder(sql_allocator *sa, stmt *s, stmt *t, int direction);

extern stmt *stmt_convert(sql_allocator *sa, stmt *v, sql_subtype *from, sql_subtype *to);
extern stmt *stmt_unop(sql_allocator *sa, stmt *op1, sql_subfunc *op);
extern stmt *stmt_binop(sql_allocator *sa, stmt *op1, stmt *op2, sql_subfunc *op);
extern stmt *stmt_Nop(sql_allocator *sa, stmt *ops, sql_subfunc *op);
extern stmt *stmt_aggr(sql_allocator *sa, stmt *op1, group *grp, sql_subaggr *op, int reduce);
extern stmt *stmt_aggr2(sql_allocator *sa, stmt *op1, stmt *op2, sql_subaggr *op);
extern stmt *stmt_unique(sql_allocator *sa, stmt *s, group *grp);

extern stmt *stmt_alias(sql_allocator *sa, stmt *op1, char *tname, char *name);

extern stmt *stmt_ordered(sql_allocator *sa, stmt *order, stmt *res);
extern stmt *stmt_output(sql_allocator *sa, stmt *l);
extern stmt *stmt_affected_rows(sql_allocator *sa, stmt *l);

/* flow control statements */
extern stmt *stmt_cond(sql_allocator *sa, stmt *cond, stmt *outer, int loop);
extern stmt *stmt_control_end(sql_allocator *sa, stmt *cond);
extern stmt *stmt_while(sql_allocator *sa, stmt *cond, stmt *whilestmts );
extern stmt *stmt_if(sql_allocator *sa, stmt *cond, stmt *ifstmts, stmt *elsestmts);
extern stmt *stmt_return(sql_allocator *sa, stmt *val, int nr_of_declared_tables);
extern stmt *stmt_assign(sql_allocator *sa, char *varname, stmt *val, int level);

extern sql_subtype *head_type(stmt *st);
extern sql_subtype *tail_type(stmt *st);
extern int stmt_has_null( stmt *s );

extern char *column_name(sql_allocator *sa, stmt *st);
extern char *table_name(sql_allocator *sa, stmt *st);
extern char *schema_name(sql_allocator *sa, stmt *st);

/*Dependency control*/
extern list* stmt_list_dependencies(sql_allocator *sa, stmt *s, int depend_type);

extern group *grp_create(sql_allocator *sa, stmt *s, group *og);
extern void grp_done(group *g);

extern stmt *const_column(sql_allocator *sa, stmt *val );

#endif /* _SQL_STATEMENT_H_ */

