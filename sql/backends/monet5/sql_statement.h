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
 * Copyright August 2008-2014 MonetDB B.V.
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
	struct stmt *stval;

	struct sql_column *cval;
	struct sql_idx *idxval;
	struct sql_table *tval;

	sql_subtype typeval;
	struct sql_subaggr *aggrval;
	struct sql_subfunc *funcval;
	sql_rel *rel;
} stmtdata;

typedef enum stmt_type {
	st_none,
	st_var,			/* use and/or declare variable */

	st_table,		/* some functions return a table */
	st_temp,		/* temporal bat */
	st_single,		/* single value bat */
	st_rs_column,
	st_tid,
	st_bat,
	st_idxbat,
	st_const,
	st_mark,
	st_gen_group,
	st_reverse,
	st_mirror,
	st_result,		/* get nth result of a statement */

	st_limit,
	st_limit2,
	st_sample,
	st_order,
	st_reorder,

	st_output,
	st_affected_rows,

	st_atom,
	st_uselect,
	st_uselect2,
	st_tunion,
	st_tdiff,
	st_tinter,

	st_join,
	st_join2,
	st_joinN,
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

	st_group,
	st_unique,
	st_convert,
	st_unop,
	st_binop,
	st_Nop,
	st_func,
	st_aggr,

	st_alias,

	/* used internally only */
	st_list,

	/* flow control statements */
	st_cond,
	st_control_end,
	st_return,
	st_assign
} st_type;

/* flag to indicate anti join/select */
#define SWAPPED 16
#define ANTI ANTISEL
#define GRP_DONE 32

typedef struct stmt {
	st_type type;
	struct stmt *op1;
	struct stmt *op2;
	struct stmt *op3;
	stmtdata op4;		/* only op4 will hold other types */

	char nrcols;
	char key;		/* key (aka all values are unique) */
	char aggr;		/* aggregated */

	int flag;

	int nr;			/* variable assignment */

	char *tname;
	char *cname;

	int optimized;
	struct stmt *rewritten;
} stmt;

extern int stmt_key(stmt *s);

extern stmt **stmt_array(sql_allocator *sa, stmt *s);
extern void print_stmts(sql_allocator *sa, stmt **stmts);
extern void print_tree(sql_allocator *sa, stmt *stmts);
extern void clear_stmts(stmt **stmts);

extern stmt *stmt_none(sql_allocator *sa);

#define VAR_DECLARE 1
#define VAR_GLOBAL(f) ((f>>1)==1)
extern stmt *stmt_var(sql_allocator *sa, char *varname, sql_subtype *t, int declare, int level);
extern stmt *stmt_varnr(sql_allocator *sa, int nr, sql_subtype *t);

extern stmt *stmt_table(sql_allocator *sa, stmt *cols, int temp);
extern stmt *stmt_rs_column(sql_allocator *sa, stmt *result_set, int i, sql_subtype *tpe);

extern stmt *stmt_bat(sql_allocator *sa, sql_column *c, int access);
extern stmt *stmt_idxbat(sql_allocator *sa, sql_idx *i, int access);
extern stmt *stmt_tid(sql_allocator *sa, sql_table *t);

extern stmt *stmt_append_col(sql_allocator *sa, sql_column *c, stmt *b);
extern stmt *stmt_append_idx(sql_allocator *sa, sql_idx *i, stmt *b);
extern stmt *stmt_update_col(sql_allocator *sa, sql_column *c, stmt *tids, stmt *upd);
extern stmt *stmt_update_idx(sql_allocator *sa, sql_idx *i, stmt *tids, stmt *upd);
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
extern stmt *stmt_atom_int(sql_allocator *sa, int i);
extern stmt *stmt_atom_wrd(sql_allocator *sa, wrd i);
extern stmt *stmt_atom_wrd_nil(sql_allocator *sa);
extern stmt *stmt_bool(sql_allocator *sa, int b);

extern stmt *stmt_uselect(sql_allocator *sa, stmt *op1, stmt *op2, comp_type cmptype, stmt *sub);
/* cmp
       0 ==   l <  x <  h
       1 ==   l <  x <= h
       2 ==   l <= x <  h
       3 ==   l <= x <= h
       */
extern stmt *stmt_uselect2(sql_allocator *sa, stmt *op1, stmt *op2, stmt *op3, int cmp, stmt *sub);
extern stmt *stmt_genselect(sql_allocator *sa, stmt *l, stmt *rops, sql_subfunc *f, stmt *sub);

extern stmt *stmt_tunion(sql_allocator *sa, stmt *op1, stmt *op2);
extern stmt *stmt_tdiff(sql_allocator *sa, stmt *op1, stmt *op2);
extern stmt *stmt_tinter(sql_allocator *sa, stmt *op1, stmt *op2);

extern stmt *stmt_join(sql_allocator *sa, stmt *op1, stmt *op2, comp_type cmptype);
extern stmt *stmt_join2(sql_allocator *sa, stmt *l, stmt *ra, stmt *rb, int cmp, int swapped);
/* generic join operator, with a left and right statement list */
extern stmt *stmt_joinN(sql_allocator *sa, stmt *l, stmt *r, stmt *opt, sql_subfunc *op, int swapped);

extern stmt *stmt_project(sql_allocator *sa, stmt *op1, stmt *op2);
extern stmt *stmt_project_delta(sql_allocator *sa, stmt *col, stmt *upd, stmt *ins);
extern stmt *stmt_reorder_project(sql_allocator *sa, stmt *op1, stmt *op2);

extern stmt *stmt_diff(sql_allocator *sa, stmt *op1, stmt *op2);
extern stmt *stmt_union(sql_allocator *sa, stmt *op1, stmt *op2);
extern stmt *stmt_list(sql_allocator *sa, list *l);
extern void stmt_set_nrcols(stmt *s);

extern stmt *stmt_group(sql_allocator *sa, stmt *op1, stmt *grp, stmt *ext, stmt *cnt);
extern void stmt_group_done(stmt *grp);

/* raise exception incase the condition (cond) holds, continue with stmt res */
extern stmt *stmt_exception(sql_allocator *sa, stmt *cond, char *errstr, int errcode);

extern stmt *stmt_const(sql_allocator *sa, stmt *s, stmt *val);

extern stmt *stmt_mark_tail(sql_allocator *sa, stmt *s, oid id);
extern stmt *stmt_gen_group(sql_allocator *sa, stmt *gids, stmt *cnts);	/* given a gid,cnt blowup to full groups */
extern stmt *stmt_reverse(sql_allocator *sa, stmt *s);
extern stmt *stmt_mirror(sql_allocator *sa, stmt *s);
extern stmt *stmt_result(sql_allocator *sa, stmt *s, int nr);

#define LIMIT_DIRECTION(dir,order,before_project) \
		(dir<<2)+(before_project<<1)+(order)
extern stmt *stmt_limit(sql_allocator *sa, stmt *s, stmt *offset, stmt *limit, int direction);
extern stmt *stmt_limit2(sql_allocator *sa, stmt *s, stmt *sb, stmt *offset, stmt *limit, int direction);
extern stmt *stmt_sample(sql_allocator *sa, stmt *s, stmt *sample);
extern stmt *stmt_order(sql_allocator *sa, stmt *s, int direction);
extern stmt *stmt_reorder(sql_allocator *sa, stmt *s, int direction, stmt *orderby_ids, stmt *orderby_grp);

extern stmt *stmt_convert(sql_allocator *sa, stmt *v, sql_subtype *from, sql_subtype *to);
extern stmt *stmt_unop(sql_allocator *sa, stmt *op1, sql_subfunc *op);
extern stmt *stmt_binop(sql_allocator *sa, stmt *op1, stmt *op2, sql_subfunc *op);
extern stmt *stmt_Nop(sql_allocator *sa, stmt *ops, sql_subfunc *op);
extern stmt *stmt_func(sql_allocator *sa, stmt *ops, char *name, sql_rel *imp);
extern stmt *stmt_aggr(sql_allocator *sa, stmt *op1, stmt *grp, stmt *ext, sql_subaggr *op, int reduce, int no_nil);
extern stmt *stmt_unique(sql_allocator *sa, stmt *s, stmt *grp, stmt *ext, stmt *cnt);

extern stmt *stmt_alias(sql_allocator *sa, stmt *op1, char *tname, char *name);

extern stmt *stmt_output(sql_allocator *sa, stmt *l);
extern stmt *stmt_affected_rows(sql_allocator *sa, stmt *l);

/* flow control statements */
extern stmt *stmt_cond(sql_allocator *sa, stmt *cond, stmt *outer, int loop);
extern stmt *stmt_control_end(sql_allocator *sa, stmt *cond);
extern stmt *stmt_while(sql_allocator *sa, stmt *cond, stmt *whilestmts);
extern stmt *stmt_if(sql_allocator *sa, stmt *cond, stmt *ifstmts, stmt *elsestmts);
extern stmt *stmt_return(sql_allocator *sa, stmt *val, int nr_of_declared_tables);
extern stmt *stmt_assign(sql_allocator *sa, char *varname, stmt *val, int level);

extern sql_subtype *tail_type(stmt *st);
extern int stmt_has_null(stmt *s);

extern char *column_name(sql_allocator *sa, stmt *st);
extern char *table_name(sql_allocator *sa, stmt *st);
extern char *schema_name(sql_allocator *sa, stmt *st);

/*Dependency control*/
extern list *stmt_list_dependencies(sql_allocator *sa, stmt *s, int depend_type);

extern stmt *const_column(sql_allocator *sa, stmt *val);

#endif /* _SQL_STATEMENT_H_ */
