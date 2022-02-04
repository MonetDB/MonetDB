/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _SQL_STATEMENT_H_
#define _SQL_STATEMENT_H_

#include "sql_mem.h"
#include "sql_types.h"
#include "sql_atom.h"
#include "sql_string.h"
#include "sql_mvc.h"
#include "mal_backend.h"

typedef union stmtdata {
	struct atom *aval;
	struct list *lval;
	struct stmt *stval;

	struct sql_column *cval;
	struct sql_idx *idxval;
	struct sql_table *tval;

	sql_subtype typeval;
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
	st_gen_group,
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
	st_semijoin,

	st_export,
	st_claim,
	st_depend,
	st_predicate,
	st_append,
	st_append_bulk,
	st_replace,
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

	unsigned int
	 nrcols:2,
	 key:1,			/* key (aka all values are unique) */ // TODO make this thing a bool
	 aggr:1,		/* aggregated */
	 partition:1,	/* selected as mitosis candidate */
	 reduce:1,		/* used to reduce number of rows (also for joins) */
	 loop:1,		/* cond statement is looping */
	 argument_independence:1; /*list statement represents a list of function call arguments which are independent of the inner project relation. */

	struct stmt *cand;	/* optional candidate list */

	int flag;
	int nr;			/* variable assignment */

	const char *tname;
	const char *cname;
	InstrPtr q;
	list *extra;	/* used for merge statments, this will be cleaned out on the pushcands branch :) */
} stmt;

/* which MAL modules can push candidates */
#define can_push_cands(sel, mod, fimp) \
	(sel && (((strcmp(mod, "calc") == 0 && strcmp(fimp, "ifthenelse") != 0)) || \
			 strcmp(mod, "mmath") == 0 || \
			 strcmp(mod, "mtime") == 0 || \
			 strcmp(mod, "blob") == 0 || \
			 (strcmp(mod, "str") == 0 && batstr_func_has_candidates(fimp))))

extern int stmt_key(stmt *s);

extern stmt *stmt_none(backend *be);

extern stmt *stmt_var(backend *be, const char *sname, const char *varname, sql_subtype *t, int declare, int level);
extern stmt *stmt_vars(backend *be, const char *varname, sql_table *t, int declare, int level);
extern stmt *stmt_varnr(backend *be, int nr, sql_subtype *t);

extern stmt *stmt_table(backend *be, stmt *cols, int temp);
extern stmt *stmt_rs_column(backend *be, stmt *result_set, int i, sql_subtype *tpe);

extern stmt *stmt_bat(backend *be, sql_column *c, int access, int partition);
extern stmt *stmt_idxbat(backend *be, sql_idx *i, int access, int partition);
extern stmt *stmt_tid(backend *be, sql_table *t, int partition);

extern stmt *stmt_claim(backend *be, sql_table *t, stmt *cnt);
extern stmt *stmt_dependency_change(backend *be, sql_table *t, stmt *cnt);
extern stmt *stmt_column_predicate(backend *be, sql_column *c);
extern stmt *stmt_append_col(backend *be, sql_column *c, stmt *offset, stmt *b, int *mvc_var_update, int locked);
extern stmt *stmt_append_idx(backend *be, sql_idx *i, stmt *offset, stmt *b);
extern stmt *stmt_update_col(backend *be, sql_column *c, stmt *tids, stmt *upd);
extern stmt *stmt_update_idx(backend *be, sql_idx *i, stmt *tids, stmt *upd);
extern stmt *stmt_delete(backend *be, sql_table *t, stmt *b);

extern stmt *stmt_append(backend *be, stmt *c, stmt *values);
extern stmt *stmt_append_bulk(backend *be, stmt *c, list *l);
extern stmt *stmt_replace(backend *be, stmt *c, stmt *id, stmt *val);
extern stmt *stmt_table_clear(backend *be, sql_table *t, int restart_sequences);

extern stmt *stmt_export(backend *be, stmt *t, const char *sep, const char *rsep, const char *ssep, const char *null_string, int onclient, stmt *file);
extern stmt *stmt_trans(backend *b, int type, stmt *chain, stmt *name);
extern stmt *stmt_catalog(backend *be, int type, stmt *args);

extern stmt *stmt_temp(backend *be, sql_subtype *t);
extern stmt *stmt_atom(backend *be, atom *a);
extern stmt *stmt_atom_string(backend *be, const char *s);
extern stmt *stmt_atom_string_nil(backend *be);
extern stmt *stmt_atom_int(backend *be, int i);
extern stmt *stmt_atom_lng(backend *be, lng i);
extern stmt *stmt_atom_lng_nil(backend *be);
extern stmt *stmt_bool(backend *be, int b);

extern stmt *stmt_uselect(backend *be, stmt *op1, stmt *op2, comp_type cmptype, stmt *sub, int anti, int is_semantics);
/* cmp
       0 ==   l <  x <  h
       1 ==   l <  x <= h
       2 ==   l <= x <  h
       3 ==   l <= x <= h
       */
extern stmt *stmt_uselect2(backend *be, stmt *op1, stmt *op2, stmt *op3, int cmp, stmt *sub, int anti, int symmetric, int reduce);
extern stmt *stmt_genselect(backend *be, stmt *lops, stmt *rops, sql_subfunc *f, stmt *sub, int anti);

extern stmt *stmt_tunion(backend *be, stmt *op1, stmt *op2);
extern stmt *stmt_tdiff(backend *be, stmt *op1, stmt *op2, stmt *lcand);
extern stmt *stmt_tdiff2(backend *be, stmt *op1, stmt *op2, stmt *lcand);
extern stmt *stmt_tinter(backend *be, stmt *op1, stmt *op2, bool single);

extern stmt *stmt_join(backend *be, stmt *op1, stmt *op2, int anti, comp_type cmptype, int need_left, int is_semantics, bool single);
extern stmt *stmt_join2(backend *be, stmt *l, stmt *ra, stmt *rb, int cmp, int anti, int symmetric, int swapped);
/* generic join operator, with a left and right statement list */
extern stmt *stmt_genjoin(backend *be, stmt *l, stmt *r, sql_subfunc *op, int anti, int swapped);
extern stmt *stmt_semijoin(backend *be, stmt *l, stmt *r, stmt *lcand, stmt *rcand, int is_semantics, bool single);
extern stmt *stmt_join_cand(backend *be, stmt *l, stmt *r, stmt *lcand, stmt *rcand, int anti, comp_type cmptype, int need_left, int is_semantics, bool single);

extern stmt *stmt_project(backend *be, stmt *op1, stmt *op2);
extern stmt *stmt_project_delta(backend *be, stmt *col, stmt *upd);
extern stmt *stmt_left_project(backend *be, stmt *op1, stmt *op2, stmt *op3);
extern stmt *stmt_dict(backend *be, stmt *op1, stmt *op2);
extern stmt *stmt_for(backend *be, stmt *op1, stmt *minval);

extern stmt *stmt_list(backend *be, list *l);
extern void stmt_set_nrcols(stmt *s);

extern stmt *stmt_group(backend *be, stmt *op1, stmt *grp, stmt *ext, stmt *cnt, int done);
extern stmt *stmt_unique(backend *be, stmt *op1);

/* raise exception incase the condition (cond) holds, continue with stmt res */
extern stmt *stmt_exception(backend *be, stmt *cond, const char *errstr, int errcode);

extern stmt *stmt_const(backend *be, stmt *s, stmt *val);

extern stmt *stmt_gen_group(backend *be, stmt *gids, stmt *cnts);	/* given a gid,cnt blowup to full groups */
extern stmt *stmt_mirror(backend *be, stmt *s);
extern stmt *stmt_result(backend *be, stmt *s, int nr);

/*
 * distinct: compute topn on unique groups
 * dir:      direction of the ordering, ie 1 Ascending, 0 decending
 * last:     intermediate step or last step
 * order:    is order important or not (firstn vs slice)
 */
extern stmt *stmt_limit(backend *sa, stmt *c, stmt *piv, stmt *gid, stmt *offset, stmt *limit, int distinct, int dir, int nullslast, int last, int order);
extern stmt *stmt_sample(backend *be, stmt *s, stmt *sample, stmt *seed);
extern stmt *stmt_order(backend *be, stmt *s, int direction, int nullslast);
extern stmt *stmt_reorder(backend *be, stmt *s, int direction, int nullslast, stmt *orderby_ids, stmt *orderby_grp);

extern stmt *stmt_convert(backend *sa, stmt *v, stmt *sel, sql_subtype *from, sql_subtype *to);
extern stmt *stmt_unop(backend *be, stmt *op1, stmt *sel, sql_subfunc *op);
extern stmt *stmt_binop(backend *be, stmt *op1, stmt *op2, stmt *sel, sql_subfunc *op);
extern stmt *stmt_Nop(backend *be, stmt *ops, stmt *sel, sql_subfunc *op, stmt* rows);
extern stmt *stmt_func(backend *be, stmt *ops, const char *name, sql_rel *imp, int f_union);
extern stmt *stmt_direct_func(backend *be, InstrPtr q);
extern stmt *stmt_aggr(backend *be, stmt *op1, stmt *grp, stmt *ext, sql_subfunc *op, int reduce, int no_nil, int nil_if_empty);

extern stmt *stmt_alias(backend *be, stmt *op1, const char *tname, const char *name);

extern int stmt_output(backend *be, stmt *l);
extern int stmt_affected_rows(backend *be, int lastnr);

/* flow control statements */
extern stmt *stmt_cond(backend *be, stmt *cond, stmt *outer, int loop, int anti);
extern stmt *stmt_control_end(backend *be, stmt *cond);
extern stmt *stmt_return(backend *be, stmt *val, int nr_of_declared_tables);
extern stmt *stmt_assign(backend *be, const char *sname, const char *varname, stmt *val, int level);

extern sql_subtype *tail_type(stmt *st);
extern int stmt_has_null(stmt *s);

extern const char *column_name(sql_allocator *sa, stmt *st);
extern const char *table_name(sql_allocator *sa, stmt *st);
extern const char *schema_name(sql_allocator *sa, stmt *st);

extern stmt *const_column(backend *ba, stmt *val);
extern stmt *stmt_fetch(backend *ba, stmt *val);

#endif /* _SQL_STATEMENT_H_ */
