/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_mem.h"
#include "sql_stack.h"
#include "sql_statement.h"
#include <string.h>

static sql_subtype *
dup_subtype(sql_allocator *sa, sql_subtype *st)
{
	sql_subtype *res = SA_NEW(sa, sql_subtype);

	*res = *st;
	return res;
}

static sql_subfunc *
dup_subfunc(sql_allocator *sa, sql_subfunc *f)
{
	sql_subfunc *res = SA_NEW(sa, sql_subfunc);

	*res = *f;
	return res;
}

static sql_subaggr *
dup_subaggr(sql_allocator *sa, sql_subaggr *f)
{
	sql_subaggr *res = SA_NEW(sa, sql_subaggr);

	*res = *f;
	return res;
}

int
stmt_key(stmt *s)
{
	const char *nme = column_name(NULL, s);

	return hash_key(nme);
}

static const char *
st_type2string(st_type type)
{
	switch (type) {
#define ST(TYPE) case st_##TYPE : return #TYPE
		ST(none);
		ST(var);

		ST(table);
		ST(temp);
		ST(single);

		ST(rs_column);

		ST(tid);
		ST(bat);
		ST(idxbat);

		ST(const);

		ST(gen_group);
		ST(mirror);
		ST(result);

		ST(limit);
		ST(limit2);
		ST(sample);
		ST(order);
		ST(reorder);

		ST(output);
		ST(affected_rows);

		ST(atom);

		ST(uselect);
		ST(uselect2);
		ST(tunion);
		ST(tdiff);
		ST(tinter);

		ST(join);
		ST(join2);
		ST(joinN);

		ST(export);
		ST(append);
		ST(table_clear);
		ST(exception);
		ST(trans);
		ST(catalog);

		ST(append_col);
		ST(append_idx);
		ST(update_col);
		ST(update_idx);
		ST(delete);

		ST(group);

		ST(convert);
		ST(Nop);
		ST(func);
		ST(aggr);

		ST(alias);

		ST(list);

		ST(cond);
		ST(control_end);
		ST(return);
		ST(assign);
	default:
		return "unknown";	/* just needed for broken compilers ! */
	}
}

/* #TODO make proper traversal operations */
stmt *
stmt_atom_string(sql_allocator *sa, const char *S)
{
	const char *s = sql2str(sa_strdup(sa, S));
	sql_subtype t;

	sql_find_subtype(&t, "varchar", _strlen(s), 0);
	return stmt_atom(sa, atom_string(sa, &t, s));
}

stmt *
stmt_atom_string_nil(sql_allocator *sa)
{
	sql_subtype t;

	sql_find_subtype(&t, "clob", 0, 0);
	return stmt_atom(sa, atom_string(sa, &t, NULL));
}

stmt *
stmt_atom_int(sql_allocator *sa, int i)
{
	sql_subtype t;

	sql_find_subtype(&t, "int", 32, 0);
	return stmt_atom(sa, atom_int(sa, &t, i));
}

stmt *
stmt_atom_wrd(sql_allocator *sa, wrd i)
{
	sql_subtype t;

	if (sizeof(wrd) == sizeof(int))
		sql_find_subtype(&t, "wrd", 32, 0);
	else
		sql_find_subtype(&t, "wrd", 64, 0);
	return stmt_atom(sa, atom_int(sa, &t, i));
}

stmt *
stmt_atom_wrd_nil(sql_allocator *sa)
{
	sql_subtype t;

	if (sizeof(wrd) == sizeof(int))
		sql_find_subtype(&t, "wrd", 32, 0);
	else
		sql_find_subtype(&t, "wrd", 64, 0);
	return stmt_atom(sa, atom_general(sa, &t, NULL));
}

stmt *
stmt_bool(sql_allocator *sa, int b)
{
	sql_subtype t;

	sql_find_subtype(&t, "boolean", 0, 0);
	if (b) {
		return stmt_atom(sa, atom_bool(sa, &t, TRUE));
	} else {
		return stmt_atom(sa, atom_bool(sa, &t, FALSE));
	}
}

static stmt *
stmt_create(sql_allocator *sa, st_type type)
{
	stmt *s = SA_NEW(sa, stmt);

	s->type = type;
	s->op1 = NULL;
	s->op2 = NULL;
	s->op3 = NULL;
	s->op4.lval = NULL;
	s->flag = 0;
	s->nrcols = 0;
	s->key = 0;
	s->aggr = 0;
	s->nr = 0;
	s->optimized = -1;
	s->rewritten = NULL;
	s->tname = s->cname = NULL;
	return s;
}

stmt *
stmt_group(sql_allocator *sa, stmt *s, stmt *grp, stmt *ext, stmt *cnt)
{
	stmt *ns = stmt_create(sa, st_group);

	ns->op1 = s;

	if (grp) {
		ns->op2 = grp;
		ns->op3 = ext;
		ns->op4.stval = cnt;
	}
	ns->nrcols = s->nrcols;
	ns->key = 0;
	return ns;
}

void
stmt_group_done(stmt *grp)
{
	if (grp) {
		assert(grp->type == st_group);
		grp->flag = GRP_DONE;
	}
}

static void stmt_deps(list *dep_list, stmt *s, int depend_type, int dir);

static int
id_cmp(int *id1, int *id2)
{
	if (*id1 == *id2)
		return 0;
	return -1;
}

static list *
cond_append(list *l, int *id)
{
	if (!list_find(l, id, (fcmp) &id_cmp))
		 list_append(l, id);
	return l;
}

static void
list_deps(list *dep_list, list *l, int depend_type, int dir)
{
	if (l) {
		node *n;
		for (n = l->h; n; n = n->next)
			stmt_deps(dep_list, n->data, depend_type, dir);
	}
}

#define push(s) stack[top++] = s
#define pop()	stack[--top]
static void
stmt_deps(list *dep_list, stmt *s, int depend_type, int dir)
{
	stmt **stack;
	int top = 0, sz = 1024;

	stack = NEW_ARRAY(stmt *, sz + 1);
	if (stack == NULL)
		return;
	push(NULL);
	push(s);
	while ((s = pop()) != NULL) {
		if ((dir < 0 && s->optimized < 0) || (dir >= 0 && s->optimized >= 0)) {
			/* only add dependency once */
			if (dir < 0)
				s->optimized = 0;
			else
				s->optimized = -1;
			switch (s->type) {
			case st_list:
				list_deps(dep_list, s->op4.lval, depend_type, dir);
				break;
				/* simple case of statements of only statements */
			case st_alias:
			case st_tunion:
			case st_tdiff:
			case st_tinter:
			case st_join:
			case st_join2:
			case st_joinN:
			case st_append:
			case st_rs_column:

			case st_cond:
			case st_control_end:
			case st_return:
			case st_assign:
			case st_exception:
			case st_table:
			case st_export:
			case st_convert:
			case st_const:
			case st_gen_group:
			case st_mirror:
			case st_result:
			case st_limit:
			case st_limit2:
			case st_sample:
			case st_order:
			case st_reorder:
			case st_output:
			case st_affected_rows:

			case st_group:

			case st_uselect:
			case st_uselect2:
				if (s->op1)
					push(s->op1);
				if (s->op2)
					push(s->op2);
				if (s->op3)
					push(s->op3);
				break;

				/* special cases */
			case st_tid:
				if (depend_type == COLUMN_DEPENDENCY) {
					dep_list = cond_append(dep_list, &s->op4.tval->base.id);
				}
				break;
			case st_table_clear:
				if (depend_type == TRIGGER_DEPENDENCY) {
					dep_list = cond_append(dep_list, &s->op4.tval->base.id);
				}
				break;
			case st_bat:
			case st_append_col:
			case st_update_col:
				if (depend_type == COLUMN_DEPENDENCY) {
					if (isTable(s->op4.cval->t))
						dep_list = cond_append(dep_list, &s->op4.cval->base.id);
					dep_list = cond_append(dep_list, &s->op4.cval->t->base.id);
				}
				break;
			case st_aggr:
				if (s->op1)
					push(s->op1);
				if (s->op2)
					push(s->op2);
				if (s->op3)
					push(s->op3);
				if (depend_type == FUNC_DEPENDENCY) {
					dep_list = cond_append(dep_list, &s->op4.aggrval->aggr->base.id);
				}
				break;
			case st_Nop:
			case st_func:
				if (s->op1)
					push(s->op1);
				if (s->op2)
					push(s->op2);
				if (s->op3)
					push(s->op3);
				if (depend_type == FUNC_DEPENDENCY && s->type == st_Nop) {
					dep_list = cond_append(dep_list, &s->op4.funcval->func->base.id);
				}
				break;
				/* skip */
			case st_append_idx:
			case st_update_idx:
			case st_delete:
			case st_idxbat:
			case st_none:
			case st_var:
			case st_temp:
			case st_single:
			case st_atom:
			case st_trans:
			case st_catalog:
				break;
			}
		}
		if (top + 10 >= sz) {
			sz *= 2;
			stack = RENEW_ARRAY(stmt *, stack, sz);
		}
	}
	_DELETE(stack);
}

list *
stmt_list_dependencies(sql_allocator *sa, stmt *s, int depend_type)
{
	list *dep_list = sa_list(sa);

	stmt_deps(dep_list, s, depend_type, s->optimized);
	return dep_list;
}

stmt *
stmt_none(sql_allocator *sa)
{
	return stmt_create(sa, st_none);
}

stmt *
stmt_var(sql_allocator *sa, const char *varname, sql_subtype *t, int declare, int level)
{
	stmt *s = stmt_create(sa, st_var);

	s->op1 = stmt_atom_string(sa, varname);
	if (t)
		s->op4.typeval = *t;
	else
		s->op4.typeval.type = NULL;
	s->flag = declare + (level << 1);
	s->key = 1;
	return s;
}

stmt *
stmt_vars(sql_allocator *sa, const char *varname, sql_table *t, int declare, int level)
{
	stmt *s = stmt_create(sa, st_var);

	s->op1 = stmt_atom_string(sa, varname);
	s->op3 = (stmt*)t; /* ugh */
	s->flag = declare + (level << 1);
	s->key = 1;
	return s;
}

stmt *
stmt_varnr(sql_allocator *sa, int nr, sql_subtype *t)
{
	stmt *s = stmt_create(sa, st_var);

	s->op1 = NULL;
	if (t)
		s->op4.typeval = *t;
	else
		s->op4.typeval.type = NULL;
	s->flag = nr;
	s->key = 1;
	return s;
}

stmt *
stmt_table(sql_allocator *sa, stmt *cols, int temp)
{
	stmt *s = stmt_create(sa, st_table);

	s->op1 = cols;
	s->flag = temp;
	return s;
}

stmt *
stmt_temp(sql_allocator *sa, sql_subtype *t)
{
	stmt *s = stmt_create(sa, st_temp);

	s->op4.typeval = *t;
	s->nrcols = 1;
	return s;
}

stmt *
stmt_tid(sql_allocator *sa, sql_table *t)
{
	stmt *s = stmt_create(sa, st_tid);

	s->op4.tval = t;
	s->nrcols = 1;
	return s;
}

stmt *
stmt_bat(sql_allocator *sa, sql_column *c, int access)
{
	stmt *s = stmt_create(sa, st_bat);

	s->op4.cval = c;
	s->nrcols = 1;
	s->flag = access;
	return s;
}

stmt *
stmt_idxbat(sql_allocator *sa, sql_idx *i, int access)
{
	stmt *s = stmt_create(sa, st_idxbat);

	s->op4.idxval = i;
	s->nrcols = 1;
	s->flag = access;
	return s;
}

stmt *
stmt_append_col(sql_allocator *sa, sql_column *c, stmt *b)
{
	stmt *s = stmt_create(sa, st_append_col);

	s->op1 = b;
	s->op4.cval = c;
	return s;
}

stmt *
stmt_append_idx(sql_allocator *sa, sql_idx *i, stmt *b)
{
	stmt *s = stmt_create(sa, st_append_idx);

	s->op1 = b;
	s->op4.idxval = i;
	return s;
}

stmt *
stmt_update_col(sql_allocator *sa, sql_column *c, stmt *tids, stmt *upd)
{
	stmt *s = stmt_create(sa, st_update_col);

	assert(tids && upd);
	s->op1 = tids;
	s->op2 = upd;
	s->op4.cval = c;
	return s;
}

stmt *
stmt_update_idx(sql_allocator *sa, sql_idx *i, stmt *tids, stmt *upd)
{
	stmt *s = stmt_create(sa, st_update_idx);

	assert(tids && upd);
	s->op1 = tids;
	s->op2 = upd;
	s->op4.idxval = i;
	return s;
}

stmt *
stmt_delete(sql_allocator *sa, sql_table *t, stmt *tids)
{
	stmt *s = stmt_create(sa, st_delete);

	s->op1 = tids;
	s->op4.tval = t;
	return s;
}

static stmt *
stmt_const_(sql_allocator *sa, stmt *s, stmt *val)
{
	stmt *ns = stmt_create(sa, st_const);

	ns->op1 = s;
	ns->op2 = val;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->aggr = s->aggr;
	return ns;
}

/* some functions have side_effects, for example next_value_for. When these are
 * used in update statements we need to make sure we call these functions once
 * for every to be inserted value. 
 */
static stmt *
push_project(sql_allocator *sa, stmt *rows, stmt *val)
{
	node *n;
	stmt *l;

	switch (val->type) {
	case st_convert:
		val->op1 = push_project(sa, rows, val->op1);
		break;
	case st_Nop:
		if (val->op4.funcval->func->side_effect) {
			l = val->op1;
			n = l->op4.lval->h;
			if (n) {
				n->data = stmt_const_(sa, rows, n->data);
			} else {	
				l->op4.lval = list_append(sa_list(sa), stmt_const_(sa, rows, stmt_atom_int(sa, 0)));
			}
		} else {
			/* push through arguments of Nop */
			l = val->op1;
			for (n = l->op4.lval->h; n; n = n->next)
				n->data = push_project(sa, rows, n->data);
		}
		break;
	case st_func:
		/* push through arguments of func */
		l = val->op1;
		for (n = l->op4.lval->h; n; n = n->next)
			n->data = push_project(sa, rows, n->data);
		break;
	default:
		if (!val->nrcols)
			val = stmt_const_(sa, rows, val);
		return val;
	}
	val->nrcols = rows->nrcols;
	return val;
}

int
has_side_effect(stmt *val)
{
	int se = 0;

	switch (val->type) {
	case st_convert:
		se = has_side_effect(val->op1);
		break;
	case st_Nop:
		se = val->op4.funcval->func->side_effect;
		if (!se) {
			stmt *l = val->op1;
			node *n;
			for (n = l->op4.lval->h; n; n = n->next)
				se += has_side_effect(n->data);
		}
		break;
	default:
		return se;
	}
	return se;
}

stmt *
stmt_const(sql_allocator *sa, stmt *rows, stmt *val)
{
	if (val && has_side_effect(val)) {
		stmt *x = push_project(sa, rows, val);
		return x;
	} else {
		return stmt_const_(sa, rows, val);
	}
}

stmt *
stmt_gen_group(sql_allocator *sa, stmt *gids, stmt *cnts)
{
	stmt *ns = stmt_create(sa, st_gen_group);

	ns->op1 = gids;
	ns->op2 = cnts;

	ns->nrcols = gids->nrcols;
	ns->key = 0;
	ns->aggr = 0;
	return ns;
}

stmt *
stmt_mirror(sql_allocator *sa, stmt *s)
{
	stmt *ns = stmt_create(sa, st_mirror);

	ns->op1 = s;
	ns->nrcols = 2;
	ns->key = s->key;
	ns->aggr = s->aggr;
	return ns;
}

stmt *
stmt_result(sql_allocator *sa, stmt *s, int nr)
{
	stmt *ns;

	if (s->type == st_join && s->flag == cmp_joined) {
		if (nr)
			return s->op2;
		return s->op1;
	}
	ns = stmt_create(sa, st_result);
	ns->op1 = s;
	ns->flag = nr;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->aggr = s->aggr;
	return ns;
}


/* limit maybe atom nil */
stmt *
stmt_limit(sql_allocator *sa, stmt *c, stmt *offset, stmt *limit, int direction)
{
	stmt *ns = stmt_create(sa, st_limit);

	ns->op1 = c;
	ns->op2 = offset;
	ns->op3 = limit;
	ns->nrcols = c->nrcols;
	ns->key = c->key;
	ns->aggr = c->aggr;
	ns->flag = direction;
	return ns;
}

stmt *
stmt_limit2(sql_allocator *sa, stmt *c, stmt *piv, stmt *gid, stmt *offset, stmt *limit, int direction)
{
	stmt *ns = stmt_create(sa, st_limit2);

	ns->op1 = stmt_list(sa, list_append(list_append(list_append(sa_list(sa), c), piv), gid));
	ns->op2 = offset;
	ns->op3 = limit;
	ns->nrcols = piv->nrcols;
	ns->key = piv->key;
	ns->aggr = piv->aggr;
	ns->flag = direction;
	return ns;
}

stmt *
stmt_sample(sql_allocator *sa, stmt *s, stmt *sample)
{
	stmt *ns = stmt_create(sa, st_sample);

	ns->op1 = s;
	ns->op2 = sample;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->aggr = s->aggr;
	ns->flag = 0;
	return ns;
}


stmt *
stmt_order(sql_allocator *sa, stmt *s, int direction)
{
	stmt *ns = stmt_create(sa, st_order);

	ns->op1 = s;
	ns->flag = direction;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->aggr = s->aggr;
	return ns;
}

stmt *
stmt_reorder(sql_allocator *sa, stmt *s, int direction, stmt *orderby_ids, stmt *orderby_grp)
{
	stmt *ns = stmt_create(sa, st_reorder);

	ns->op1 = s;
	ns->op2 = orderby_ids;
	ns->op3 = orderby_grp;
	ns->flag = direction;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->aggr = s->aggr;
	return ns;
}

stmt *
stmt_atom(sql_allocator *sa, atom *op1)
{
	stmt *s = stmt_create(sa, st_atom);

	s->op4.aval = op1;
	s->key = 1;		/* values are also unique */
	return s;
}

stmt *
stmt_genselect(sql_allocator *sa, stmt *lops, stmt *rops, sql_subfunc *f, stmt *sub)
{
	stmt *s = stmt_create(sa, st_uselect);

	s->op1 = lops;
	s->op2 = rops;
	s->op3 = sub;
	s->op4.funcval = dup_subfunc(sa, f);
	s->flag = cmp_filter;
	s->nrcols = (lops->nrcols == 2) ? 2 : 1;
	return s;
}

stmt *
stmt_uselect(sql_allocator *sa, stmt *op1, stmt *op2, comp_type cmptype, stmt *sub)
{
	stmt *s = stmt_create(sa, st_uselect);

	s->op1 = op1;
	s->op2 = op2;
	s->op3 = sub;
	s->flag = cmptype;
	s->nrcols = (op1->nrcols == 2) ? 2 : 1;
	return s;
}

stmt *
stmt_uselect2(sql_allocator *sa, stmt *op1, stmt *op2, stmt *op3, int cmp, stmt *sub)
{
	stmt *s = stmt_create(sa, st_uselect2);

	s->op1 = op1;
	s->op2 = op2;
	s->op3 = op3;
	s->op4.stval = sub;
	s->flag = cmp;
	s->nrcols = (op1->nrcols == 2) ? 2 : 1;
	return s;
}

stmt *
stmt_tunion(sql_allocator *sa, stmt *op1, stmt *op2)
{
	stmt *s = stmt_create(sa, st_tunion);

	s->op1 = op1;
	s->op2 = op2;
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	s->aggr = op1->aggr;
	return s;
}

stmt *
stmt_tdiff(sql_allocator *sa, stmt *op1, stmt *op2)
{
	stmt *s = stmt_create(sa, st_tdiff);

	s->op1 = op1;
	s->op2 = op2;
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	s->aggr = op1->aggr;
	return s;
}

stmt *
stmt_tinter(sql_allocator *sa, stmt *op1, stmt *op2)
{
	stmt *s = stmt_create(sa, st_tinter);

	s->op1 = op1;
	s->op2 = op2;
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	s->aggr = op1->aggr;
	return s;
}

stmt *
stmt_join(sql_allocator *sa, stmt *op1, stmt *op2, comp_type cmptype)
{
	stmt *s = stmt_create(sa, st_join);

	s->op1 = op1;
	s->op2 = op2;
	s->flag = cmptype;
	s->key = op1->key;
	s->nrcols = 2;
	return s;
}

stmt *
stmt_project(sql_allocator *sa, stmt *op1, stmt *op2)
{
	return stmt_join(sa, op1, op2, cmp_project);
}

stmt *
stmt_project_delta(sql_allocator *sa, stmt *col, stmt *upd, stmt *ins)
{
	stmt *s = stmt_join(sa, col, upd, cmp_project);
	s->op3 = ins;
	return s;
}

stmt *
stmt_left_project(sql_allocator *sa, stmt *op1, stmt *op2, stmt *op3)
{
	stmt *s = stmt_join(sa, op1, op2, cmp_left_project);
	s->op3 = op3;
	return s;
}

stmt *
stmt_join2(sql_allocator *sa, stmt *l, stmt *ra, stmt *rb, int cmp, int swapped)
{
	stmt *s = stmt_create(sa, st_join2);

	s->op1 = l;
	s->op2 = ra;
	s->op3 = rb;
	s->flag = cmp;
	if (swapped)
		s->flag |= SWAPPED;
	s->nrcols = 2;
	return s;
}

stmt *
stmt_genjoin(sql_allocator *sa, stmt *l, stmt *r, sql_subfunc *op, int swapped)
{
	stmt *s = stmt_create(sa, st_joinN);

	s->op1 = l;
	s->op2 = r;
	s->op4.funcval = op;
	s->nrcols = 2;
	if (swapped)
		s->flag |= SWAPPED;
	return s;
}

stmt *
stmt_rs_column(sql_allocator *sa, stmt *rs, int i, sql_subtype *tpe)
{
	stmt *s = stmt_create(sa, st_rs_column);

	s->op1 = rs;
	s->op4.typeval = *tpe;
	s->flag = i;
	s->nrcols = 1;
	s->key = 0;
	return s;
}

stmt *
stmt_export(sql_allocator *sa, stmt *t, const char *sep, const char *rsep, const char *ssep, const char *null_string, stmt *file)
{
	stmt *s = stmt_create(sa, st_export);

	s->op1 = t;
	s->op2 = file;
	s->op4.lval = sa_list(sa);
	list_append(s->op4.lval, (char*)sep);
	list_append(s->op4.lval, (char*)rsep);
	list_append(s->op4.lval, (char*)ssep);
	list_append(s->op4.lval, (char*)null_string);
	return s;
}

stmt *
stmt_trans(sql_allocator *sa, int type, stmt *chain, stmt *name)
{
	stmt *s = stmt_create(sa, st_trans);

	s->op1 = chain;
	s->op2 = name;
	s->flag = type;
	return s;
}

stmt *
stmt_catalog(sql_allocator *sa, int type, stmt *args)
{
	stmt *s = stmt_create(sa, st_catalog);

	s->op1 = args;
	s->flag = type;
	return s;
}

void
stmt_set_nrcols(stmt *s)
{
	int nrcols = 0;
	int key = 1;
	node *n;
	list *l = s->op4.lval;

	assert(s->type == st_list);
	for (n = l->h; n; n = n->next) {
		stmt *f = n->data;

		if (!f)
			continue;
		if (f->nrcols > nrcols)
			nrcols = f->nrcols;
		key &= f->key;
	}
	s->nrcols = nrcols;
	s->key = key;
}

stmt *
stmt_list(sql_allocator *sa, list *l)
{
	stmt *s = stmt_create(sa, st_list);

	s->op4.lval = l;
	stmt_set_nrcols(s);
	return s;
}

stmt *
stmt_output(sql_allocator *sa, stmt *l)
{
	stmt *s = stmt_create(sa, st_output);

	s->op1 = l;
	return s;
}

stmt *
stmt_affected_rows(sql_allocator *sa, stmt *l)
{
	stmt *s = stmt_create(sa, st_affected_rows);

	s->op1 = l;
	return s;
}

stmt *
stmt_append(sql_allocator *sa, stmt *c, stmt *a)
{
	stmt *s = stmt_create(sa, st_append);

	s->op1 = c;
	s->op2 = a;
	s->nrcols = c->nrcols;
	s->key = c->key;
	return s;
}

stmt *
stmt_table_clear(sql_allocator *sa, sql_table *t)
{
	stmt *s = stmt_create(sa, st_table_clear);

	s->op4.tval = t;
	s->nrcols = 0;
	return s;
}

stmt *
stmt_exception(sql_allocator *sa, stmt *cond, char *errstr, int errcode)
{
	stmt *s = stmt_create(sa, st_exception);

	assert(cond);
	s->op1 = cond;
	s->op2 = stmt_atom_string(sa, errstr);
	s->op3 = stmt_atom_int(sa, errcode);
	s->nrcols = 0;
	return s;
}

stmt *
stmt_convert(sql_allocator *sa, stmt *v, sql_subtype *from, sql_subtype *to)
{
	stmt *s = stmt_create(sa, st_convert);
	list *l = sa_list(sa);

	from = dup_subtype(sa, from);
	to = dup_subtype(sa, to);
	list_append(l, from);
	list_append(l, to);
	s->op1 = v;
	s->op4.lval = l;
	s->nrcols = 0;	/* function without arguments returns single value */
	s->key = v->key;
	s->nrcols = v->nrcols;
	s->aggr = v->aggr;
	return s;
}

stmt *
stmt_unop(sql_allocator *sa, stmt *op1, sql_subfunc *op)
{
	list *ops = sa_list(sa);
	list_append(ops, op1);
	return stmt_Nop(sa, stmt_list(sa, ops), op);
}

stmt *
stmt_binop(sql_allocator *sa, stmt *op1, stmt *op2, sql_subfunc *op)
{
	list *ops = sa_list(sa);
	list_append(ops, op1);
	list_append(ops, op2);
	return stmt_Nop(sa, stmt_list(sa, ops), op);
}

stmt *
stmt_Nop(sql_allocator *sa, stmt *ops, sql_subfunc *op)
{
	node *n;
	stmt *o = NULL, *s = stmt_create(sa, st_Nop);

	s->op1 = ops;
	assert(op);
	s->op4.funcval = dup_subfunc(sa, op);
	if (list_length(ops->op4.lval)) {
		for (n = ops->op4.lval->h, o = n->data; n; n = n->next) {
			stmt *c = n->data;

			if (o->nrcols < c->nrcols)
				o = c;
		}
	}

	if (o) {
		s->nrcols = o->nrcols;
		s->key = o->key;
		s->aggr = o->aggr;
	} else {
		s->nrcols = 0;
		s->key = 1;
	}
	return s;
}

stmt *
stmt_func(sql_allocator *sa, stmt *ops, const char *name, sql_rel *rel)
{
	node *n;
	stmt *o = NULL, *s = stmt_create(sa, st_func);

	s->op1 = ops;
	s->op2 = stmt_atom_string(sa, name);
	s->op4.rel = rel;
	if (ops && list_length(ops->op4.lval)) {
		for (n = ops->op4.lval->h, o = n->data; n; n = n->next) {
			stmt *c = n->data;

			if (o->nrcols < c->nrcols)
				o = c;
		}
	}

	if (o) {
		s->nrcols = o->nrcols;
		s->key = o->key;
		s->aggr = o->aggr;
	} else {
		s->nrcols = 0;
		s->key = 1;
	}
	return s;
}

stmt *
stmt_aggr(sql_allocator *sa, stmt *op1, stmt *grp, stmt *ext, sql_subaggr *op, int reduce, int no_nil)
{
	stmt *s = stmt_create(sa, st_aggr);

	s->op1 = op1;
	if (grp) {
		s->op2 = grp;
		s->op3 = ext;
		s->nrcols = 1;
	} else {
		if (!reduce)
			s->nrcols = 1;
	}
	s->key = reduce;
	s->aggr = reduce;
	s->op4.aggrval = dup_subaggr(sa, op);
	s->flag = no_nil;
	return s;
}

stmt *
stmt_alias(sql_allocator *sa, stmt *op1, const char *tname, const char *alias)
{
	stmt *s = stmt_create(sa, st_alias);

	s->op1 = op1;
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	s->aggr = op1->aggr;

	s->tname = tname;
	s->cname = alias;
	return s;
}

sql_subtype *
tail_type(stmt *st)
{
	switch (st->type) {
	case st_const:
		return tail_type(st->op2);

	case st_uselect:
	case st_uselect2:
	case st_limit:
	case st_limit2:
	case st_sample:
	case st_tunion:
	case st_tdiff:
	case st_tinter:
	case st_append:
	case st_alias:
	case st_gen_group:
	case st_order:
		return tail_type(st->op1);

	case st_list:
		return tail_type(st->op4.lval->h->data);

	case st_bat:
		return &st->op4.cval->type;
	case st_idxbat:
		if (hash_index(st->op4.idxval->type)) {
			return sql_bind_localtype("wrd");
		} else if (st->op4.idxval->type == join_idx) {
			return sql_bind_localtype("oid");
		}
		/* fall through */
	case st_join:
	case st_join2:
	case st_joinN:
		if (st->flag == cmp_project)
			return tail_type(st->op2);
		/* fall through */
	case st_reorder:
	case st_group:
	case st_result:
	case st_tid:
	case st_mirror:
		return sql_bind_localtype("oid");
	case st_table_clear:
		return sql_bind_localtype("lng");

	case st_aggr: {
		list *res = st->op4.aggrval->res; 

		if (res && list_length(res) == 1)
			return res->h->data;
		
	} 	break;
	case st_Nop: {
		list *res = st->op4.funcval->res; 

		if (res && list_length(res) == 1)
			return res->h->data;
	} break;
	case st_atom:
		return atom_type(st->op4.aval);
	case st_convert:
		return st->op4.lval->t->data;
	case st_temp:
	case st_single:
	case st_rs_column:
		return &st->op4.typeval;
	case st_var:
		if (st->op4.typeval.type)
			return &st->op4.typeval;
		/* fall through */
	case st_exception:
		return NULL;
	case st_table:
		return sql_bind_localtype("bat");
	default:
		fprintf(stderr, "missing tail type %u: %s\n", st->type, st_type2string(st->type));
		assert(0);
		return NULL;
	}
	return NULL;
}

int
stmt_has_null(stmt *s)
{
	switch (s->type) {
	case st_aggr:
	case st_Nop:
	case st_uselect:
	case st_uselect2:
	case st_atom:
		return 0;
	case st_join:
		return stmt_has_null(s->op2);
	case st_bat:
		return s->op4.cval->null;

	default:
		return 1;
	}
}

static const char *
func_name(sql_allocator *sa, const char *n1, const char *n2)
{
	int l1 = _strlen(n1), l2;

	if (!sa)
		return n1;
	if (!n2)
		return sa_strdup(sa, n1);
	l2 = _strlen(n2);

	if (l2 > 16) {		/* only support short names */
		char *ns = SA_NEW_ARRAY(sa, char, l2 + 1);

		strncpy(ns, n2, l2);
		ns[l2] = 0;
		return ns;
	} else {
		char *ns = SA_NEW_ARRAY(sa, char, l1 + l2 + 2), *s = ns;

		strncpy(ns, n1, l1);
		ns += l1;
		*ns++ = '_';
		strncpy(ns, n2, l2);
		ns += l2;
		*ns = '\0';
		return s;
	}
}

const char *_column_name(sql_allocator *sa, stmt *st);

const char *
column_name(sql_allocator *sa, stmt *st)
{
	if (!st->cname)
		st->cname = _column_name(sa, st);
	return st->cname;
}

const char *
_column_name(sql_allocator *sa, stmt *st)
{
	switch (st->type) {
	case st_order:
	case st_reorder:
		return column_name(sa, st->op1);
	case st_const:
	case st_join:
	case st_join2:
	case st_joinN:
		return column_name(sa, st->op2);

	case st_mirror:
	case st_group:
	case st_result:
	case st_append:
	case st_gen_group:
	case st_uselect:
	case st_uselect2:
	case st_limit:
	case st_limit2:
	case st_sample:
	case st_tunion:
	case st_tdiff:
	case st_tinter:
	case st_convert:
		return column_name(sa, st->op1);
	case st_Nop:
	{
		const char *cn = column_name(sa, st->op1);
		return func_name(sa, st->op4.funcval->func->base.name, cn);
	}
	case st_aggr:
	{
		const char *cn = column_name(sa, st->op1);
		return func_name(sa, st->op4.aggrval->aggr->base.name, cn);
	}
	case st_alias:
		return column_name(sa, st->op3);
	case st_bat:
		return st->op4.cval->base.name;
	case st_atom:
		if (st->op4.aval->data.vtype == TYPE_str)
			return atom2string(sa, st->op4.aval);
		/* fall through */
	case st_var:
	case st_temp:
	case st_single:
		if (sa)
			return sa_strdup(sa, "single_value");
		return "single_value";

	case st_list:
		if (list_length(st->op4.lval))
			return column_name(sa, st->op4.lval->h->data);
		/* fall through */
	case st_rs_column:
		return NULL;
	default:
		fprintf(stderr, "missing column name %u: %s\n", st->type, st_type2string(st->type));
		return NULL;
	}
}

const char *_table_name(sql_allocator *sa, stmt *st);

const char *
table_name(sql_allocator *sa, stmt *st)
{
	if (!st->tname)
		st->tname = _table_name(sa, st);
	return st->tname;
}

const char *
_table_name(sql_allocator *sa, stmt *st)
{
	switch (st->type) {
	case st_const:
	case st_join:
	case st_join2:
	case st_joinN:
	case st_append:
		return table_name(sa, st->op2);
	case st_mirror:
	case st_group:
	case st_result:
	case st_gen_group:
	case st_uselect:
	case st_uselect2:
	case st_limit:
	case st_limit2:
	case st_sample:
	case st_tunion:
	case st_tdiff:
	case st_tinter:
	case st_aggr:
		return table_name(sa, st->op1);

	case st_table_clear:
		return st->op4.tval->base.name;
	case st_idxbat:
	case st_bat:
	case st_tid:
		return st->op4.cval->t->base.name;
	case st_alias:
		if (st->tname)
			return st->tname;
		else
			/* there are no table aliases, ie look into the base column */
			return table_name(sa, st->op1);
	case st_atom:
		if (st->op4.aval->data.vtype == TYPE_str && st->op4.aval->data.val.sval && _strlen(st->op4.aval->data.val.sval))
			return st->op4.aval->data.val.sval;
		return NULL;

	case st_list:
		if (list_length(st->op4.lval) && st->op4.lval->h)
			return table_name(sa, st->op4.lval->h->data);
		return NULL;

	case st_var:
	case st_temp:
	case st_single:
	default:
		return NULL;
	}
}

const char *
schema_name(sql_allocator *sa, stmt *st)
{
	switch (st->type) {
	case st_const:
	case st_join:
	case st_join2:
	case st_joinN:
		return schema_name(sa, st->op2);
	case st_mirror:
	case st_group:
	case st_result:
	case st_append:
	case st_gen_group:
	case st_uselect:
	case st_uselect2:
	case st_limit:
	case st_limit2:
	case st_sample:
	case st_tunion:
	case st_tdiff:
	case st_tinter:
	case st_convert:
	case st_Nop:
	case st_aggr:
		return schema_name(sa, st->op1);
	case st_alias:
		/* there are no schema aliases, ie look into the base column */
		return schema_name(sa, st->op1);
	case st_bat:
		return st->op4.cval->t->s->base.name;
	case st_atom:
		return NULL;
	case st_var:
	case st_temp:
	case st_single:
		return NULL;
	case st_list:
		if (list_length(st->op4.lval))
			return schema_name(sa, st->op4.lval->h->data);
		return NULL;
	default:
		return NULL;
	}
}

stmt *
stmt_while(sql_allocator *sa, stmt *cond, stmt *whilestmts)
{
	/* while is a if - block true with leave statement
	 * needed because the condition needs to be inside this outer block */
	list *l = sa_list(sa);
	stmt *cstmt, *wstmt;

	list_append(l, cstmt = stmt_cond(sa, stmt_bool(sa, 1), NULL, 0));
	list_append(l, cond);
	list_append(l, wstmt = stmt_cond(sa, cond, cstmt, 1));
	list_append(l, whilestmts);
	list_append(l, stmt_control_end(sa, wstmt));
	list_append(l, stmt_control_end(sa, cstmt));
	return stmt_list(sa, l);
}

stmt *
stmt_cond(sql_allocator *sa, stmt *cond, stmt *outer, int loop /* 0 if, 1 while */ )
{
	stmt *s = stmt_create(sa, st_cond);

	s->op1 = cond;
	s->op2 = outer;
	s->flag = loop;
	return s;
}

stmt *
stmt_control_end(sql_allocator *sa, stmt *cond)
{
	stmt *s = stmt_create(sa, st_control_end);
	s->op1 = cond;
	return s;
}


stmt *
stmt_if(sql_allocator *sa, stmt *cond, stmt *ifstmts, stmt *elsestmts)
{
	list *l = sa_list(sa);
	stmt *cstmt;
	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *not = sql_bind_func(sa, NULL, "not", bt, NULL, F_FUNC);

	list_append(l, cstmt = stmt_cond(sa, cond, NULL, 0));
	list_append(l, ifstmts);
	list_append(l, stmt_control_end(sa, cstmt));
	if (elsestmts) {
		sql_subfunc *or = sql_bind_func(sa, NULL, "or", bt, bt, F_FUNC);
		sql_subfunc *isnull = sql_bind_func(sa, NULL, "isnull", bt, NULL, F_FUNC);
		cond = stmt_binop(sa, 
				stmt_unop(sa, cond, not),
				stmt_unop(sa, cond, isnull), or);
		list_append(l, cstmt = stmt_cond(sa, cond, NULL, 0));
		list_append(l, elsestmts);
		list_append(l, stmt_control_end(sa, cstmt));
	}
	return stmt_list(sa, l);
}

stmt *
stmt_return(sql_allocator *sa, stmt *val, int nr_declared_tables)
{
	stmt *s = stmt_create(sa, st_return);

	s->op1 = val;
	s->flag = nr_declared_tables;
	return s;
}

stmt *
stmt_assign(sql_allocator *sa, const char *varname, stmt *val, int level)
{
	stmt *s = stmt_create(sa, st_assign);

	s->op1 = stmt_atom_string(sa, sa_strdup(sa, varname));
	s->op2 = val;
	s->flag = (level << 1);
	return s;
}

stmt *
const_column(sql_allocator *sa, stmt *val)
{
	sql_subtype *ct = tail_type(val);
/*
	stmt *temp = stmt_temp(ct);
	return stmt_append(temp, val);
*/
	stmt *s = stmt_create(sa, st_single);

	s->op1 = val;
	s->op4.typeval = *ct;
	s->nrcols = 1;

	s->tname = val->tname;
	s->cname = val->cname;
	return s;
}

static void
stack_push_stmt(sql_stack *stk, stmt *s, int first)
{
	if (first && s->nr == 0) {
		sql_stack_push(stk, s);
	} else if (!first && s->nr == 0) {
		s->nr = -stk->top;
		sql_stack_push(stk, s);
	}
}

static void
stack_push_list(sql_stack *stk, list *l)
{
	int top;
	node *n;

	/* Push in reverse order */
	for (n = l->h; n; n = n->next)
		sql_stack_push(stk, NULL);
	for (n = l->h, top = stk->top; n; n = n->next)
		stk->values[--top] = n->data;
}

static void
stack_push_children(sql_stack *stk, stmt *s)
{
	switch (s->type) {
	case st_list:
		stack_push_list(stk, s->op4.lval);
		break;
	default:
		if ((s->type == st_uselect2 || s->type == st_group) && s->op4.stval)
			stack_push_stmt(stk, s->op4.stval, 1);
		if (s->op2) {
			if (s->op3)
				stack_push_stmt(stk, s->op3, 1);
			stack_push_stmt(stk, s->op2, 1);
		}
		if (s->op1)
			stack_push_stmt(stk, s->op1, 1);
	}
}

void
clear_stmts(stmt **stmts)
{
	int nr = 0;

	while (stmts[nr]) {
		stmt *s = stmts[nr++];
		s->nr = 0;
	}
}

stmt **
stmt_array(sql_allocator *sa, stmt *s)
{
	int sz = 1024, top = 0;
	stmt **res = SA_NEW_ARRAY(sa, stmt *, sz);
	sql_stack *stk = sql_stack_new(sa, sz);

	stack_push_stmt(stk, s, 1);
	while ((s = sql_stack_pop(stk)) != NULL) {
		/* not handled */
		if (s->nr == 0) {
			stack_push_stmt(stk, s, 0);
			stack_push_children(stk, s);
			/* push all children */
		} else if (s->nr < 0) {
			/* children are handled put in the array */
			s->nr = top;
			if (top + 10 >= sz) {
				size_t osz = sz;
				sz *= 2;
				res = SA_RENEW_ARRAY(sa, stmt *, res, sz, osz);
				assert(res != NULL);
			}
			res[top++] = s;
		} else if (s->nr > 0) {	/* ?? */
		}
	}
	res[top++] = NULL;	/* mark end */
	return res;
}

static void
print_stmt(sql_allocator *sa, stmt *s)
{
	switch (s->type) {
	case st_var:
		if (s->op1)
			printf("s%d := %s:%s\n", s->nr, s->op1->op4.aval->data.val.sval, s->op4.typeval.type->base.name);
		else
			printf("s%d := A%d:%s\n", s->nr, s->flag, s->op4.typeval.type->base.name);
		break;
	case st_atom:
		printf("s%d := '%s':%s\n", s->nr, atom2string(sa, s->op4.aval), s->op4.aval->tpe.type->base.name);
		break;
	case st_list:{
		node *n;
		printf("s%d := %s(", s->nr, st_type2string(s->type));
		for (n = s->op4.lval->h; n; n = n->next) {
			stmt *e = n->data;
			printf("s%d%s", e->nr, n->next ? ", " : "");
		}
		printf(");\n");
	} break;
	default:
		printf("s%d := %s(", s->nr, st_type2string(s->type));
		switch (s->type) {
		case st_temp:
		case st_single:
			printf("%s", s->op4.typeval.type->base.name);
			break;
		case st_rs_column:
			printf("%s, ", s->op4.typeval.type->base.name);
			break;
		case st_tid:
			printf("%s.%s.TID(), ", s->op4.tval->s->base.name, s->op4.tval->base.name);
			break;
		case st_bat:
		case st_append_col:
		case st_update_col:
			printf("%s.%s.%s, ", s->op4.cval->t->s->base.name, s->op4.cval->t->base.name, s->op4.cval->base.name);
			break;
		case st_idxbat:
		case st_append_idx:
		case st_update_idx:
			printf("%s.%s.%s, ", s->op4.idxval->t->s->base.name, s->op4.idxval->t->base.name, s->op4.idxval->base.name);
			break;
		case st_delete:
		case st_table_clear:
			printf("%s.%s, ", s->op4.tval->s->base.name, s->op4.tval->base.name);
			break;
		case st_convert:{
			sql_subtype *f = s->op4.lval->h->data;
			sql_subtype *t = s->op4.lval->t->data;
			printf("%s, %s", f->type->base.name, t->type->base.name);
		} break;
		case st_Nop:
			printf("%s, ", s->op4.funcval->func->base.name);
			break;
		case st_aggr:
			printf("%s, ", s->op4.aggrval->aggr->base.name);
			break;
		default:
			break;
		}
		if (s->op1)
			printf("s%d", s->op1->nr);
		if (s->op2)
			printf(", s%d", s->op2->nr);
		if (s->op3)
			printf(", s%d", s->op3->nr);
		printf(");\n");
		break;
	}
}

void
print_stmts(sql_allocator *sa, stmt **stmts)
{
	int nr = 0;

	while (stmts[nr]) {
		stmt *s = stmts[nr++];
		print_stmt(sa, s);
	}
}

void
print_tree(sql_allocator *sa, stmt *s)
{
	stmt **stmts = stmt_array(sa, s);

	print_stmts(sa, stmts);
	clear_stmts(stmts);
}
