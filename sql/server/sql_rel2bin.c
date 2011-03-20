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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */


#include "monetdb_config.h"
#include "sql_rel2bin.h"
#include "sql_env.h"
#include <stdio.h>
#include "rel_semantic.h"

static stmt * head_column(stmt *st);

static stmt *
tail_column(stmt *st)
{
	switch (st->type) {
	case st_join:
	case st_outerjoin:
	case st_reorder:
		return tail_column(st->op2);

	case st_join2:
	case st_joinN:
	case st_reljoin:
		return tail_column(st->op2);
	case st_releqjoin:
		return tail_column(st->op4.lval->h->data);

	case st_select:
	case st_select2:
	case st_selectN:
	case st_uselect:
	case st_uselect2:
	case st_uselectN:
	case st_semijoin:
	case st_limit:
	case st_limit2:

	case st_diff:
	case st_union:

	case st_append:

	case st_unique:
	case st_aggr:
	case st_order:

	case st_alias:

	case st_convert:

		return tail_column(st->op1);

	case st_table_clear:
	case st_bat:
		return st;

	case st_mirror:
	case st_reverse:
		return head_column(st->op1);
	case st_list:
		return tail_column(st->op4.lval->h->data);

		/* required for shrink_select_ranges() in sql_rel2bin.mx */
	case st_rs_column:
	case st_idxbat:

		/* required for eliminate_semijoin() in sql_rel2bin.mx */
	case st_relselect:

		/* some statements have no column coming from any basetable */
	case st_atom:
	case st_var:
	case st_mark:
	case st_gen_group:
	case st_derive:
	case st_group_ext:
	case st_group:
	case st_basetable:	/* a table is not a column */
	case st_table:		/* a table is not a column */
	case st_temp:
	case st_single:
	case st_unop:
	case st_binop:
	case st_Nop:
	case st_const:
		return NULL;

	default:
		fprintf(stderr, "missing tail column %u: %s\n", st->type, st_type2string(st->type));
		assert(0);
		return NULL;
	}
}

static stmt *
head_column(stmt *st)
{
	switch (st->type) {
	case st_const:
	case st_mark:
	case st_gen_group:
	case st_mirror:

	case st_select:
	case st_select2:
	case st_selectN:
	case st_uselect:
	case st_uselect2:
	case st_uselectN:
	case st_semijoin:
	case st_limit:
	case st_limit2:

	case st_join:
	case st_join2:
	case st_joinN:
	case st_outerjoin:
	case st_diff:
	case st_union:

	case st_append:

	case st_group_ext:
	case st_group:
	case st_unique:
	case st_unop:
	case st_binop:
	case st_Nop:
	case st_aggr:
	case st_order:

	case st_alias:

	case st_convert:

		return head_column(st->op1);

	case st_relselect:
	case st_releqjoin:
	case st_reljoin:
		if (st->op2)
			return head_column(st->op2);
		if (st->op1)
			return head_column(st->op1);
		return NULL;
	case st_table_clear:
	case st_bat:
		return st;

	case st_reverse:
		return tail_column(st->op1);

	case st_reorder:
	case st_derive:
		return tail_column(st->op2);

	case st_list:
		return head_column(st->op4.lval->h->data);

		/* required for eliminate_semijoin() in sql_rel2bin.mx */
		/* st_temp has no head column coming from any basetable */
	case st_atom:
	case st_var:
	case st_basetable:
	case st_table:
	case st_temp:
	case st_single:
	case st_idxbat:

	case st_rs_column:
		return NULL;

		/* stmts without head column which also are not expected */
	case st_none:

	case st_dbat:

	case st_append_col:
	case st_update_col:
	case st_append_idx:
	case st_update_idx:
	case st_delete:

	case st_ordered:
	case st_output:
	case st_affected_rows:

	case st_export:
	case st_exception:
	case st_trans:
	case st_catalog:

	case st_cond:
	case st_control_end:
	case st_return:
	case st_assign:
	case st_connection:
		fprintf(stderr, "missing head column %u: %s\n", st->type, st_type2string(st->type));
		assert(0);
		return NULL;
	}
	return NULL;
}

/* basecolum is used by eliminate_semijoins and shrink_ranges to reduce the
 * number of semijoins and selects.
 * There we need too know which columns come from the same basecolumn.
 */
sql_column *
basecolumn(stmt *st)
{
	if (!st)
		return NULL;	/* required for shrink_select_ranges() and eliminate_semijoin() */
	switch (st->type) {
	case st_reverse:
		return basecolumn(head_column(st->op1));

	case st_bat:
		return st->op4.cval;

	default:
		return basecolumn(tail_column(st));
	}
}


static int
cmp_sel2(stmt *sel, st_type *tpe)
{
	(void) tpe;		/* Stefan: unsed, but required for list_select() / type fcmp() */
	if (sel->type == st_select2 || sel->type == st_uselect2) {
		/* for now we don't split anti-selects */
		if ((sel->flag&ANTI) == 0)
			return 0;
	}
	return -1;
}

static int
cmp_sel_oneCol(stmt *sel)
{
	int rtrn = -1;

	if (sel->type >= st_select && sel->type <= st_uselect2) {
		if (sel->op2->nrcols == 0) {
			rtrn = 0;
		}
#ifndef NDEBUG
/*
	} else {
		if (sel->type == st_reverse) {
			printf("= TODO: sql_rel2bin: shrink_select_ranges: handle %s(%s)!\n", st_type2string(sel->type), st_type2string(sel->op1->type));
		} else {
			printf("= TODO: sql_rel2bin: shrink_select_ranges: handle %s!\n", st_type2string(sel->type));
		}
*/
#endif
	}
	return rtrn;
}

static int
cmp_sel_twoCol(stmt *sel)
{
	return !cmp_sel_oneCol(sel);
}

static int
cmp_sel_hasNoBasecol(stmt *sel)
{
	int rtrn = -1;

	if (basecolumn(sel) == NULL) {
		rtrn = 0;
	}
	return rtrn;
}

static int
cmp_sel_hasBasecol(stmt *sel)
{
	int rtrn = -1;

	if (basecolumn(sel) != NULL) {
		rtrn = 0;
	}
	return rtrn;
}

static int
cmp_sel_basecol(stmt *sel, stmt *key)
{
	if (basecolumn(key) == basecolumn(sel)) {
		return 0;
	}
	return -1;
}

static int
cmp_sel_val(stmt *sel, stmt *key)
{
	if (sel->op2 == key->op2) {
		return 0;
	}
	return -1;
}

static int
cmp_sel_comp_type(stmt *sel, comp_type *flg)
{
	if ((sel->type == st_select || sel->type == st_uselect) && (comp_type) sel->flag == *flg) {
		return 0;
	}
	return -1;
}

static stmt *
stmt_min(sql_allocator *sa, stmt *x, stmt *y)
{
	sql_subtype *t;
	sql_subfunc *f;

	assert(x->nrcols == 0);
	assert(y->nrcols == 0);
	t = tail_type(x);
	assert(t);
/*	assert(t == tail_type(y));	*/
	f = sql_bind_func_result(sa, NULL, "sql_min", t, t, t);
	assert(f);
	return stmt_binop(sa, x, y, f);
}

static stmt *
stmt_max(sql_allocator *sa, stmt *x, stmt *y)
{
	sql_subtype *t;
	sql_subfunc *f;

	assert(x->nrcols == 0);
	assert(y->nrcols == 0);
	t = tail_type(x);
	assert(t);
/*	assert(t == tail_type(y));	*/
	f = sql_bind_func_result(sa, NULL, "sql_max", t, t, t);
	assert(f);
	return stmt_binop(sa, x, y, f);
}

list *
shrink_select_ranges(mvc *sql, list *oldsels)
{
	/* find minimal ranges for selects per column on one table */
	list *newsels, *haveNoBasecol, *haveBasecol, *basecols, *oneColSels, *twoColSels;
	node *bc;

	newsels = list_new(sql->sa);

	/* we skip selects that involve two columns, e.g., R.a < R.b, here */
	twoColSels = list_select(oldsels, (void *) 1L, (fcmp) &cmp_sel_twoCol, NULL);
	oneColSels = list_select(oldsels, (void *) 1L, (fcmp) &cmp_sel_oneCol, NULL);
	assert(list_length(twoColSels) + list_length(oneColSels) == list_length(oldsels));

	/* some statments (e.g., st_idxbat when checking key constraints during inserts)
	   don't seem to have a basecolumn, hence, we skip them, here */
	haveNoBasecol = list_select(oneColSels, (void *) 1L, (fcmp) &cmp_sel_hasNoBasecol, NULL);
	haveBasecol = list_select(oneColSels, (void *) 1L, (fcmp) &cmp_sel_hasBasecol, NULL);
	assert(list_length(haveNoBasecol) + list_length(haveBasecol) == list_length(oneColSels));

	basecols = list_distinct(haveBasecol, (fcmp) &cmp_sel_basecol, NULL);
	for (bc = basecols->h; bc; bc = bc->next) {
		list *colsels = list_select(haveBasecol, bc->data, (fcmp) &cmp_sel_basecol, NULL);
		sql_subtype *tt = tail_type(((stmt *) (colsels->h->data))->op2);
		sql_subtype *tt2 = tt;

		if (list_length(colsels) > 1)
			tt2 = tail_type(((stmt *) (colsels->h->next->data))->op2);

		assert(tt);
		if (list_length(colsels) == 1) {
			stmt *s = colsels->h->data;
			/* only one select on this column; simply keep it. */
			/* we prefer range selects over simple selects */
			if (s->type == st_uselect2 || s->type == st_select2)
				list_prepend(newsels, s);
			else
				list_append(newsels, s);
		} else if (tt2 != tt ||  tt->type->localtype == TYPE_str || !sql_bind_func_result(sql->sa, sql->session->schema, "sql_min", tt, tt, tt) || !sql_bind_func_result(sql->sa, sql->session->schema, "sql_max", tt, tt, tt)) {
			/* no "min" and/or "max" available on this data type, hence, we cannot apply the following "tricks" */
			list_merge(newsels, colsels, NULL);
		} else {
			list *sels2, *sels1[cmp_all];
			stmt *bound[4] = { NULL, NULL, NULL, NULL };
			stmt *col[4] = { NULL, NULL, NULL, NULL };
			comp_type ct;
			node *n;
			int flg, len = 0;

			/* separate all single-range, equal, notequal, notlike, & like selects and eliminate duplicates
			 * (notequal, notlike, & like selects are saved and re-added at the end) */
			for (ct = cmp_gt; ct < cmp_all; ct++) {
				list *l = list_select(colsels, (void *) &ct, (fcmp) &cmp_sel_comp_type, NULL);

				len += list_length(l);
				sels1[ct] = list_distinct(l, (fcmp) &cmp_sel_val, NULL);
			}
			/* separate all double-sided range selects */
			sels2 = list_select(colsels, (void *) 1L, (fcmp) &cmp_sel2, NULL);
			len += list_length(sels2);

			/* make sure we didn't miss anything */
			assert(len == list_length(colsels));

			/* split-up each double-sided range select in two single-sided ones
			 * (x<[=]a<[=]y  =>  a>[=]x && a<[=]y) to find the minimal ranges below */
			for (n = sels2->h; n; n = n->next) {
				stmt *sl, *sr, *s = n->data;
				comp_type cl = cmp_all, cr = cmp_all;	/* invalid values; just to pacify compilers */

				switch (s->flag) {
				case 0:
					cl = cmp_gt;
					cr = cmp_lt;
					break;
				case 1:
					cl = cmp_gt;
					cr = cmp_lte;
					break;
				case 2:
					cl = cmp_gte;
					cr = cmp_lt;
					break;
				case 3:
					cl = cmp_gte;
					cr = cmp_lte;
					break;
				default:
					assert(0);
				}
				sl = stmt_uselect(sql->sa, s->op1, s->op2, cl);
				sr = stmt_uselect(sql->sa, s->op1, s->op3, cr);
				list_append(sels1[cl], sl);
				list_append(sels1[cr], sr);
			}

			/* split-up each equal (point) select in two single-sided ones
			 * (a=x  =>  a>=x && a<=x) to find the minimal ranges below */
			/*
			 * in the "worst case", this gets later re-combined to
			 *      x<=a<=x   which is identical to   a=x
			 *
			 * in case there are other equal (point) or range selects,
			 * this improves optimization, especially empty selections
			 * are detected instantly, e.g.:
			 *      a=x && a=y
			 *  =>  a>=x && a<=x && a>=y && a<=y
			 *  =>  a>=max(x,y) && a<=min(x,y)
			 *  =>  max(x,y)<=a<=min(x,y)
			 * or
			 *      a=x && a<=y
			 *  =>  a>=x && a<=x && a<=y
			 *  =>  a>=x && a<=min(x,y)
			 *  =>  x<=a<=min(x,y)
			 *
			 * in both cases we end up with just a single select
			 * instead of two selects plus a semijoin
			 */
			for (n = sels1[cmp_equal]->h; n; n = n->next) {
				stmt *sl, *sr, *s = n->data;

				sl = stmt_uselect(sql->sa, s->op1, s->op2, cmp_gte);
				sr = stmt_uselect(sql->sa, s->op1, s->op2, cmp_lte);
				list_append(sels1[cmp_gte], sl);
				list_append(sels1[cmp_lte], sr);
			}

			/* minimize all single-sided range selects */
			/*
			 * while
			 *      1) a< x && a< y  =>  a <  min(x,y)
			 *      2) a<=x && a<=y  =>  a <= min(x,y)
			 *      3) a> x && a> y  =>  a >  max(x,y)
			 *      4) a>=x && a>=y  =>  a >= max(x,y)
			 * we cannot do similar simple stuff for
			 *      5) a < x && a <= y
			 * and
			 *      6) a > x && a >= y
			 * TODO:
			 * for the latter two cases, produce code like
			 *      5: if (x<=y) { a < x } else { a <= y }
			 *      6: if (x>=y) { a > x } else { a >= y }
			 */
			for (ct = cmp_gt; ct <= cmp_lt; ct++) {
				if (list_length(sels1[ct]) > 0) {
					col[ct] = ((stmt *) (sels1[ct]->h->data))->op1;
				}
				if (list_length(sels1[ct]) == 1) {
					bound[ct] = ((stmt *) (sels1[ct]->h->data))->op2;
				}
				if (list_length(sels1[ct]) > 1) {
					list *bnds = list_new(sql->sa);

					for (n = sels1[ct]->h; n; n = n->next) {
						list_append(bnds, ((stmt *) (n->data))->op2);
					}
					if (ct <= cmp_gte)
						bound[ct] = (stmt *) list_reduce2(bnds, (freduce2) &stmt_max, sql->sa);
					else
						bound[ct] = (stmt *) list_reduce2(bnds, (freduce2) &stmt_min, sql->sa);
				}
			}

			/* pairwise (re-)combine single-sided range selects to double-sided range selects */
			/*
			 *      0) a> x && a< y  =>  x< a< y
			 *      1) a>=x && a< y  =>  x<=a< y
			 *      2) a> x && a<=y  =>  x< a<=y
			 *      3) a>=x && a<=y  =>  x<=a<=y
			 */
			for (flg = 0; flg <= 3; flg++) {
				comp_type cl = cmp_all, cr = cmp_all;	/* invalid values; just to pacify compilers */

				switch (flg) {
				case 0:
					cl = cmp_gt;
					cr = cmp_lt;
					break;
				case 1:
					cl = cmp_gte;
					cr = cmp_lt;
					break;
				case 2:
					cl = cmp_gt;
					cr = cmp_lte;
					break;
				case 3:
					cl = cmp_gte;
					cr = cmp_lte;
					break;
				default:
					assert(0);
				}
				if (bound[cl] && bound[cr]) {
					list_prepend(newsels, stmt_uselect2(sql->sa, col[cl], bound[cl], bound[cr], flg));
					bound[cl] = bound[cr] = NULL;
				}
			}

			/* collect remaining single-sided range selects that haven't found a partner */
			for (ct = cmp_gt; ct <= cmp_lt; ct++) {
				if (bound[ct]) {
					list_append(newsels, stmt_uselect(sql->sa, col[ct], bound[ct], ct));
				}
			}

			/* finally collect all saved like, notlike, and notequal selects */
			for (ct = cmp_ilike; ct >= cmp_notequal; ct--) {
				list_merge(newsels, sels1[ct], NULL);
			}
		}
	}
	/* re-add the skipped statements without basecolumn */
	list_merge(newsels, haveNoBasecol, NULL);
	/* re-add the skipped two-column selects */
	list_merge(newsels, twoColSels, NULL);
	return newsels;
}

#if 0
/* warning: ‘sel_find_keycolumn’ defined but not used */
static int 
sel_find_keycolumn( stmt *s, sql_kc *kc)
{
	s = s->op1;
	if (s && s->type == st_bat && s->op4.cval == kc->c)
		return 0;
	return -1;
}
#endif

#if 0
/* warning: ‘select_hash_key’ defined but not used */
static stmt *
select_hash_key( mvc *sql, sql_idx *i, list *l ) 
{
	sql_subtype *it, *wrd;
	stmt *h = NULL;
	stmt *bits = stmt_atom_int(sql->sa, 1 + ((sizeof(wrd)*8)-1)/(list_length(l)+1));
	node *icn;

	it = sql_bind_localtype("int");
	wrd = sql_bind_localtype("wrd");
        for(icn = i->columns->h; icn; icn = icn->next) {
		sql_kc *kc = icn->data;
		node *n = list_find(l, kc, (fcmp)&sel_find_keycolumn);
		stmt *s;

		assert(n);

		s = n->data;
		if (h) {
			sql_subfunc *xor = sql_bind_func_result3(sql->sa, sql->session->schema, "rotate_xor_hash", wrd, it, tail_type(s), wrd);

			h = stmt_Nop(sql->sa, stmt_list(sql->sa,  list_append( list_append(
				list_append(list_new(sql->sa), h), 
				bits), 
				s->op2)), 
				xor);
		} else {
			sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", tail_type(s), NULL, wrd);
			h = stmt_unop(sql->sa, s->op2, hf);
		}
	}
	return stmt_uselect(sql->sa, stmt_idxbat(sql->sa, i, RDONLY), h, cmp_equal);
}
#endif

static stmt *
join_hash_key( mvc *sql, list *l ) 
{
	node *m;
	sql_subtype *it, *wrd;
	stmt *h = NULL;
	stmt *bits = stmt_atom_int(sql->sa, 1 + ((sizeof(wrd)*8)-1)/(list_length(l)+1));
	stmt *o = NULL;

	it = sql_bind_localtype("int");
	wrd = sql_bind_localtype("wrd");
	for (m = l->h; m; m = m->next) {
		stmt *s = m->data;

		if (h) {
			sql_subfunc *xor = sql_bind_func_result3(sql->sa, sql->session->schema, "rotate_xor_hash", wrd, it, tail_type(s), wrd);

			h = stmt_Nop(sql->sa, stmt_list(sql->sa,  list_append( list_append(
				list_append(list_new(sql->sa), h), bits), 
				stmt_project(sql->sa, o, s ))), 
				xor);
		} else {
			sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", tail_type(s), NULL, wrd);
			o = stmt_mark(sql->sa, stmt_reverse(sql->sa, s), 40); 
			h = stmt_unop(sql->sa, stmt_mark(sql->sa, s, 40), hf);
		}
	}
	return stmt_join(sql->sa, stmt_reverse(sql->sa, o), h, cmp_equal);
}


/* TODO find out if the columns have an (hash) index */
static stmt *
releqjoin( mvc *sql, list *l1, list *l2 )
{
	node *n1, *n2;
	stmt *l, *r, *res;

	if (list_length(l1) <= 1) {
		l = l1->h->data;
		r = l2->h->data;
		return stmt_join(sql->sa, l, stmt_reverse(sql->sa, r), cmp_equal);
	}
	l = join_hash_key(sql, l1);
	r = join_hash_key(sql, l2);
	res = stmt_join(sql->sa, l, stmt_reverse(sql->sa, r), cmp_equal);
	l = stmt_mark(sql->sa, stmt_reverse(sql->sa, res), 4);
	r = stmt_mark(sql->sa, res, 4);
	for (n1 = l1->h, n2 = l2->h; n1 && n2; n1 = n1->next, n2 = n2->next) {
		stmt *ld = n1->data;
		stmt *rd = n2->data;
		stmt *le = stmt_project(sql->sa, l, ld );
		stmt *re = stmt_project(sql->sa, r, rd );
		/* intentional both tail_type's of le (as re sometimes is a
		   find for bulk loading */
		sql_subfunc *f=sql_bind_func(sql->sa, sql->session->schema, "=", tail_type(le), tail_type(le));

		stmt * cmp;

		assert(f);

		/* TODO use uselect only */
		cmp = stmt_binop(sql->sa, le, re, f);

		cmp = stmt_uselect(sql->sa, cmp, stmt_bool(sql->sa, 1), cmp_equal);

		/* TODO the semijoin may break the order!! */
		l = stmt_semijoin(sql->sa, l, cmp);
		r = stmt_semijoin(sql->sa, r, cmp);
	}
	res = stmt_join(sql->sa, stmt_reverse(sql->sa, l), r, cmp_equal);
	return res;
}

static stmt *
_project(mvc *sql, stmt *o, stmt *v )
{
	if (v->nrcols == 0)
		return v;
	return stmt_project(sql->sa, o, v);
}

static stmt *
reljoin( mvc *sql, stmt *rj, list *l2 )
{
	node *n = l2?l2->h:NULL;
	stmt *l, *r, *res;

	if (!rj && list_length(l2) == 1) 
		return l2->h->data;
	if (rj) {
		l = stmt_mark(sql->sa, stmt_reverse(sql->sa, rj), 50);
		r = stmt_mark(sql->sa, rj, 50);
	} else {
		res = n->data;
		l = stmt_mark(sql->sa, stmt_reverse(sql->sa, res), 4);
		r = stmt_mark(sql->sa, res, 4);
		n = n->next;
	}
	/* TODO also handle joinN */
	for (; n; n = n->next) {
		stmt *j = n->data, *ld, *o2, *rd, *cmp;
		if (j->type == st_reverse) {
			stmt *sw;
			j = j->op1;
			ld = j->op1;
			o2 = j->op2;
			rd = (j->type == st_join)?stmt_reverse(sql->sa, o2):o2;
			sw = l;
			l = r;
			r = sw;
		} else {
			ld = j->op1;
			o2 = j->op2;
			rd = (j->type == st_join)?stmt_reverse(sql->sa, o2):o2;
		}

		if (j->type == st_joinN) {
			list *ol,*nl = list_new(sql->sa);
			node *m;

			ol = j->op1->op4.lval;
			for (m = ol->h; m; m = m->next) 
				list_append(nl, _project(sql, l, m->data));
			ol = j->op2->op4.lval;
			for (m = ol->h; m; m = m->next) 
				list_append(nl, _project(sql, r, m->data));
			/* find function */
			cmp = stmt_uselect(sql->sa, stmt_Nop(sql->sa, stmt_list(sql->sa, nl), j->op4.funcval), stmt_bool(sql->sa, 1), cmp_equal);
		} else if (j->type == st_join2) {
			stmt *le = stmt_project(sql->sa, l, ld );
			stmt *re = stmt_project(sql->sa, r, rd );
			comp_type c1 = range2lcompare(j->flag);
			comp_type c2 = range2rcompare(j->flag);
			stmt *r2 = stmt_project(sql->sa, r, j->op3);
			stmt *cmp1 = stmt_uselect(sql->sa, le, re, c1);
			stmt *cmp2 = stmt_uselect(sql->sa, le, r2, c2);

			cmp = stmt_semijoin(sql->sa, cmp1, cmp2);
		} else {
			stmt *le = stmt_project(sql->sa, l, ld );
			stmt *re = stmt_project(sql->sa, r, rd );
			/* TODO force scan select ? */
			cmp = stmt_uselect(sql->sa, le, re, (comp_type)j->flag);
		}
		cmp = stmt_mark(sql->sa, stmt_reverse(sql->sa, cmp), 50);
		l = stmt_project(sql->sa, cmp, l);
		r = stmt_project(sql->sa, cmp, r);
		if (j != n->data) { /* reversed */
			stmt *sw = l;
			l = r;
			r = sw;
		}
	}
	res = stmt_join(sql->sa, stmt_reverse(sql->sa, l), r, cmp_equal);
	return res;
}

int
find_unique( stmt *s, void *v)
{
	stmt *c = head_column(s);

	(void)v;
	if (c && c->type == st_bat && c->op4.cval->unique == 1)
		return 0;
	return -1;
}

/* push the semijoin of (select,s) through the select statement (select) */
stmt *
push_semijoin( mvc *sql, stmt *select, stmt *s )
{
	if (select->type == st_list){ 
		list *l = list_new(sql->sa);
		node *n;

		for(n = select->op4.lval->h; n; n = n->next) {
			stmt *a = n->data, *n;

			if (a->nrcols) {
				n = push_semijoin(sql, a, s);
				if (n == a) 
					return select;
				list_append(l, n);
			} else {
				list_append(l, a);
			}
		}
		return stmt_list(sql->sa, l);
	}
	if (select->type == st_convert){ 
		list *types = select->op4.lval;
		sql_subtype *f = types->h->data;
		sql_subtype *t = types->h->next->data;
		stmt *op1 = select->op1;

		op1 = push_semijoin(sql, op1, s);
		return stmt_convert(sql->sa, op1, f, t, 0);
	}
	if (select->type == st_unop){ 
		stmt *op1 = select->op1;

		op1 = push_semijoin(sql, op1, s);
		op1 = stmt_unop(sql->sa, op1, select->op4.funcval);
		return op1;
	}
	if (select->type == st_binop) {
		stmt *op1 = select->op1;
		stmt *op2 = select->op2;
		if (op1->nrcols) 
			op1 = push_semijoin(sql, op1, s);
		if (op2->nrcols) 
			op2 = push_semijoin(sql, op2, s);
		return stmt_binop(sql->sa, op1, op2, select->op4.funcval);
	}
	if (select->type == st_Nop) {
		stmt *ops = select->op1;
		if (ops->nrcols) 
			ops = push_semijoin(sql, ops, s);
		return stmt_Nop(sql->sa, ops, select->op4.funcval);
	}
	if (select->type == st_diff) {
		stmt *op1 = select->op1;
		stmt *op2 = select->op2;

		op1 = push_semijoin(sql, op1, s);
		return stmt_diff(sql->sa, op1, op2);
	}
	if (select->type == st_union) {
		stmt *op1 = select->op1;
		stmt *op2 = select->op2;

		op1 = push_semijoin(sql, op1, s);
		op2 = push_semijoin(sql, op2, s);
		return stmt_union(sql->sa, op1, op2);
	}

	/* semijoin(reverse(semijoin(reverse(x)),s) */
	if (select->type == st_reverse &&
	    select->op1->type == st_semijoin &&
	    select->op1->op1->type == st_reverse) {
		stmt *op1 = select->op1->op1->op1;
		stmt *op2 = select->op1->op2;

		op1 = push_semijoin(sql, op1, s );
		return stmt_reverse(sql->sa, stmt_semijoin(sql->sa,  stmt_reverse(sql->sa, op1), op2));
	}
	if (select->type != st_select2 && select->type != st_uselect2 &&
	    select->type != st_select && select->type != st_uselect)
		return stmt_semijoin(sql->sa, select, s);

	s = push_semijoin(sql, select->op1, s);
	if (select->type == st_select2) {
		comp_type cmp = (comp_type)select->flag;
		stmt *op2 = select->op2;
		stmt *op3 = select->op3;

		return stmt_select2(sql->sa,  s, op2, op3, cmp);
	}

	if (select->type == st_uselect2) {
		comp_type cmp = (comp_type)select->flag;
		stmt *op2 = select->op2;
		stmt *op3 = select->op3;

		return stmt_uselect2(sql->sa,  s, op2, op3, cmp);
	}

	if (select->type == st_select) {
		comp_type cmp = (comp_type)select->flag;
		stmt *op2 = select->op2;

		if (cmp == cmp_like || cmp == cmp_notlike ||
		    cmp == cmp_ilike || cmp == cmp_notilike)
		{
			stmt *op3 = select->op3;

			return stmt_likeselect(sql->sa, s, op2, op3, cmp);
		} else {
			return stmt_select(sql->sa,  s, op2, cmp);
		}
	}

	if (select->type == st_uselect) {
		comp_type cmp = (comp_type)select->flag;
		stmt *op2 = select->op2;

		return stmt_uselect(sql->sa,  s, op2, cmp);
	}
	assert(0);
	return NULL;
}

static stmt *
push_select_stmt( mvc *c, list *l, stmt *sel )
{
	node *n;

	for (n = l->h; n; n = n->next) {
		stmt *s = rel2bin(c, n->data);

		sel = push_semijoin(c, s, sel);
	}
	return sel;
}

#if 0
/* warning: ‘use_ukey’ defined but not used */
static list *
use_ukey( mvc *sql, list *l )
{
	sql_table *t;
	node *n;
	stmt *s;

	for( n = l->h; n; n = n->next) {
		s = n->data;

		/* we can only use hash indices for lookups, not for ranges */
		if (!(PSEL(s) && s->flag == cmp_equal))
			return l;
		/* we only want selects on base columns */
		if (s->op1->type != st_bat)
			return l;
	}

	s = l->h->data;
	if (!s->h || s->h->type != st_basetable)
		return l;
	t = s->h->op1.tval;
	
	if (t->idxs.set) {
		int cnt = 0;
		node *in;
		sql_idx *i;
	   	for(in = t->idxs.set->h; in; in = in->next){
 			i = in->data;
			if (hash_index(i->type) && 
			    list_length(l) == list_length(i->columns)) {
				node *icn;

				cnt = 0;
              			for(icn = i->columns->h; icn; icn = icn->next, cnt++) {
					sql_kc *kc = icn->data;
					node *n = list_find(l, kc, (fcmp)&sel_find_keycolumn);
					if (!n)
						break;
				}
              			if (list_length(i->columns) == cnt) {
					break;
				}
				cnt = 0;
			}
		}
		if (cnt) { /* result can only be one row! */
			stmt *hash = select_hash_key(sql, i, l);
			list_prepend(l, hash);
		}
	}
	return l;
}
#endif

stmt *
rel2bin(mvc *c, stmt *s)
{
	assert(!(s->optimized < 2 && s->rewritten));
	if (s->optimized >= 2) {
		if (s->rewritten)
			return s->rewritten;
		else
			return s;
	}

	switch (s->type) {
		/* first just return those statements which we cannot optimize,
		 * such as schema manipulation, transaction managment, 
		 * and user authentication.
		 */
	case st_none:
	case st_connection:
	case st_rs_column:
	case st_dbat:
	case st_basetable:

	case st_atom:
	case st_export:
	case st_var:
	case st_table_clear:

		s->optimized = 2;
		return s;

	case st_releqjoin:{

		list *l1 = list_new(c->sa);
		list *l2 = list_new(c->sa);
		node *n1, *n2;
		stmt *res;

		for (n1 = s->op1->op4.lval->h, n2 = s->op2->op4.lval->h; n1 && n2; n1 = n1->next, n2 = n2->next) {
			list_append(l1, rel2bin(c, n1->data));
			list_append(l2, rel2bin(c, n2->data));
		}
		res = releqjoin(c, l1, l2);
		s->optimized = res->optimized = 2;
		if (res != s) {
			assert(s->rewritten==NULL);
			s->rewritten = res;
		}
		return res;
	}

	case st_reljoin:{

		stmt *rj = NULL, *rjr = NULL;
		stmt *res;

		if (s->op1)
			rj = rel2bin(c, s->op1);
		if (s->op2)
			rjr = rel2bin(c, s->op2);

		res = reljoin(c, rj, (rjr)?rjr->op4.lval:NULL);
		s->optimized = res->optimized = 2;
		if (res != s) {
			assert(s->rewritten==NULL);
			s->rewritten = res;
		}
		return res;
	}

	case st_relselect:{

		stmt *res;
		node *n;

		list *l = s->op1->op4.lval;

		assert(list_length(l));
		if (list_length(l) == 1) {
			res = rel2bin(c, l->h->data);
		} else {
			stmt *sel;

			l = list_dup(l, NULL);
			if (!mvc_debug_on(c, 4096)) {
				l = shrink_select_ranges(c, l);
			}
			/* check if we have a unique index */
			//l = use_ukey(c, l);
			n = list_find(l, (void*)1, (fcmp) &find_unique);
			if (!n) {
				/* TODO reorder select list 
				   (this is also needed for the unique case) */
				n = l->h;
			}
			sel = n->data; 
			list_remove_node(l, n);

			sel = push_select_stmt(c, l, sel);
			res = rel2bin(c, sel);
		}

		s->optimized = res->optimized = 2;
		if (res != s) {
			assert(s->rewritten==NULL);
			s->rewritten = res;
		}
		return res;
	}

	case st_semijoin: {
/*
		stmt *res = push_semijoin(c, rel2bin(c, s->op1), rel2bin(c, s->op2));
		s->optimized = res->optimized = 2;
		if (res != s) {
			assert(s->rewritten==NULL);
			s->rewritten = res;
		}
		return res;
*/

		/* here we should rewrite semijoins into mirrorjoins */
		/* select(t.a,l1,h1).semijoin(select(t.b,l2,h2)); */
		/* select(t.a,l1,h1).mirror().join(t.b).select(l2,l2); */
		/* in the bin_optimizer we should make sure the 
		   mirror and projection join are pushed through 
		   the 'delta column'. 
		*/
	}
	case st_limit: 
	case st_limit2: 
	case st_join:
	case st_join2:
	case st_joinN:

	case st_reverse:
	case st_select:
	case st_select2:
	case st_selectN:
	case st_uselect:
	case st_uselect2: 
	case st_uselectN: 

	case st_temp:
	case st_single:
	case st_diff:
	case st_union:
	case st_outerjoin:
	case st_mirror:
	case st_const:
	case st_mark:
	case st_gen_group:
	case st_group:
	case st_group_ext:
	case st_derive:
	case st_unique:
	case st_order:
	case st_reorder:
	case st_ordered:

	case st_alias:
	case st_append:
	case st_exception:
	case st_trans:
	case st_catalog:

	case st_aggr:
	case st_unop:
	case st_binop:
	case st_Nop:
	case st_convert:

	case st_affected_rows:
	case st_table:

	case st_cond:
	case st_control_end:
	case st_return:
	case st_assign:

	case st_output: 

	case st_append_col:
	case st_update_col:
	case st_append_idx:
	case st_update_idx:
	case st_delete:

		if (s->op1) {
			stmt *os = s->op1;
			stmt *ns = rel2bin(c, os);

			assert(ns != s);
			s->op1 = ns;
		}

		if (s->op2) {
			stmt *os = s->op2;
			stmt *ns = rel2bin(c, os);

			assert(ns != s);
			s->op2 = ns;
		}
		if (s->op3) {
			stmt *os = s->op3;
			stmt *ns = rel2bin(c, os);
	
			assert(ns != s);
			s->op3 = ns;
		}
		s->optimized = 2;
		return s;

	case st_list:{

		stmt *res = NULL;
		node *n;
		list *l = s->op4.lval;
		list *nl = NULL;

		nl = list_new(c->sa);
		for (n = l->h; n; n = n->next) {
			stmt *ns = rel2bin(c, n->data);

			list_append(nl, ns);
		}
		res = stmt_list(c->sa, nl);
		s->optimized = res->optimized = 2;
		if (res != s) {
			assert(s->rewritten==NULL);
			s->rewritten = res;
		}
		return res;
	}

	case st_bat:

		if (s->flag == RDONLY && 
			!mvc_debug_on(c, 32) &&
			!mvc_debug_on(c, 64) &&
			!mvc_debug_on(c, 8192)) {
			stmt *res = stmt_delta_table_bat(c->sa,  s->op4.cval, s->h, s->flag);
			assert(s->rewritten==NULL);
			s->rewritten = res;
			s->optimized = res->optimized = 2;
			return res;
		} else {
			s->optimized = 2;
			return s;
		}

	case st_idxbat:

		if (s->flag == RDONLY && 
			!mvc_debug_on(c, 32) &&
			!mvc_debug_on(c, 64) &&
			!mvc_debug_on(c, 8192)) {
			stmt *res = stmt_delta_table_idxbat(c->sa,  s->op4.idxval, s->flag);
			assert(s->rewritten==NULL);
			s->rewritten = res;
			s->optimized = res->optimized = 2;
			return res;
		} else {
			s->optimized = 2;
			return s;
		}

	default:
		assert(0);	/* these should have been rewriten by now */
	}
	return s;
}
