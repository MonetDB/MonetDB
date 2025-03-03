/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 - 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"

#include "rel_tvtree.h"
#include "rel_exp.h"
#include "rel_bin.h"
#include "sql_statement.h"

static tv_type
tv_get_type(sql_subtype *st)
{
	if (st->multiset) {
		if (st->multiset == MS_ARRAY)
			return TV_MSET;
		if (st->multiset == MS_SETOF)
			return TV_SETOF;
	} else if (st->type->composite)
		return TV_COMP;

	return TV_BASIC;
}

static tv_tree*
tv_node(allocator *sa, sql_subtype *st, tv_type tvt)
{
	tv_tree *n = (sa)?SA_NEW(sa, tv_tree):MNEW(tv_tree);
	if (n == NULL)
		return NULL;
	n->st = st;
	n->tvt = tvt;
	n->rid_idx = 0;
	n->ctl = n->rid = n->msid = n->msnr = n->vals = NULL;

	/* allocate only the lists that we need based on the tv-tree type */
	switch (n->tvt) {
		case TV_BASIC:
			n->vals = sa_list(sa);
       		return n;
        case TV_COMP:
			n->ctl = sa_list(sa);
			for (node *sf = st->type->d.fields->h; sf; sf = sf->next) {
				sql_arg *sfa = sf->data;
				append(n->ctl, tv_node(sa, &sfa->type, tv_get_type(&sfa->type)));
			}
        	return n;
        case TV_MSET:
			n->msnr = sa_list(sa);
			/* fall through */
        case TV_SETOF:
        	n->rid = sa_list(sa);
        	n->msid = sa_list(sa);
			n->ctl = sa_list(sa);

			/* For MSET/SETOF we make a new child node for the values
			 * NOTE: the ->st of the child is the same as this node so
			 * we need to **EXPLICITLY** specify the tv_type */
			tv_tree *sn;
			if (st->type->composite)
				sn = tv_node(sa, st, TV_COMP);
			else
				sn = tv_node(sa, st, TV_BASIC);
			sn->st = st;

			append(n->ctl, sn);

			return n;
		default:
			assert(0);
			break;
	}

	return NULL;
}

tv_tree *
tv_create(backend *be, sql_subtype *st)
{
	/* there is some ambiguity with the types-value tree construction:
	 * nodes which are mset/setof have their underlying type (composite/basic)
	 * in the same subtype->type struct. That's why we have to be careful
	 * with how we generate the nodes. Read carefully tv_node */
	return tv_node(be->mvc->sa, st, tv_get_type(st));
}

static bool
tv_parse_values_(backend *be, tv_tree *t, sql_exp *value, stmt *left, stmt *sel);

static bool
append_values_from_varchar(backend *be, tv_tree *t, stmt *sl, stmt *left, stmt *sel, int *sid)
{
    node *n, *m;

	switch(t->tvt) {
		case TV_BASIC:
			if (sl->type == st_result)
				list_append(t->vals, sl);
			else if (sl->type == st_list) {
				stmt *sa = sl->op4.lval->h->data;
				list_append(t->vals, sa->op1);

				// caller (self with TV_MSET/SETOF) asserts proper sid value
				(*sid)++;
			}
			return true;
		case TV_COMP:
			for (n = t->ctl->h, m = sl->op4.lval->h; n; n = n->next, m = m->next, (*sid)++) {
			    stmt *ts = m->data;
			    assert(ts->type == st_alias);
				if (!append_values_from_varchar(be, n->data, ts->op1, left, sel, sid))
					return false;
			}
			return true;
		case TV_MSET:
		case TV_SETOF:
			assert(list_length(t->ctl) == 1);

			append_values_from_varchar(be, t->ctl->h->data, sl, left, sel, sid);

			list_append(t->msid, list_fetch(sl->op4.lval, (*sid)++));
			if (t->tvt == TV_MSET)
				list_append(t->msnr, list_fetch(sl->op4.lval, (*sid)++));
			list_append(t->rid, list_fetch(sl->op4.lval, (*sid)++));

			assert(list_length(sl->op4.lval) == *sid);

			return true;
		default:
			assert(0);
			break;
	}
	return true;
}

static bool
mset_value_from_array_constructor(backend *be, tv_tree *t, sql_exp *values, stmt *left, stmt *sel)
{
    /* rowid */
    stmt *rid = stmt_atom_int(be, t->rid_idx);
    if (!rid)
        return false;
    assert(t->rid);
    list_append(t->rid, rid);

    /* per value insert actual data, msid(=rowid), msnr(for MSET only) */
    int msnr_idx = 1;  /* NOTE: in mset-value values are 1-offset indexed */
    list *ms_vals = values->f;
    for (node *n = ms_vals->h; n; n = n->next, msnr_idx++) {

		/* vals (in the child tree) */
		assert(list_length(t->ctl) == 1);
		tv_tree *ct = t->ctl->h->data;
		tv_parse_values_(be, ct, n->data, left, sel);

		/* msid */
        stmt *msid = stmt_atom_int(be, t->rid_idx);
        if (!msid)
            return false;
        list_append(t->msid, msid);

		/* msnr */
        if (t->tvt == TV_MSET) {
            stmt *msnr = stmt_atom_int(be, msnr_idx);
            if (!msnr)
                return false;
            list_append(t->msnr, msnr);
        }
    }

    /* we inserted all the mset-value's subvalues so now
     * increment this tv_tree node's (mset) rowid index */
    t->rid_idx++;

    return true;
}

static bool
mset_value_from_literal(backend *be, tv_tree *t, sql_exp *values, stmt *left, stmt *sel)
{
    /* per entry-value in the literal the call to exp_bin() will generate an
     * `sql.from_varchar()` instruction. This instructions for a given returns
     * either 3 or 4 bat results corresponding to rowid, value, msid and optionally
     * msnr (multiset vs setof). Those return values will be in an st_list stmt
     * so we have to retrieve them (from stmt_list's op4) and append them to the
     * tv_tree list of stmts */
    assert(t->tvt == TV_SETOF || t->tvt == TV_MSET);
	assert(!t->vals && t->ctl);

	stmt *i = exp_bin(be, values, left, NULL, NULL, NULL, NULL, sel, 0, 0, 0);
	if (!i)
		return false;

	assert(i->type == st_list);

	int sid = 0;
	append_values_from_varchar(be, t, i, left, sel, &sid);

    return true;
}

static bool
comp_value_from_parenthesis(backend *be, tv_tree *t, sql_exp *values, stmt *left, stmt *sel)
{
	assert(values->f);
	list *ct_vals = values->f;

	int cnt = 0;
	for (node *n = ct_vals->h; n; cnt++, n = n->next)
		if (false == tv_parse_values_(be, list_fetch(t->ctl, cnt), n->data, left, sel))
			return false;

	return true;
}

static bool
comp_value_from_literal(backend *be, tv_tree *t, sql_exp *values, stmt *left, stmt *sel)
{
    assert(t->tvt == TV_COMP);
	assert(!t->vals && t->ctl);

	stmt *i = exp_bin(be, values, left, NULL, NULL, NULL, NULL, sel, 0, 0, 0);
	if (!i)
		return false;

	// TODO: consume all the values
	int sid = 0;
	append_values_from_varchar(be, t, i, left, sel, &sid);

	return true;
}

static bool
tv_parse_values_(backend *be, tv_tree *t, sql_exp *value, stmt *left, stmt *sel)
{
	stmt *i;

	switch (t->tvt) {
		case TV_BASIC:
			i = exp_bin(be, value, left, NULL, NULL, NULL, NULL, sel, 0, 0, 0);
			if (!i)
				return false;
			assert(t->vals);
			list_append(t->vals, i);
			break;
		case TV_MSET:
		case TV_SETOF:
            if (is_convert(value->type))
               	/* VALUES ('{1, 2, 3}') */
                return mset_value_from_literal(be, t, value, left, sel);
            else
               	/* VALUES (array[1, 2, 3]) */
                return mset_value_from_array_constructor(be, t, value, left, sel);
            break;
		case TV_COMP:
			if (is_convert(value->type))
				/* VALUES ('(1,"alice")') */
				return comp_value_from_literal(be, t, value, left, sel);
			else
				/* VALUES ((1,'alice')) */
				return comp_value_from_parenthesis(be, t, value, left, sel);
			break;
		default:
			assert(0);
			break;
	}

	return true;
}

static inline sql_exp *
tv_exp_wrap_list(backend *be, tv_tree *t, list *l)
{
	sql_exp *e = exp_null(be->mvc->sa, t->st);
	e->l = e->f = 0;
	e->f = l;
	return e;
}

bool
tv_parse_values(backend *be, tv_tree *t, sql_exp *col_vals, stmt *left, stmt *sel)
{
	list *vals = exp_get_values(col_vals);
	/* vals is a list with values that correspond to a column whose
	 * (possibly "complex") type is represented by the tv_tree. NOTE:
	 * in this case vals might be either
	 *   1. a list of many values or
	 *   2. a single value of composite or mset/setof with composite/basic type.
	 * that's why we need to check for
	 * 	 a. ->row in the first entry of col_vals exp
	 * 	 b. for mset/setof the first ->row in the first entry of vals
	 * If it is set it means that we are dealing with a single row insert and we
	 * need a dummy expression de (to put vals at its e->f) so parsing the values
	 * stays similar with the general case
	 */
	bool single_row_val = false;
	single_row_val |= col_vals->row;
	if ((t->tvt == TV_MSET) || (t->tvt == TV_SETOF)) {
		// single value MSET/SETOF of basic type
		single_row_val |= !((sql_exp*)vals->h->data)->f;
		// single value MSET/SETOF of composite type
		single_row_val |= ((sql_exp*)vals->h->data)->row;
	}

    if (single_row_val) {
		/* we need to create a dummy expression to single row vals
		 * to adhere to the rest of the api */
		sql_exp *de = tv_exp_wrap_list(be, t, vals);
		if (false == tv_parse_values_(be, t, de, left, sel))
			return false;
	} else {
		for (node *n = vals->h; n; n = n->next) {
			sql_exp *e = n->data;
			if (false == tv_parse_values_(be, t, e, left, sel))
				return false;
		}
	}
	return true;
}

stmt *
tv_generate_stmts(backend *be, tv_tree *t)
{
	stmt *ap, *tmp, *s;
	list *sl;

	switch (t->tvt) {
		case TV_BASIC:
			return stmt_append_bulk(be, stmt_temp(be, t->st), t->vals);
		case TV_MSET:
		case TV_SETOF:
			/* vals (in the child tree) */
			assert(list_length(t->ctl) == 1);
			tv_tree *ct = t->ctl->h->data;
			tmp = tv_generate_stmts(be, ct);

			/* if the lower tv node does NOT returns a list (e.g. because
			 * it is basic type) we need to create it explicitly */
			if (tmp->type == st_list) {
				s = tmp;
			} else {
				s = stmt_list(be, sa_list(be->mvc->sa));
				list_append(s->op4.lval, tmp);
			}

			/* msid */
			tmp = stmt_temp(be, tail_type(t->msid->h->data));
			ap = stmt_append_bulk(be, tmp, t->msid);
			append(s->op4.lval, ap);

			/* msnr */
			if (t->tvt == TV_MSET) {
				tmp = stmt_temp(be, tail_type(t->msnr->h->data));
				ap = stmt_append_bulk(be, tmp, t->msnr);
				append(s->op4.lval, ap);
			}

			/* rid */
			tmp = stmt_temp(be, tail_type(t->rid->h->data));
			ap = stmt_append_bulk(be, tmp, t->rid);
			append(s->op4.lval, ap);

			/* we've appended in the stmt_list so update nrcols */
			stmt_set_nrcols(s);

			return s;
		case TV_COMP:
			sl = sa_list(be->mvc->sa);
			/* gather all the composite (sub)field's statements */
			for (node *n = t->ctl->h; n; n = n->next)
				list_append(sl, tv_generate_stmts(be, n->data));
			return stmt_list(be, sl);
		default:
			assert(0);
			return NULL;
	}
	return s;
}
