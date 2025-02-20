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
	bool comp = st->type->composite;

	if (st->multiset) {
		if (st->multiset == MS_ARRAY)
			return comp ? TV_MS_COMP : TV_MS_BSC;
		if (st->multiset == MS_SETOF)
			return comp ? TV_SO_COMP : TV_SO_BSC;
	} else if (comp)
		return TV_COMP;

	return TV_BASIC;
}

static tv_tree*
tv_node(allocator *sa, sql_subtype *st)
{
	tv_tree *n = (sa)?SA_NEW(sa, tv_tree):MNEW(tv_tree);
	if (n == NULL)
		return NULL;

	n->st = st;
	n->tvt = tv_get_type(st);
	n->rid_idx = 0;
	n->cf = n->rid = n->msid = n->msnr = n->vals = NULL;

	/* allocate only the lists that we need based on tv-tree type */
	switch (n->tvt) {
        case TV_MS_BSC:
        	n->msnr = sa_list(sa);
			/* fall through */
        case TV_SO_BSC:
        	n->rid = sa_list(sa);
        	n->msid = sa_list(sa);
			/* fall through */
		case TV_BASIC:
			n->vals = sa_list(sa);
       		break;
        case TV_MS_COMP:
			n->msnr = sa_list(sa);
			/* fall through */
		case TV_SO_COMP:
        	n->rid = sa_list(sa);
        	n->msid = sa_list(sa);
			/* fall through */
        case TV_COMP:
			n->cf = sa_list(sa);
        	break;
		default:
			assert(0);
			break;
	}

	return n;
}

tv_tree *
tv_create(backend *be, sql_subtype *st)
{
	tv_tree *t = tv_node(be->mvc->sa, st);

	switch (t->tvt) {
		case TV_MS_BSC:
		case TV_SO_BSC:
		case TV_BASIC:
			/* no need to do anything */
			break;
		case TV_MS_COMP:
		case TV_SO_COMP:
		case TV_COMP:
			for (node *n = st->type->d.fields->h; n; n = n->next) {
				sql_arg *fa = n->data;
				append(t->cf, tv_create(be, &fa->type));
			}
			break;
		default:
			assert(0);
			break;
	}

	return t;
}

static bool
tv_parse_values_(backend *be, tv_tree *t, sql_exp *value, stmt *left, stmt *sel)
{
	switch (t->tvt) {
		case TV_MS_BSC:
		case TV_SO_BSC:
			// TODO
			break;
		case TV_BASIC:
			assert(!value->f);
			stmt *i = exp_bin(be, value, left, NULL, NULL, NULL, NULL, sel, 0, 0, 0);
			if (!i)
				return false;
			assert(t->vals);
			list_append(t->vals, i);
			break;
		case TV_MS_COMP:
		case TV_SO_COMP:
			assert(value->f);

			/* add the rowid to the mset "origin" table */
			stmt *rid = stmt_atom_int(be, t->rid_idx);
			if (!rid)
				return false;
			assert(t->rid);
			list_append(t->rid, rid);

			/* per value insert actual data, msid(=rowid), msnr(for MS only) */
			list *ms_vals = value->f;
			for (node *n = ms_vals->h; n; n = n->next) {

				int msnr_idx = 1;  /* NOTE: in mset-value values are 1-offset indexed */
				list *cf_vals = ((sql_exp*)n->data)->f;
				for (node *m = cf_vals->h; m; m = m->next, msnr_idx++)
					if (false == tv_parse_values_(be, list_fetch(t->cf, msnr_idx - 1), m->data, left, sel))
						return false;

				stmt *msid = stmt_atom_int(be, t->rid_idx);
				if (!msid)
					return false;
				list_append(t->msid, msid);

				if (t->tvt == TV_MS_COMP) {
					stmt *msnr = stmt_atom_int(be, msnr_idx);
					if (!msnr)
						return false;
					list_append(t->msnr, msnr);
				}
			}

			/* we inserted all the mset values for a value for a given
			 * row so now increment this tv_tree node's (mset) rowid */
			t->rid_idx++;

			break;
		case TV_COMP:
			assert(value->f);
			int cnt = 0;
			list *cf_vals = value->f;
			for (node *n = cf_vals->h; n; cnt++, n = n->next)
				if (false == tv_parse_values_(be, list_fetch(t->cf, cnt), n->data, left, sel))
					return false;
			break;
		default:
			assert(0);
			break;
	}

	return true;
}

bool
tv_parse_values(backend *be, tv_tree *t, list *col_vals, stmt *left, stmt *sel)
{
	/* col_vals is a list with values that correspond to a column whose
	 * (probably "complex") type is represented by the tv_tree
	 */
	for (node *n = col_vals->h; n; n = n->next)
		if (false == tv_parse_values_(be, t, n->data, left, sel))
			return false;
	return true;
}

static void
tv_generate_stmts_(backend *be, tv_tree *t, list *stmts_list)
{
	stmt *ap;

	switch (t->tvt) {
		case TV_MS_BSC:
		case TV_SO_BSC:
			// TODO
			break;
		case TV_BASIC:
			ap = stmt_append_bulk(be, stmt_temp(be, t->st), t->vals);
			list_append(stmts_list, ap);
			break;
		case TV_MS_COMP:
		case TV_SO_COMP:
			stmt *tmp;

			tmp = stmt_temp(be, tail_type(t->rid->h->data));
			ap = stmt_append_bulk(be, tmp, t->rid);
			append(stmts_list, ap);

			for (node *n = t->cf->h; n; n = n->next)
				tv_generate_stmts_(be, n->data, stmts_list);

			tmp = stmt_temp(be, tail_type(t->msid->h->data));
			ap = stmt_append_bulk(be, tmp, t->msid);
			append(stmts_list, ap);

			if (t->tvt == TV_MS_COMP) {
				tmp = stmt_temp(be, tail_type(t->msnr->h->data));
				ap = stmt_append_bulk(be, tmp, t->msnr);
				append(stmts_list, ap);
			}
			break;
		case TV_COMP:
			/* gather all the composite (sub)field's statements */
			for (node *n = t->cf->h; n; n = n->next)
				tv_generate_stmts_(be, n->data, stmts_list);
			break;
		default:
			assert(0);
			break;
	}
}

stmt *
tv_generate_stmts(backend *be, tv_tree *t)
{
	list *stmts_list = sa_list(be->mvc->sa);
	tv_generate_stmts_(be, t, stmts_list);
	if (t->tvt == TV_BASIC)
		return stmts_list->h->data;
	else
		return stmt_list(be, stmts_list);
}
