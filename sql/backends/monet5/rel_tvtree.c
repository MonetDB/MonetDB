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
tv_tuple_value(backend *be, tv_tree *t, sql_exp *tuple, stmt *left, stmt *sel)
{
	switch (t->tvt) {
		case TV_MS_BSC:
		case TV_SO_BSC:
			// TODO
			break;
		case TV_BASIC:
			assert(!tuple->f);
			stmt *i = exp_bin(be, tuple, left, NULL, NULL, NULL, NULL, sel, 0, 0, 0);
			if (!i)
				return NULL;
			list_append(t->vals, i);
			break;
		case TV_MS_COMP:
		case TV_SO_COMP:
			// TODO
			break;
		case TV_COMP:
			assert(tuple->f);
			int cnt = 0;
			list *cvals = tuple->f;
			for (node *n = cvals->h; n; cnt++, n = n->next)
				if (false == tv_tuple_value(be, list_fetch(t->cf, cnt), n->data, left, sel))
					return false;
			break;
		default:
			assert(0);
			break;
	}

	return true;
}

bool
tv_parse_values(backend *be, tv_tree *t, list *cvals, stmt *left, stmt *sel)
{
	/* cvals is a list with values that correspond to a column whose
	 * (probably "complex") type is represented by the tv_tree
	 */
	for (node *n = cvals->h; n; n = n->next)
		if (false == tv_tuple_value(be, t, n->data, left, sel))
			return false;
	return true;
}

stmt *
tv_generate_stmts(backend *be, tv_tree *t)
{
	switch (t->tvt) {
		case TV_MS_BSC:
		case TV_SO_BSC:
			// TODO
			break;
		case TV_BASIC:
			return stmt_append_bulk(be, stmt_temp(be, t->st), t->vals);
		case TV_MS_COMP:
		case TV_SO_COMP:
			// TODO
			break;
		case TV_COMP:
			/* gather all the composite (sub)field's statements */
			list *fsts = sa_list(be->mvc->sa);
			for (node *n = t->cf->h; n; n = n->next)
				append(fsts, tv_generate_stmts(be, n->data));
			return stmt_list(be, fsts);
		default:
			assert(0);
			break;
	}

	return NULL;
}
