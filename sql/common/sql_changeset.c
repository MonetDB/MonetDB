/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_catalog.h"

void
cs_new(changeset * cs, sql_allocator *sa, fdestroy destroy)
{
	*cs = (changeset) {
		.sa = sa,
		.destroy = destroy,
	};
}

void
cs_destroy(changeset * cs)
{
	if (cs->set) {
		list_destroy(cs->set);
		cs->set = NULL;
	}
	if (cs->dset) {
		list_destroy(cs->dset);
		cs->dset = NULL;
	}
}

void
cs_add(changeset * cs, void *elm, int flags)
{
	if (!cs->set) 
		cs->set = list_new(cs->sa, cs->destroy);
	list_append(cs->set, elm);
	if (newFlagSet(flags) && !cs->nelm)
		cs->nelm = cs->set->t;
}

void *
cs_transverse_with_validate(changeset * cs, void *elm, fvalidate cmp)
{
	return list_traverse_with_validate(cs->set, elm, cmp);
}

void*
cs_add_with_validate(changeset * cs, void *elm, int flags, fvalidate cmp)
{
	void* res = NULL;
	if (!cs->set)
		cs->set = list_new(cs->sa, cs->destroy);
	if((res = list_append_with_validate(cs->set, elm, cmp)) != NULL)
		return res;
	if (newFlagSet(flags) && !cs->nelm)
		cs->nelm = cs->set->t;
	return res;
}

void
cs_add_before(changeset * cs, node *n, void *elm)
{
	list_append_before(cs->set, n, elm);
}

void
cs_del(changeset * cs, node *elm, int flags)
{
	if (newFlagSet(flags)) {	/* remove just added */
		if (cs->nelm == elm)
			cs->nelm = elm->next;
		list_remove_node(cs->set, elm);
	} else {
		if (!cs->dset) 
			cs->dset = list_new(cs->sa, cs->destroy);
		list_move_data(cs->set, cs->dset, elm->data);
	}
}

void
cs_move(changeset *from, changeset *to, void *data)
{
	if (!to->set)
		to->set = list_new(to->sa, to->destroy);
	list_move_data(from->set, to->set, data);
}

int
cs_size(changeset * cs)
{
	if (cs->set)
		return list_length(cs->set);
	return 0;
}

node *
cs_first_node(changeset * cs)
{
	return cs->set->h;
}

node *
cs_last_node(changeset * cs)
{
	return cs->set->t;
}

void 
cs_remove_node(changeset * cs, node *n)
{
	node *nxt = n->next;

	list_remove_node(cs->set, n);
	if (cs->nelm == n)
		cs->nelm = nxt;
}
