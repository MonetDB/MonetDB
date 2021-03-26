/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
cs_destroy(changeset * cs, void *data)
{
	if (cs->set) {
		list_destroy2(cs->set, data);
		cs->set = NULL;
	}
	if (cs->dset) {
		list_destroy2(cs->dset, data);
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

void
cs_del(changeset * cs, void *gdata, node *elm, int flags)
{
	if (cs->nelm == elm)
		cs->nelm = elm->next;
	if (newFlagSet(flags)) {	/* remove just added */
		list_remove_node(cs->set, gdata, elm);
	} else {
		if (!cs->dset)
			cs->dset = list_new(cs->sa, cs->destroy);
		list_move_data(cs->set, cs->dset, elm->data);
	}
}

int
cs_size(changeset * cs)
{
	if (cs->set)
		return list_length(cs->set);
	return 0;
}

