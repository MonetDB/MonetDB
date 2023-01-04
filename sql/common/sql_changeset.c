/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_catalog.h"

void
cs_new(changeset * cs, sql_allocator *sa, fdestroy destroy, fkeyvalue hfunc)
{
	*cs = (changeset) {
		.sa = sa,
		.destroy = destroy,
		.fkeyvalue = hfunc,
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

changeset *
cs_add(changeset * cs, void *elm, bool isnew)
{
	if (!cs->set && !(cs->set = list_new(cs->sa, cs->destroy)))
		return NULL;

	int sz = list_length(cs->set);
	/* re-hash or create hash table before inserting */
	if ((!cs->set->ht || cs->set->ht->size * 16 < sz) && sz > HASH_MIN_SIZE) {
		hash_destroy(cs->set->ht);
		if (!(cs->set->ht = hash_new(cs->sa, MAX(sz, 1) * 16, cs->fkeyvalue)))
			return NULL;
		for (node *n = cs->set->h; n; n = n->next) {
			if (!hash_add(cs->set->ht, cs->set->ht->key(n->data), n->data))
				return NULL;
		}
	}
	if (!list_append(cs->set, elm))
		return NULL;
	if (isnew && !cs->nelm)
		cs->nelm = cs->set->t;
	return cs;
}

changeset *
cs_del(changeset * cs, void *gdata, node *n, bool force)
{
	if (!force && !cs->dset && !(cs->dset = list_new(cs->sa, cs->destroy)))
		return NULL;

	if (cs->nelm == n) /* update nelm pointer */
		cs->nelm = n->next;
	if (cs->set->ht) /* delete hashed value */
		hash_del(cs->set->ht, cs->set->ht->key(n->data), n->data);

	if (force) { /* remove just added */
		if (!list_remove_node(cs->set, gdata, n))
			return NULL;
	} else {
		if (!list_move_data(cs->set, cs->dset, n->data))
			return NULL;
	}
	return cs;
}

int
cs_size(changeset * cs)
{
	if (cs->set)
		return list_length(cs->set);
	return 0;
}

