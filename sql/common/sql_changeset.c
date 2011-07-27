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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */


#include "monetdb_config.h"
#include "sql_catalog.h"

void
cs_init(changeset * cs, fdestroy destroy)
{
	cs->sa = NULL;
	cs->destroy = destroy;
	cs->set = NULL;
	cs->dset = NULL;
	cs->nelm = NULL;
}

void
cs_new(changeset * cs, sql_allocator *sa)
{
	cs->sa = sa;
	cs->destroy = NULL;
	cs->set = NULL;
	cs->dset = NULL;
	cs->nelm = NULL;
}

void
cs_destroy(changeset * cs)
{
	if (cs->set)
		list_destroy(cs->set);
	if (cs->dset)
		list_destroy(cs->dset);
}

void
cs_add(changeset * cs, void *elm, int flag)
{
	if (!cs->set) {
		if (cs->sa)
			cs->set = list_new(cs->sa);
		else
			cs->set = list_create(cs->destroy);
	}
	list_append(cs->set, elm);
	if (flag == TR_NEW && !cs->nelm)
		cs->nelm = cs->set->t;
}

void
cs_add_before(changeset * cs, node *n, void *elm)
{
	list_append_before(cs->set, n, elm);
}

void
cs_del(changeset * cs, node *elm, int flag)
{
	if (flag == TR_NEW) {	/* remove just added */
		if (cs->nelm == elm)
			cs->nelm = elm->next;
		list_remove_node(cs->set, elm);
	} else {
		if (!cs->dset) {
			if (cs->sa)
				cs->dset = list_new(cs->sa);
			else
				cs->dset = list_create(cs->destroy);
		}
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
