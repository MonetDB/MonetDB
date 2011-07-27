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

node *
cs_find_name(changeset * cs, char *name)
{
	return list_find_name(cs->set, name);
}

node *
list_find_name(list *l, char *name)
{
	node *n;

	if (l)
		for (n = l->h; n; n = n->next) {
			sql_base *b = n->data;

			/* check if names match */
			if (name[0] == b->name[0] && strcmp(name, b->name) == 0) {
				return n;
			}
		}
	return NULL;
}

node *
cs_find_id(changeset * cs, int id)
{
	node *n;
	list *l = cs->set;

	if (l)
		for (n = l->h; n; n = n->next) {
			sql_base *b = n->data;

			/* check if names match */
			if (b->id == id) {
				return n;
			}
		}
	return NULL;
}

node *
list_find_id(list *l, int id)
{
	node *n;

	if (l)
		for (n = l->h; n; n = n->next) {

			/* check if ids match */
			if (id == *(int *) n->data) {
				return n;
			}
		}
	return NULL;
}

node *
list_find_base_id(list *l, int id)
{
	node *n;

	if (l)
		for (n = l->h; n; n = n->next) {
			sql_base *b = n->data;

			/* check if names match */
			if (id == b->id) {
				return n;
			}
		}
	return NULL;
}


node *
find_sql_key_node(sql_table *t, char *kname, int id)
{
	if (kname)
		return cs_find_name(&t->keys, kname);
	else
		return cs_find_id(&t->keys, id);
}

sql_key *
find_sql_key(sql_table *t, char *kname)
{
	node *n = find_sql_key_node(t, kname, -1);

	if (n)
		return n->data;
	return NULL;
}

node *
find_sql_idx_node(sql_table *t, char *kname, int id)
{
	if (kname)
		return cs_find_name(&t->idxs, kname);
	else
		return cs_find_id(&t->idxs, id);
}

sql_idx *
find_sql_idx(sql_table *t, char *kname)
{
	node *n = find_sql_idx_node(t, kname, -1);

	if (n)
		return n->data;
	return NULL;
}

node *
find_sql_column_node(sql_table *t, char *cname, int id)
{
	if (cname)
		return cs_find_name(&t->columns, cname);
	else
		return cs_find_id(&t->columns, id);
}

sql_column *
find_sql_column(sql_table *t, char *cname)
{
	node *n = find_sql_column_node(t, cname, -1);

	if (n)
		return n->data;
	return NULL;
}

node *
find_sql_table_node(sql_schema *s, char *tname, int id)
{
	if (tname)
		return cs_find_name(&s->tables, tname);
	else
		return cs_find_id(&s->tables, id);
}

sql_table *
find_sql_table(sql_schema *s, char *tname)
{
	node *n = find_sql_table_node(s, tname, -1);

	if (n)
		return n->data;
	return NULL;
}

sql_table *
find_sql_table_id(sql_schema *s, int id)
{
	node *n = find_sql_table_node(s, NULL, id);

	if (n)
		return n->data;
	return NULL;
}

node *
find_sql_sequence_node(sql_schema *s, char *sname, int id)
{
	if (sname)
		return cs_find_name(&s->seqs, sname);
	else
		return cs_find_id(&s->seqs, id);
}

sql_sequence *
find_sql_sequence(sql_schema *s, char *sname)
{
	node *n = find_sql_sequence_node(s, sname, -1);

	if (n)
		return n->data;
	return NULL;
}

node *
find_sql_schema_node(sql_trans *t, char *sname, int id)
{
	if (sname)
		return cs_find_name(&t->schemas, sname);
	else
		return cs_find_id(&t->schemas, id);
}

sql_schema *
find_sql_schema(sql_trans *t, char *sname)
{
	node *n = find_sql_schema_node(t, sname, -1);

	if (n)
		return n->data;
	return NULL;
}

node *
find_sqlname(list *l, char *name)
{
	if (l) {
		node *n;

		for (n = l->h; n; n = n->next) {
			sql_type *t = n->data;

			if (strcmp(t->sqlname, name) == 0)
				return n;
		} 
	}
	return NULL;
}

node *
find_sql_type_node(sql_schema * s, char *tname, int id)
{
	if (tname)
		return find_sqlname(s->types.set, tname);
	else 
		return cs_find_id(&s->types, id);
}

sql_type *
find_sql_type(sql_schema * s, char *tname)
{
	node *n = find_sql_type_node(s, tname, -1);

	if (n)
		return n->data;
	return NULL;
}

sql_type *
sql_trans_bind_type(sql_trans *tr, sql_schema *c, char *name)
{
	node *n;
	sql_type *t = NULL;

	if (tr->schemas.set)
		for (n = tr->schemas.set->h; n && !t; n = n->next) {
			sql_schema *s = n->data;

			t = find_sql_type(s, name);
		}

	if (!t && c)
		t = find_sql_type(c, name);

	if (!t)
		return NULL;
	/* t->base.rtime = tr->rtime; */
	return t;
}

node *
find_sql_func_node(sql_schema * s, char *tname, int id)
{
	if (tname)
		return cs_find_name(&s->funcs, tname);
	else
		return cs_find_id(&s->funcs, id);
}

sql_func *
find_sql_func(sql_schema * s, char *tname)
{
	node *n = find_sql_func_node(s, tname, -1);

	if (n)
		return n->data;
	return NULL;
}

list *
find_all_sql_func(sql_schema * s, char *name, int is_func)
{
	list *l = s->funcs.set, *res = NULL;
	node *n = NULL;

	if (l) {
		for (n = l->h; n; n = n->next) {
			sql_base *b = n->data;
			sql_func *f = n->data;

			/* check if names match */
			if (f->is_func == is_func && name[0] == b->name[0] && strcmp(name, b->name) == 0) {
				if (!res)
					res = list_create((fdestroy)NULL);
				list_append(res, n->data);
			}
		}
	}
	return res;
}

sql_func *
sql_trans_bind_func(sql_trans *tr, char *name)
{
	node *n;
	sql_func *t = NULL;

	if (tr->schemas.set)
		for (n = tr->schemas.set->h; n && !t; n = n->next) {
			sql_schema *s = n->data;

			t = find_sql_func(s, name);
		}
	if (!t)
		return NULL;
	/*
	   t->base.rtime = tr->rtime;
	 */

	return t;
}
