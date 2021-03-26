/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "store_dependency.h"

static void
_free(void *dummy, void *data)
{
	(void)dummy;
	GDKfree(data);
}

static sqlid
list_find_func_id(list *ids, sqlid id)
{
	node *n = ids->h;
	while (n) {
		sql_func * f = n->data;
		if (f->base.id == id)
			return id;
		else
			n = n->next;
	}
	return 0;
}

/*Function to create a dependency*/
void
sql_trans_create_dependency(sql_trans* tr, sqlid id, sqlid depend_id, sql_dependency depend_type)
{
	assert(id && depend_id);
	sqlstore *store = tr->store;
	sql_schema * s = find_sql_schema(tr, "sys");
	sql_table *t = find_sql_table(tr, s, "dependencies");
	sql_column *c_id = find_sql_column(t, "id");
	sql_column *c_dep_id = find_sql_column(t, "depend_id");
	sql_column *c_dep_type = find_sql_column(t, "depend_type");
	sht dtype = (sht) depend_type;

	if (is_oid_nil(store->table_api.column_find_row(tr, c_id, &id, c_dep_id, &depend_id, c_dep_type, &dtype, NULL)))
		store->table_api.table_insert(tr, t, &id, &depend_id, &dtype);
}

/*Function to drop the dependencies on depend_id*/
void
sql_trans_drop_dependencies(sql_trans* tr, sqlid depend_id)
{
	sqlstore *store = tr->store;
	oid rid;
	sql_schema * s = find_sql_schema(tr, "sys");
	sql_table* deps = find_sql_table(tr, s, "dependencies");
	sql_column * dep_dep_id = find_sql_column(deps, "depend_id");
	rids *rs;

	rs = store->table_api.rids_select(tr, dep_dep_id, &depend_id, &depend_id, NULL);
	for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs))
		store->table_api.table_delete(tr, deps, rid);
	store->table_api.rids_destroy(rs);
}

/*Function to drop the dependency between object and target, ie obj_id/depend_id*/
void
sql_trans_drop_dependency(sql_trans* tr, sqlid obj_id, sqlid depend_id, sql_dependency depend_type)
{
	sqlstore *store = tr->store;
	oid rid;
	sql_schema * s = find_sql_schema(tr, "sys");
	sql_table* deps = find_sql_table(tr, s, "dependencies");
	sql_column *dep_obj_id = find_sql_column(deps, "id");
	sql_column *dep_dep_id = find_sql_column(deps, "depend_id");
	sql_column *dep_dep_type = find_sql_column(deps, "depend_type");
	sht dtype = (sht) depend_type;
	rids *rs;

	rs = store->table_api.rids_select(tr, dep_obj_id, &obj_id, &obj_id, dep_dep_id, &depend_id, &depend_id, dep_dep_type, &dtype, &dtype, NULL);
	for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs))
		store->table_api.table_delete(tr, deps, rid);
	store->table_api.rids_destroy(rs);
}

/*It returns a list with depend_id_1, depend_type_1, depend_id_2, depend_type_2, ....*/
list*
sql_trans_get_dependencies(sql_trans* tr, sqlid id, sql_dependency depend_type, list * ignore_ids)
{
	sqlstore *store = tr->store;
	void *v;
	sql_schema *s = find_sql_schema(tr, "sys");
	sql_table *deps = find_sql_table(tr, s, "dependencies");
	sql_column *dep_id, *dep_dep_id, *dep_dep_type, *tri_id, *table_id;
	list *dep_list = list_create((fdestroy) _free);
	oid rid;
	rids *rs;

	if (!dep_list)
		return NULL;

	dep_id = find_sql_column(deps, "id");
	dep_dep_id = find_sql_column(deps, "depend_id");
	dep_dep_type = find_sql_column(deps, "depend_type");

	rs = store->table_api.rids_select(tr, dep_id, &id, &id, NULL);
	for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)){
		v = store->table_api.column_find_value(tr, dep_dep_id, rid);
		id = *(sqlid*)v;
		if (!(ignore_ids  && list_find_func_id(ignore_ids, id))) {
			list_append(dep_list, v);
			v = store->table_api.column_find_value(tr, dep_dep_type, rid);
			list_append(dep_list, v);
		} else {
			_DELETE(v);
		}
	}
	store->table_api.rids_destroy(rs);

	if (depend_type == TABLE_DEPENDENCY) {
		sql_table *triggers = find_sql_table(tr, s, "triggers");
		table_id = find_sql_column(triggers, "table_id");
		tri_id = find_sql_column(triggers, "id");
		depend_type = TRIGGER_DEPENDENCY;

		rs = store->table_api.rids_select(tr, table_id, &id, &id, NULL);
		for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)) {
			v = store->table_api.column_find_value(tr, tri_id, rid);
			list_append(dep_list, v);
			v = MNEW(sht);
			if (v) {
				*(sht *) v = (sht) depend_type;
			} else {
				list_destroy(dep_list);
				return NULL;
			}
			list_append(dep_list, v);
		}
		store->table_api.rids_destroy(rs);
	}
	return dep_list;
}

/*It checks if there are dependency between two ID's */
sqlid
sql_trans_get_dependency_type(sql_trans *tr, sqlid id, sql_dependency depend_type)
{
	sqlstore *store = tr->store;
	oid rid;
	sql_schema *s;
	sql_table *dep;
	sql_column *dep_id, *dep_dep_id, *dep_dep_type;
	sht dtype = (sht) depend_type;

	s = find_sql_schema(tr, "sys");
	dep = find_sql_table(tr, s, "dependencies");
	dep_id = find_sql_column(dep, "id");
	dep_dep_id = find_sql_column(dep, "depend_id");
	dep_dep_type = find_sql_column(dep, "depend_type");

	rid = store->table_api.column_find_row(tr, dep_id, &id, dep_dep_type, &dtype, NULL);
	if (!is_oid_nil(rid)) {
		return store->table_api.column_find_sqlid(tr, dep_dep_id, rid);
	} else {
		return -1;
	}
}

/*It checks if there are dependency between two ID's */
int
sql_trans_check_dependency(sql_trans *tr, sqlid id, sqlid depend_id, sql_dependency depend_type)
{
	sqlstore *store = tr->store;
	oid rid;
	sql_schema *s;
	sql_table *dep;
	sql_column *dep_id, *dep_dep_id, *dep_dep_type;
	sht dtype = (sht) depend_type;

	s = find_sql_schema(tr, "sys");
	dep = find_sql_table(tr, s, "dependencies");
	dep_id = find_sql_column(dep, "id");
	dep_dep_id = find_sql_column(dep, "depend_id");
	dep_dep_type = find_sql_column(dep, "depend_type");

	rid = store->table_api.column_find_row(tr, dep_id, &id, dep_dep_id, &depend_id, dep_dep_type, &dtype, NULL);
	if (!is_oid_nil(rid))
		return 1;
	else return 0;
}

/*Schema on users*/

list *
sql_trans_schema_user_dependencies(sql_trans *tr, sqlid schema_id)
{
	sqlstore *store = tr->store;
	void *v;
	sql_schema * s = find_sql_schema(tr, "sys");
	sql_table *auths = find_sql_table(tr, s, "auths");
	sql_column *auth_id = find_sql_column(auths, "id");
	sql_dependency type = USER_DEPENDENCY;
	list *l = list_create((fdestroy) _free);
	rids *users;
	oid rid;

	if (!l)
		return NULL;

	users = backend_schema_user_dependencies(tr, schema_id);

	for (rid = store->table_api.rids_next(users); !is_oid_nil(rid); rid = store->table_api.rids_next(users)) {
		v = store->table_api.column_find_value(tr, auth_id, rid);
		list_append(l,v);
		v = MNEW(sht);
		if (v) {
			*(sht*)v = (sht) type;
		} else {
			list_destroy(l);
			store->table_api.rids_destroy(users);
			return NULL;
		}
		list_append(l,v);
	}
	store->table_api.rids_destroy(users);
	return l;
}

/*owner on schemas*/
list *
sql_trans_owner_schema_dependencies(sql_trans *tr, sqlid owner_id)
{
	sqlstore *store = tr->store;
	void *v;
	sql_schema * s = find_sql_schema(tr, "sys");
	sql_table *schemas = find_sql_table(tr, s, "schemas");
	sql_column *schema_owner = find_sql_column(schemas, "authorization");
	sql_column *schema_id = find_sql_column(schemas, "id");
	sql_dependency type = SCHEMA_DEPENDENCY;
	list *l = list_create((fdestroy) _free);
	rids *rs;
	oid rid;

	if (!l)
		return NULL;

	rs = store->table_api.rids_select(tr, schema_owner, &owner_id, &owner_id, NULL);

	for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)) {
		v = store->table_api.column_find_value(tr, schema_id, rid);
		list_append(l, v);
		v = MNEW(sht);
		if (v) {
			*(sht*)v = (sht) type;
		} else {
			list_destroy(l);
			store->table_api.rids_destroy(rs);
			return NULL;
		}
		list_append(l,v);
	}
	store->table_api.rids_destroy(rs);
	return l;
}

/*Function on Functions*/
