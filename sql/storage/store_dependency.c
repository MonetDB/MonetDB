/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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
int
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
	int log_res = LOG_OK;

	if (is_oid_nil(store->table_api.column_find_row(tr, c_id, &id, c_dep_id, &depend_id, c_dep_type, &dtype, NULL)))
		log_res = store->table_api.table_insert(tr, t, &id, &depend_id, &dtype);

	return log_res;
}

/*Function to drop the dependencies on depend_id*/
int
sql_trans_drop_dependencies(sql_trans* tr, sqlid depend_id)
{
	sqlstore *store = tr->store;
	oid rid;
	sql_schema * s = find_sql_schema(tr, "sys");
	sql_table* deps = find_sql_table(tr, s, "dependencies");
	sql_column * dep_dep_id = find_sql_column(deps, "depend_id");
	rids *rs;
	int log_res = LOG_OK;

	rs = store->table_api.rids_select(tr, dep_dep_id, &depend_id, &depend_id, NULL);
	if (rs == NULL)
		return LOG_ERR;
	for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid) && log_res == LOG_OK; rid = store->table_api.rids_next(rs))
		log_res = store->table_api.table_delete(tr, deps, rid);
	store->table_api.rids_destroy(rs);
	return log_res;
}

/*Function to drop the dependency between object and target, ie obj_id/depend_id*/
int
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
	int log_res = LOG_OK;

	rs = store->table_api.rids_select(tr, dep_obj_id, &obj_id, &obj_id, dep_dep_id, &depend_id, &depend_id, dep_dep_type, &dtype, &dtype, NULL);
	if (rs == NULL)
		return LOG_ERR;
	for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid) && log_res == LOG_OK; rid = store->table_api.rids_next(rs))
		log_res = store->table_api.table_delete(tr, deps, rid);
	store->table_api.rids_destroy(rs);
	return log_res;
}

/*It returns a list with depend_id_1, depend_type_1,
                         depend_id_2, depend_type_2, ....*/
list*
sql_trans_get_dependents(sql_trans* tr, sqlid id,
						 sql_dependency dependent_type,
						 list * ignore_ids)
{
	sqlstore *store = tr->store;
	table_functions table_api = store->table_api;
	sql_schema *s = find_sql_schema(tr, "sys");
	sql_table *deps = find_sql_table(tr, s, "dependencies");
	sql_column *dep_id, *dep_dep_id, *dep_dep_type, *tri_id, *table_id;
	list *dep_list = list_create((fdestroy)_free),
		*schema_tables = NULL;
	void *v;
	oid rid;
	rids *rs;
	sqlid low_id = id, high_id = -1;

	if (!dep_list)
		return NULL;

	dep_id = find_sql_column(deps, "id");
	dep_dep_id = find_sql_column(deps, "depend_id");
	dep_dep_type = find_sql_column(deps, "depend_type");

	if (dependent_type == SCHEMA_DEPENDENCY) {
		sql_schema *s = find_sql_schema_id(tr, id);
		assert(s);
		schema_tables = list_create((fdestroy)_free);
		if (schema_tables == NULL) {
			list_destroy(dep_list);
			return NULL;
		}
		struct os_iter oi;
		os_iterator(&oi, s->tables, tr, NULL);
		bool first = true;
		for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
			sqlid* local_id = MNEW(sqlid);
			if (local_id == NULL) {
				list_destroy(dep_list);
				return NULL;
			}
			*local_id = b->id;
			if (list_append(schema_tables, local_id) == NULL) {
				list_destroy(dep_list);
				list_destroy(schema_tables);
				return NULL;
			}
			if (first) {
				low_id = b->id;
				first = false;
			}
			high_id = b->id;
		}
	}

	rs = table_api.rids_select(tr, dep_id, &low_id,
							   high_id == -1 ? &low_id :
							   low_id == high_id ? &low_id : &high_id,
							   NULL);
	if (rs == NULL) {
		list_destroy(dep_list);
		list_destroy(schema_tables);
		return NULL;
	}

	for (rid = table_api.rids_next(rs); !is_oid_nil(rid); rid = table_api.rids_next(rs)){
		if (dependent_type == SCHEMA_DEPENDENCY) {
			if (!(v = table_api.column_find_value(tr, dep_id, rid))) {
				list_destroy(dep_list);
				list_destroy(schema_tables);
				table_api.rids_destroy(rs);
				return NULL;
			}
			if (list_find_id(schema_tables, *(sqlid*)v) == NULL)
				continue;
		}
		if (!(v = table_api.column_find_value(tr, dep_dep_id, rid))) {
			list_destroy(dep_list);
			list_destroy(schema_tables);
			table_api.rids_destroy(rs);
			return NULL;
		}
		id = *(sqlid*)v;
		if (!(ignore_ids && list_find_func_id(ignore_ids, id))) {
			if (list_append(dep_list, v) == NULL) {
				_DELETE(v);
				list_destroy(dep_list);
				list_destroy(schema_tables);
				table_api.rids_destroy(rs);
				return NULL;
			}
			if (!(v = table_api.column_find_value(tr, dep_dep_type, rid))) {
				list_destroy(dep_list);
				list_destroy(schema_tables);
				table_api.rids_destroy(rs);
				return NULL;
			}
			if (list_append(dep_list, v) == NULL) {
				_DELETE(v);
				list_destroy(dep_list);
				list_destroy(schema_tables);
				table_api.rids_destroy(rs);
				return NULL;
			}
		} else {
			_DELETE(v);
		}
	}
	table_api.rids_destroy(rs);

	if (dependent_type == SCHEMA_DEPENDENCY)
		list_destroy(schema_tables);

	if (dependent_type == TABLE_DEPENDENCY) {
		sql_table *triggers = find_sql_table(tr, s, "triggers");
		table_id = find_sql_column(triggers, "table_id");
		tri_id = find_sql_column(triggers, "id");
		dependent_type = TRIGGER_DEPENDENCY;

		rs = table_api.rids_select(tr, table_id, &id, &id, NULL);
		if (rs == NULL) {
			list_destroy(dep_list);
			return NULL;
		}
		for (rid = table_api.rids_next(rs); !is_oid_nil(rid); rid = table_api.rids_next(rs)) {
			if (!(v = table_api.column_find_value(tr, tri_id, rid))) {
				list_destroy(dep_list);
				table_api.rids_destroy(rs);
				return NULL;
			}
			if (list_append(dep_list, v) == NULL) {
				_DELETE(v);
				list_destroy(dep_list);
				table_api.rids_destroy(rs);
				return NULL;
			}
			if (!(v = MNEW(sht))) {
				list_destroy(dep_list);
				table_api.rids_destroy(rs);
				return NULL;
			}
			*(sht *) v = (sht) dependent_type;
			if (list_append(dep_list, v) == NULL) {
				_DELETE(v);
				list_destroy(dep_list);
				table_api.rids_destroy(rs);
				return NULL;
			}
		}
		table_api.rids_destroy(rs);
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

	if (!l || !(users = backend_schema_user_dependencies(tr, schema_id))) {
		list_destroy(l);
		return NULL;
	}

	for (rid = store->table_api.rids_next(users); !is_oid_nil(rid); rid = store->table_api.rids_next(users)) {
		if (!(v = store->table_api.column_find_value(tr, auth_id, rid))) {
			list_destroy(l);
			store->table_api.rids_destroy(users);
			return NULL;
		}
		list_append(l,v);
		if (!(v = MNEW(sht))) {
			list_destroy(l);
			store->table_api.rids_destroy(users);
			return NULL;
		}
		*(sht*)v = (sht) type;
		list_append(l,v);
	}
	store->table_api.rids_destroy(users);

	if (list_length(l) == 0) {
		list_destroy(l);
		l = NULL;
	}

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
	if (rs == NULL)
		return NULL;

	for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)) {
		if (!(v = store->table_api.column_find_value(tr, schema_id, rid))) {
			list_destroy(l);
			store->table_api.rids_destroy(rs);
			return NULL;
		}
		list_append(l, v);
		if (!(v = MNEW(sht))) {
			list_destroy(l);
			store->table_api.rids_destroy(rs);
			return NULL;
		}
		*(sht*)v = (sht) type;
		list_append(l,v);
	}
	store->table_api.rids_destroy(rs);
	return l;
}

/*Function on Functions*/
