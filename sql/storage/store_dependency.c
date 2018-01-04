/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "store_dependency.h"

static int
list_find_func_id(list *ids, int id) {
	node *n = ids->h;
	while(n) {
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
sql_trans_create_dependency(sql_trans* tr, sqlid id, sqlid depend_id, short depend_type)
{
	sql_schema * s = find_sql_schema(tr, "sys");
	sql_table *t = find_sql_table(s, "dependencies");
	sql_column *c_id = find_sql_column(t, "id");
	sql_column *c_dep_id = find_sql_column(t, "depend_id");
	sql_column *c_dep_type = find_sql_column(t, "depend_type");

	if (table_funcs.column_find_row(tr, c_id, &id, c_dep_id, &depend_id, c_dep_type, &depend_type, NULL) == oid_nil)
		table_funcs.table_insert(tr, t, &id, &depend_id, &depend_type);
}

/*Function to drop the dependencies on depend_id*/
void
sql_trans_drop_dependencies(sql_trans* tr, sqlid depend_id)
{
	oid rid;
	sql_schema * s = find_sql_schema(tr, "sys");
	sql_table* deps = find_sql_table(s, "dependencies");
	sql_column * dep_dep_id = find_sql_column(deps, "depend_id");
	rids *rs;
	
	rs = table_funcs.rids_select(tr, dep_dep_id, &depend_id, &depend_id, NULL);
	for(rid = table_funcs.rids_next(rs); rid != oid_nil; rid = table_funcs.rids_next(rs)) 
		table_funcs.table_delete(tr, deps, rid);
	table_funcs.rids_destroy(rs);
}

/*Function to drop the dependency between object and target, ie obj_id/depend_id*/
void
sql_trans_drop_dependency(sql_trans* tr, sqlid obj_id, sqlid depend_id, short depend_type)
{
	oid rid;
	sql_schema * s = find_sql_schema(tr, "sys");
	sql_table* deps = find_sql_table(s, "dependencies");
	sql_column *dep_obj_id = find_sql_column(deps, "id");
	sql_column *dep_dep_id = find_sql_column(deps, "depend_id");
	sql_column *dep_dep_type = find_sql_column(deps, "depend_type");
	rids *rs;
	
	rs = table_funcs.rids_select(tr, dep_obj_id, &obj_id, &obj_id, dep_dep_id, &depend_id, &depend_id, dep_dep_type, &depend_type, &depend_type, NULL);
	for(rid = table_funcs.rids_next(rs); rid != oid_nil; rid = table_funcs.rids_next(rs)) 
		table_funcs.table_delete(tr, deps, rid);
	table_funcs.rids_destroy(rs);
}


/*It returns a list with depend_id_1, depend_type_1, depend_id_2, depend_type_2, ....*/
list*
sql_trans_get_dependencies(sql_trans* tr, int id, short depend_type, list * ignore_ids)
{
	void *v;
	sql_schema *s = find_sql_schema(tr, "sys");	
	sql_table *deps = find_sql_table(s, "dependencies");
	sql_column *dep_id, *dep_dep_id, *dep_dep_type, *tri_id, *table_id;
	list *dep_list = list_create((fdestroy) GDKfree); 
	oid rid;
	rids *rs;

	dep_id = find_sql_column(deps, "id");
	dep_dep_id = find_sql_column(deps, "depend_id");
	dep_dep_type = find_sql_column(deps, "depend_type");

	rs = table_funcs.rids_select(tr, dep_id, &id, &id, NULL);
	for(rid = table_funcs.rids_next(rs); rid != oid_nil; rid = table_funcs.rids_next(rs)){
		v = table_funcs.column_find_value(tr, dep_dep_id, rid);
		id = *(sqlid*)v;		
		if (!(ignore_ids  && list_find_func_id(ignore_ids, id))) {
			list_append(dep_list, v);
			v = table_funcs.column_find_value(tr, dep_dep_type, rid);
			list_append(dep_list, v);
		} else {
			_DELETE(v);
		}
	}
	table_funcs.rids_destroy(rs);

	if (depend_type == TABLE_DEPENDENCY) {
		sql_table *triggers = find_sql_table(s, "triggers");
		table_id = find_sql_column(triggers, "table_id");
		tri_id = find_sql_column(triggers, "id");
		depend_type = TRIGGER_DEPENDENCY;

		rs = table_funcs.rids_select(tr, table_id, &id, &id, NULL);
		for(rid = table_funcs.rids_next(rs); rid != oid_nil; rid = table_funcs.rids_next(rs)) {
			v = table_funcs.column_find_value(tr, tri_id, rid);
			list_append(dep_list, v);
			v = MNEW(sht);
			if(v)
				*(sht*)v = depend_type;
			list_append(dep_list, v);
		}
		table_funcs.rids_destroy(rs);
	}
	return dep_list;
}

/*It checks if there are dependency between two ID's */
int
sql_trans_get_dependency_type(sql_trans *tr, int id, short depend_type)
{
	oid rid;
	sql_schema *s;
	sql_table *dep;
	sql_column *dep_id, *dep_dep_id, *dep_dep_type;

	s = find_sql_schema(tr, "sys");	

	dep = find_sql_table(s, "dependencies");

	dep_id = find_sql_column(dep, "id");
	dep_dep_id = find_sql_column(dep, "depend_id");
	dep_dep_type = find_sql_column(dep, "depend_type");

	rid = table_funcs.column_find_row(tr, dep_id, &id, dep_dep_type, &depend_type, NULL);
	if (rid != oid_nil) {	
		int r, *v = table_funcs.column_find_value(tr, dep_dep_id, rid);

		r = *v;
		_DELETE(v);
		return r;
	} else {
		return -1;
	}
}

/*It checks if there are dependency between two ID's */
int
sql_trans_check_dependency(sql_trans *tr, int id, int depend_id, short depend_type)
{
	oid rid;
	sql_schema *s;
	sql_table *dep;
	sql_column *dep_id, *dep_dep_id, *dep_dep_type;

	s = find_sql_schema(tr, "sys");	

	dep = find_sql_table(s, "dependencies");

	dep_id = find_sql_column(dep, "id");
	dep_dep_id = find_sql_column(dep, "depend_id");
	dep_dep_type = find_sql_column(dep, "depend_type");

	rid = table_funcs.column_find_row(tr, dep_id, &id, dep_dep_id, &depend_id, dep_dep_type, &depend_type, NULL);
	if (rid != oid_nil)	
		return 1;
	else return 0;
}



/*Schema on users*/

list *
sql_trans_schema_user_dependencies(sql_trans *tr, int schema_id)
{
	void *v;
	sql_schema * s = find_sql_schema(tr, "sys");
	sql_table *auths = find_sql_table(s, "auths");
	sql_column *auth_id = find_sql_column(auths, "id");
	short type = USER_DEPENDENCY;
	list *l = list_create((fdestroy) GDKfree);
	rids *users = backend_schema_user_dependencies(tr, schema_id);
	oid rid;
	
	for(rid = table_funcs.rids_next(users); rid != oid_nil; rid = table_funcs.rids_next(users)) {
		v = table_funcs.column_find_value(tr, auth_id, rid);
		list_append(l,v);
		v = MNEW(sht);
		if(v)
			*(sht*)v = type;
		list_append(l,v);
	}
	table_funcs.rids_destroy(users);
	return l;
}

/*owner on schemas*/
list *
sql_trans_owner_schema_dependencies(sql_trans *tr, int owner_id)
{
	void *v;
	sql_schema * s = find_sql_schema(tr, "sys");
	sql_table *schemas = find_sql_table(s, "schemas");
	sql_column *schema_owner = find_sql_column(schemas, "authorization");
	sql_column *schema_id = find_sql_column(schemas, "id");
	short type = SCHEMA_DEPENDENCY;
	list *l = list_create((fdestroy) GDKfree);
	rids *rs = table_funcs.rids_select(tr, schema_owner, &owner_id, &owner_id, NULL);
	oid rid;
	
	for(rid = table_funcs.rids_next(rs); rid != oid_nil; rid = table_funcs.rids_next(rs)) {
		v = table_funcs.column_find_value(tr, schema_id, rid);
		list_append(l, v);
		v = MNEW(sht);
		if(v)
			*(sht*)v = type;
		list_append(l,v);
	}
	table_funcs.rids_destroy(rs);
	return l;
}

/*Function on Functions*/



