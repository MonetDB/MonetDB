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


/*#define DEBUG*/

#include "monetdb_config.h"
#include "rel_distribute.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_dump.h"
#include "rel_select.h"
#include "rel_updates.h"
#include "sql_env.h"

static sql_rel *
distribute(mvc *sql, sql_rel *rel) 
{
	sql_rel *l = NULL;//, *r;
	prop *p;

	if (!rel)
		return rel;

	switch (rel->op) {
	case op_basetable: {
		sql_table *t = rel->l;

		/* set_remote() */
		if (isRemote(t)) {
			char *uri = t->query;

			p = rel->p = prop_create(sql->sa, PROP_REMOTE, rel->p); 
			p->value = uri;
		}
	}
	case op_table:
		break;
	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 

	case op_semi: 
	case op_anti: 

	case op_union: 
	case op_inter: 
	case op_except: 
		rel->l = distribute(sql, rel->l);
		rel->r = distribute(sql, rel->r);
		break;
	case op_project:
	case op_select: 
	case op_groupby: 
	case op_topn: 
	case op_sample: 
		rel->l = distribute(sql, rel->l);
		l = rel->l;
		if (l && (p = find_prop(l->p, PROP_REMOTE)) != NULL) {
			l->p = prop_remove(l->p, p);
			p->p = rel->p;
			rel->p = p;
		}
		break;
	case op_ddl: 
		rel->l = distribute(sql, rel->l);
		if (rel->r)
			rel->r = distribute(sql, rel->r);
		break;
	case op_insert:
	case op_update:
	case op_delete:
		rel->r = distribute(sql, rel->r);
		break;
	}
	return rel;
}

static sql_rel *
rel_remote_func(mvc *sql, sql_rel *rel)
{
	if (!rel)
		return rel;

	switch (rel->op) {
	case op_basetable: 
	case op_table:
		break;
	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 

	case op_semi: 
	case op_anti: 

	case op_union: 
	case op_inter: 
	case op_except: 
		rel->l = rel_remote_func(sql, rel->l);
		rel->r = rel_remote_func(sql, rel->r);
		break;
	case op_project:
	case op_select: 
	case op_groupby: 
	case op_topn: 
	case op_sample: 
		rel->l = rel_remote_func(sql, rel->l);
		break;
	case op_ddl: 
		rel->l = rel_remote_func(sql, rel->l);
		if (rel->r)
			rel->r = rel_remote_func(sql, rel->r);
		break;
	case op_insert:
	case op_update:
	case op_delete:
		rel->r = rel_remote_func(sql, rel->r);
		break;
	}
	if (find_prop(rel->p, PROP_REMOTE) != NULL) {
		list *exps = rel_projections(sql, rel, NULL, 1, 1);
		rel = rel_relational_func(sql->sa, rel, exps);
	}
	return rel;
}

sql_rel *
rel_distribute(mvc *sql, sql_rel *rel) 
{
	rel = distribute(sql, rel);
	return rel_remote_func(sql, rel);
}
