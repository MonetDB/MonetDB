/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"

#include "sql_partition.h"
#include "rel_rel.h"
#include "sql_mvc.h"
#include "sql_catalog.h"
#include "sql_relation.h"
#include "rel_updates.h"
#include "mal_exception.h"

static void exp_find_table_columns(mvc *sql, sql_exp *e, sql_table *t, list *cols);

static void
rel_find_table_columns(mvc* sql, sql_rel* rel, sql_table *t, list *cols)
{
	if(!rel)
		return;

	if(rel->exps)
		for(node *n = rel->exps->h ; n ; n = n->next)
			exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
	if(rel->l)
		rel_find_table_columns(sql, rel->l, t, cols);
	if(rel->r)
		rel_find_table_columns(sql, rel->r, t, cols);
}

static void
exp_find_table_columns(mvc *sql, sql_exp *e, sql_table *t, list *cols)
{
	if (!e)
		return;
	switch(e->type) {
		case e_psm: {
			if (e->flag & PSM_RETURN) {
				for(node *n = ((list*)e->r)->h ; n ; n = n->next)
					exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
			} else if (e->flag & PSM_WHILE) {
				exp_find_table_columns(sql, e->l, t, cols);
				for(node *n = ((list*)e->r)->h ; n ; n = n->next)
					exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
			} else if (e->flag & PSM_IF) {
				exp_find_table_columns(sql, e->l, t, cols);
				for(node *n = ((list*)e->r)->h ; n ; n = n->next)
					exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
				if (e->f)
					for(node *n = ((list*)e->f)->h ; n ; n = n->next)
						exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
			} else if (e->flag & PSM_REL) {
				rel_find_table_columns(sql, e->l, t, cols);
			} else if (e->flag & PSM_EXCEPTION) {
				exp_find_table_columns(sql, e->l, t, cols);
			}
		} break;
		case e_convert: {
			exp_find_table_columns(sql, e->l, t, cols);
		} break;
		case e_atom:
			break;
		case e_func: {
			for(node *n = ((list*)e->l)->h ; n ; n = n->next)
				exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
			if (e->r)
				for(node *n = ((list*)e->r)->h ; n ; n = n->next)
					exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
		} 	break;
		case e_aggr: {
			if (e->l)
				for(node *n = ((list*)e->l)->h ; n ; n = n->next)
					exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
		} 	break;
		case e_column: {
			if(!strcmp(e->l, t->base.name)) {
				sql_column *col = find_sql_column(t, e->r);
				if(col)
					list_append(cols, col);
			}
		} break;
		case e_cmp: {
			if (e->flag == cmp_in || e->flag == cmp_notin) {
				exp_find_table_columns(sql, e->l, t, cols);
				for(node *n = ((list*)e->r)->h ; n ; n = n->next)
					exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
			} else if (get_cmp(e) == cmp_or || get_cmp(e) == cmp_filter) {
				for(node *n = ((list*)e->l)->h ; n ; n = n->next)
					exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
				for(node *n = ((list*)e->r)->h ; n ; n = n->next)
					exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
			} else {
				if(e->l)
					exp_find_table_columns(sql, e->l, t, cols);
				if(e->r)
					exp_find_table_columns(sql, e->r, t, cols);
				if(e->f)
					exp_find_table_columns(sql, e->f, t, cols);
			}
		} break;
	}
}

static str
find_expression_type(sql_exp *e, sql_subtype *tpe, char *query)
{
	if (!e)
		throw(SQL,"sql.partition", SQLSTATE(42000) "It was not possible to compile the expression: '%s'", query);

	switch(e->type) {
		case e_convert: {
			assert(list_length(e->r) == 2);
			*tpe = *(sql_subtype *)list_fetch(e->r, 1);
		} break;
		case e_atom: {
			if (e->l) {
				atom *a = e->l;
				*tpe = a->tpe;
			} else if (e->r) {
				*tpe = e->tpe;
			} else if (e->f) {
				throw(SQL,"sql.partition", SQLSTATE(42000) "The expression should return 1 value only");
			}
		} break;
		case e_func: {
			sql_subfunc *f = e->f;
			sql_func *func = f->func;
			if(list_length(func->res) != 1)
				throw(SQL,"sql.partition", SQLSTATE(42000) "The expression should return 1 value only");
			*tpe = *(sql_subtype *)f->res->h->data;
		} 	break;
		case e_cmp: {
			sql_subtype *other = sql_bind_localtype("bit");
			*tpe = *other;
		} break;
		case e_column: {
			*tpe = e->tpe;
		} break;
		case e_psm:
			throw(SQL,"sql.partition", SQLSTATE(42000) "PSM calls not supported in expressions");
		case e_aggr:
			throw(SQL,"sql.partition", SQLSTATE(42000) "Aggregation functions not supported in expressions");
	}
	return NULL;
}

static str
bootstrap_partition_expression(mvc* sql, sql_table *mt)
{
	sql_exp *exp;
	char *query, *msg = NULL;
	int sql_ec;
	sql_rel* baset;

	assert(isPartitionedByExpressionTable(mt));

	baset = rel_basetable(sql, mt, mt->base.name);
	query = mt->part.pexp->exp;
	if((exp = rel_parse_val(sql, sa_message(sql->sa, "select %s;", query), sql->emode, baset)) == NULL) {
		throw(SQL,"sql.partition", SQLSTATE(42000) "Incorrect expression '%s'", query);
	}

	if(!mt->part.pexp->cols)
		mt->part.pexp->cols = sa_list(sql->sa);
	exp_find_table_columns(sql, exp, mt, mt->part.pexp->cols);

	if((msg = find_expression_type(exp, &(mt->part.pexp->type), query)) != NULL)
		return msg;

	sql_ec = mt->part.pexp->type.type->eclass;
	if(!(sql_ec == EC_BIT || EC_VARCHAR(sql_ec) || EC_TEMP(sql_ec) || EC_NUMBER(sql_ec) || sql_ec == EC_BLOB)) {
		char *err = sql_subtype_string(&(mt->part.pexp->type));
		if (!err) {
			throw(SQL, "sql.partition", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		} else {
			msg = createException(SQL, "sql.partition",
								  SQLSTATE(42000) "Column type %s not yet supported for the expression return value", err);
			GDKfree(err);
		}
	}
	return msg;
}

str
find_partition_type(mvc* sql, sql_subtype *tpe, sql_table *mt)
{
	str res = NULL;

	if(isPartitionedByColumnTable(mt)) {
		*tpe = mt->part.pcol->type;
	} else if(isPartitionedByExpressionTable(mt)) {
		if(!mt->part.pexp->type.type && (res = bootstrap_partition_expression(sql, mt)) != NULL)
			return res;
		*tpe = mt->part.pexp->type;
	} else {
		assert(0);
	}
	return res;
}
