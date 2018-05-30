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
#include "rel_optimizer.h"
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
				if(col) {
					int *cnr = sa_alloc(cols->sa, sizeof(int));
					*cnr = col->colnr;
					list_append(cols, cnr);
				}
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

extern list *rel_dependencies(sql_allocator *sa, sql_rel *r);

str
bootstrap_partition_expression(mvc* sql, sql_table *mt, int instantiate)
{
	sql_exp *exp;
	char *query, *msg = NULL;
	int sql_ec;
	sql_rel *r;

	assert(isPartitionedByExpressionTable(mt) && mt->part.pexp->cols);

	r = rel_basetable(sql, mt, mt->base.name);
	query = mt->part.pexp->exp;
	if((exp = rel_parse_val(sql, sa_message(sql->sa, "select %s;", query), sql->emode, r)) == NULL) {
		if(*sql->errstr) {
			if (strlen(sql->errstr) > 6 && sql->errstr[5] == '!')
				throw(SQL, "sql.partition", "%s", sql->errstr);
			else
				throw(SQL, "sql.partition", SQLSTATE(42000) "%s", sql->errstr);
		}
		throw(SQL,"sql.partition", SQLSTATE(42000) "Incorrect expression '%s'", query);
	}

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

	if(instantiate) {
		r = rel_project(sql->sa, r, NULL);
		r->exps = sa_list(sql->sa);
		list_append(r->exps, exp);

		if (r)
			r = rel_optimizer(sql, r, 0);
		if (r) {
			int i;
			node *n, *found = NULL;
			list *id_l = rel_dependencies(sql->sa, r);
			for(i = 0, n = id_l->h ; n ; n = n->next, i++) { //remove the table itself from the list of dependencies
				if(*(int *) n->data == mt->base.id)
					found = n;
			}
			assert(found);
			list_remove_node(id_l, found);
			mvc_create_dependencies(sql, id_l, mt->base.id, TABLE_DEPENDENCY);
		}
	}

	return msg;
}

void
find_partition_type(sql_subtype *tpe, sql_table *mt)
{
	if(isPartitionedByColumnTable(mt)) {
		*tpe = mt->part.pcol->type;
	} else if(isPartitionedByExpressionTable(mt)) {
		*tpe = mt->part.pexp->type;
	} else {
		assert(0);
	}
}

str
initialize_sql_parts(mvc* sql, sql_table *mt)
{
	str res = NULL;
	sql_subtype found;
	int localtype;

	if(isPartitionedByExpressionTable(mt) && (res = bootstrap_partition_expression(sql, mt, 0)) != NULL)
		return res;
	find_partition_type(&found, mt);
	localtype = found.type->localtype;

	if(localtype != TYPE_str && mt->members.set && list_length(mt->members.set)) {
		list *new = sa_list(sql->sa), *old = sa_list(sql->sa);

		for (node *n = mt->members.set->h; n; n = n->next) {
			sql_part* next = (sql_part*) n->data, *p = SA_ZNEW(sql->sa, sql_part);
			sql_table* pt = find_sql_table(mt->s, next->base.name);

			base_init(sql->sa, &p->base, pt->base.id, TR_NEW, pt->base.name);
			p->t = pt;
			p->tpe = found;
			p->with_nills = next->with_nills;

			if(isListPartitionTable(mt)) {
				p->part_type = PARTITION_LIST;
				p->part.values = sa_list(sql->sa);

				for (node *m = next->part.values->h; m; m = m->next) {
					sql_part_value *v = (sql_part_value*) m->data, *nv = SA_ZNEW(sql->sa, sql_part_value);
					ValRecord vvalue;
					ptr ok;

					nv->tpe = found;
					memset(&vvalue, 0, sizeof(ValRecord));
					ok = VALinit(&vvalue, TYPE_str, v->value);
					if(ok)
						ok = VALconvert(localtype, &vvalue);
					if(ok) {
						nv->value = sa_alloc(sql->sa, vvalue.len);
						memcpy(nv->value, VALget(&vvalue), vvalue.len);
						nv->length = vvalue.len;
					}
					list_append(p->part.values, nv);
					VALclear(&vvalue);
					if(!ok) {
						res = createException(SQL, "sql.partition",
											  SQLSTATE(42000) "Internal error while bootstrapping partitioned tables");
						goto finish;
					}
				}
			} else if(isRangePartitionTable(mt)) {
				ValRecord vmin, vmax;
				ptr ok;

				p->part_type = PARTITION_RANGE;
				memset(&vmin, 0, sizeof(ValRecord));
				memset(&vmax, 0, sizeof(ValRecord));
				ok = VALinit(&vmin, TYPE_str, next->part.range.minvalue);
				if(ok)
					ok = VALconvert(localtype, &vmin);
				if(ok)
					ok = VALinit(&vmax, TYPE_str, next->part.range.maxvalue);
				if(ok)
					ok = VALconvert(localtype, &vmax);
				if(ok) {
					p->part.range.minvalue = sa_alloc(sql->sa, vmin.len);
					p->part.range.maxvalue = sa_alloc(sql->sa, vmax.len);
					memcpy(p->part.range.minvalue, VALget(&vmin), vmin.len);
					memcpy(p->part.range.maxvalue, VALget(&vmax), vmax.len);
					p->part.range.minlength = vmin.len;
					p->part.range.maxlength = vmax.len;
				}
				VALclear(&vmin);
				VALclear(&vmax);
				if(!ok) {
					res = createException(SQL, "sql.partition",
										  SQLSTATE(42000) "Internal error while bootstrapping partitioned tables");
					goto finish;
				}
			}
			list_append(new, p);
			list_append(old, next);
		}
		for (node *n = old->h; n; n = n->next) { //remove the old
			sql_part* next = (sql_part*) n->data;
			sql_table* pt = find_sql_table(mt->s, next->base.name);

			pt->p = NULL;
			cs_del(&mt->members, n, next->base.flag);
			sql_trans_drop_dependency(sql->session->tr, next->base.id, mt->base.id, TABLE_DEPENDENCY);
		}
		for (node *n = new->h; n; n = n->next) {
			sql_part* next = (sql_part*) n->data;
			sql_table* pt = find_sql_table(mt->s, next->base.name);
			sql_part *err = NULL;

			pt->p = mt;
			if(isRangePartitionTable(mt)) {
				err = cs_add_with_validate(&mt->members, next, TR_NEW, sql_range_part_validate_and_insert);
			} else if(isListPartitionTable(mt)) {
				err = cs_add_with_validate(&mt->members, next, TR_NEW, sql_values_part_validate_and_insert);
			} else {
				assert(0);
			}
			if(err) {
				res = createException(SQL, "sql.partition",
									  SQLSTATE(42000) "Internal error while bootstrapping partitioned tables");
				goto finish;
			}
			pt->s->base.wtime = pt->base.wtime = sql->session->tr->wtime = sql->session->tr->wstime;
			sql_trans_create_dependency(sql->session->tr, pt->base.id, mt->base.id, TABLE_DEPENDENCY);
		}
		mt->s->base.wtime = mt->base.wtime = sql->session->tr->wtime = sql->session->tr->wstime;
	}
finish:
	return res;
}
