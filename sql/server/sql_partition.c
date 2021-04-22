/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"

#include "sql_partition.h"
#include "rel_rel.h"
#include "rel_basetable.h"
#include "rel_exp.h"
#include "sql_mvc.h"
#include "sql_catalog.h"
#include "sql_relation.h"
#include "rel_unnest.h"
#include "rel_optimizer.h"
#include "rel_updates.h"
#include "mal_exception.h"

static int
key_column_colnr(sql_kc *pkey)
{
	return pkey->c->colnr;
}

static int
table_column_colnr(int *colnr)
{
	return *colnr;
}

str
sql_partition_validate_key(mvc *sql, sql_table *nt, sql_key *k, const char* op)
{
	if (k->type != fkey) {
		const char *keys = (k->type == pkey) ? "primary" : "unique";
		assert(k->type == pkey || k->type == ukey);

		if (isPartitionedByColumnTable(nt)) {
			assert(nt->part.pcol);
			if (list_length(k->columns) != 1) {
				throw(SQL, "sql.partition", SQLSTATE(42000) "%s TABLE: %s.%s: in a partitioned table, the %s key's "
					  "columns must match the columns used in the partition definition", op, nt->s->base.name,
					  nt->base.name, keys);
			} else {
				sql_kc *kcol = k->columns->h->data;
				if (kcol->c->colnr != nt->part.pcol->colnr)
					throw(SQL, "sql.partition", SQLSTATE(42000) "%s TABLE: %s.%s: in a partitioned table, the %s key's "
						  "columns must match the columns used in the partition definition", op, nt->s->base.name,
						  nt->base.name, keys);
			}
		} else if (isPartitionedByExpressionTable(nt)) {
			list *kcols, *pcols;
			sql_allocator *p1, *p2;

			assert(nt->part.pexp->cols);
			if (list_length(k->columns) != list_length(nt->part.pexp->cols))
				throw(SQL, "sql.partition", SQLSTATE(42000) "%s TABLE: %s.%s: in a partitioned table, the %s key's "
					  "columns must match the columns used in the partition definition", op, nt->s->base.name,
					  nt->base.name, keys);

			p1 = k->columns->sa; /* save the original sql allocators */
			p2 = nt->part.pexp->cols->sa;
			k->columns->sa = sql->sa;
			nt->part.pexp->cols->sa = sql->sa;
			kcols = list_sort(k->columns, (fkeyvalue)&key_column_colnr, NULL);
			pcols = list_sort(nt->part.pexp->cols, (fkeyvalue)&table_column_colnr, NULL);
			k->columns->sa = p1;
			nt->part.pexp->cols->sa = p2;

			for (node *nn = kcols->h, *mm = pcols->h; nn && mm; nn = nn->next, mm = mm->next) {
				sql_kc *kcol = nn->data;
				int *colnr = mm->data;
				if (kcol->c->colnr != *colnr)
					throw(SQL, "sql.partition", SQLSTATE(42000) "%s TABLE: %s.%s: in a partitioned table, the %s key's "
						  "columns must match the columns used in the partition definition", op, nt->s->base.name,
						  nt->base.name, keys);
			}
		}
	}
	return NULL;
}

static void exp_find_table_columns(mvc *sql, sql_exp *e, sql_table *t, list *cols);

static void
rel_find_table_columns(mvc* sql, sql_rel* rel, sql_table *t, list *cols)
{
	if (THRhighwater()) {
		(void) sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return;
	}

	if (!rel)
		return;

	if (rel->exps)
		for (node *n = rel->exps->h ; n ; n = n->next)
			exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);

	switch (rel->op) {
		case op_basetable:
		case op_truncate:
			break;
		case op_table:
			if (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION) {
				if (rel->l)
					rel_find_table_columns(sql, rel->l, t, cols);
			}
			break;
		case op_join:
		case op_left:
		case op_right:
		case op_full:
		case op_union:
		case op_inter:
		case op_except:
			if (rel->l)
				rel_find_table_columns(sql, rel->l, t, cols);
			if (rel->r)
				rel_find_table_columns(sql, rel->r, t, cols);
			break;
		case op_semi:
		case op_anti:
		case op_groupby:
		case op_project:
		case op_select:
		case op_topn:
		case op_sample:
			if (rel->l)
				rel_find_table_columns(sql, rel->l, t, cols);
			break;
		case op_insert:
		case op_update:
		case op_delete:
			if (rel->r)
				rel_find_table_columns(sql, rel->r, t, cols);
			break;
		case op_ddl:
			if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
				if (rel->l)
					rel_find_table_columns(sql, rel->l, t, cols);
			} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
				if (rel->l)
					rel_find_table_columns(sql, rel->l, t, cols);
				if (rel->r)
					rel_find_table_columns(sql, rel->r, t, cols);
			}
			break;
	}
}

static void
exp_find_table_columns(mvc *sql, sql_exp *e, sql_table *t, list *cols)
{
	if (THRhighwater()) {
		(void) sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return;
	}

	if (!e)
		return;
	switch (e->type) {
		case e_psm: {
			if (e->flag & PSM_RETURN) {
				for (node *n = ((list*)e->r)->h ; n ; n = n->next)
					exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
			} else if (e->flag & PSM_WHILE) {
				exp_find_table_columns(sql, e->l, t, cols);
				for (node *n = ((list*)e->r)->h ; n ; n = n->next)
					exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
			} else if (e->flag & PSM_IF) {
				exp_find_table_columns(sql, e->l, t, cols);
				for (node *n = ((list*)e->r)->h ; n ; n = n->next)
					exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
				if (e->f)
					for (node *n = ((list*)e->f)->h ; n ; n = n->next)
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
		case e_atom: {
			if (e->f)
				for (node *n = ((list*)e->f)->h ; n ; n = n->next)
					exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
		} break;
		case e_aggr:
		case e_func: {
			if (e->l)
				for (node *n = ((list*)e->l)->h ; n ; n = n->next)
					exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
			if (e->type == e_func && e->r)
				for (node *n = ((list*)e->r)->h ; n ; n = n->next)
					exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
		} break;
		case e_column: {
			if (!strcmp(e->l, t->base.name)) {
				sql_column *col = find_sql_column(t, e->r);
				if (col) {
					int *cnr = SA_NEW(cols->sa, int);
					*cnr = col->colnr;
					list_append(cols, cnr);
				}
			}
		} break;
		case e_cmp: {
			if (e->flag == cmp_in || e->flag == cmp_notin) {
				exp_find_table_columns(sql, e->l, t, cols);
				for (node *n = ((list*)e->r)->h ; n ; n = n->next)
					exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
			} else if (e->flag == cmp_or || e->flag == cmp_filter) {
				for (node *n = ((list*)e->l)->h ; n ; n = n->next)
					exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
				for (node *n = ((list*)e->r)->h ; n ; n = n->next)
					exp_find_table_columns(sql, (sql_exp*) n->data, t, cols);
			} else {
				if (e->l)
					exp_find_table_columns(sql, e->l, t, cols);
				if (e->r)
					exp_find_table_columns(sql, e->r, t, cols);
				if (e->f)
					exp_find_table_columns(sql, e->f, t, cols);
			}
		} break;
	}
}

str
bootstrap_partition_expression(mvc *sql, sql_table *mt, int instantiate)
{
	sql_exp *exp;
	char *query, *msg = NULL;
	sql_class sql_ec;
	sql_rel *r;

	assert(isPartitionedByExpressionTable(mt));

	if (sql->emode == m_prepare)
		throw(SQL,"sql.partition", SQLSTATE(42000) "Partition expressions not compilable with prepared statements");

	r = rel_basetable(sql, mt, mt->base.name);
	query = mt->part.pexp->exp;
	if (!(exp = rel_parse_val(sql, mt->s, query, NULL, sql->emode, r))) {
		if (*sql->errstr) {
			if (strlen(sql->errstr) > 6 && sql->errstr[5] == '!')
				throw(SQL, "sql.partition", "%s", sql->errstr);
			else
				throw(SQL, "sql.partition", SQLSTATE(42000) "%s", sql->errstr);
		}
		throw(SQL,"sql.partition", SQLSTATE(42000) "Incorrect expression '%s'", query);
	}

	assert(mt->part.pexp->cols);
	exp_find_table_columns(sql, exp, mt, mt->part.pexp->cols);

	mt->part.pexp->type = *exp_subtype(exp);
	sql_ec = mt->part.pexp->type.type->eclass;
	if (!(sql_ec == EC_BIT || EC_VARCHAR(sql_ec) || EC_TEMP(sql_ec) || sql_ec == EC_POS || sql_ec == EC_NUM ||
		 EC_INTERVAL(sql_ec)|| sql_ec == EC_DEC || sql_ec == EC_BLOB)) {
		char *err = sql_subtype_string(sql->ta, &(mt->part.pexp->type));
		if (!err) {
			throw(SQL, "sql.partition", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		} else {
			msg = createException(SQL, "sql.partition",
								  SQLSTATE(42000) "Column type %s not supported for the expression return value", err);
		}
	}

	if (instantiate) {
		r = rel_project(sql->sa, r, NULL);
		sql_rel *base = r->l, *nr = r;
		r->l = NULL; /* omit table from list of dependencies */
		(void) rel_project_add_exp(sql, r, exp);

		nr = sql_processrelation(sql, nr, 0, 0);
		if (nr) {
			list *id_l = rel_dependencies(sql, nr);
			mvc_create_dependencies(sql, id_l, mt->base.id, FUNC_DEPENDENCY);
		}
		r->l = base;
	}

	return msg;
}

void
find_partition_type(sql_subtype *tpe, sql_table *mt)
{
	if (isPartitionedByColumnTable(mt)) {
		*tpe = mt->part.pcol->type;
	} else if (isPartitionedByExpressionTable(mt)) {
		*tpe = mt->part.pexp->type;
	} else {
		assert(0);
	}
}

str
initialize_sql_parts(mvc *sql, sql_table *mt)
{
	str res = NULL;
	sql_subtype found;
	int localtype;
	sql_trans *tr = sql->session->tr;

	if (isPartitionedByExpressionTable(mt) && (res = bootstrap_partition_expression(sql, mt, 0)) != NULL)
		return res;

	find_partition_type(&found, mt);
	localtype = found.type->localtype;

	if (localtype != TYPE_str && mt->members && list_length(mt->members)) {
		for (node *n = mt->members->h; n; n = n->next) {
			sql_part *p = n->data;

			if (isListPartitionTable(mt)) {
				for (node *m = p->part.values->h; m; m = m->next) {
					sql_part_value *v = (sql_part_value*) m->data, ov = *v;
					ValRecord vvalue;
					ptr ok;

					vvalue = (ValRecord) {.vtype = TYPE_void,};
					ok = VALinit(&vvalue, TYPE_str, v->value);
					if (ok)
						ok = VALconvert(localtype, &vvalue);
					if (ok) {
						v->value = SA_NEW_ARRAY(tr->sa, char, vvalue.len);
						memcpy(v->value, VALget(&vvalue), vvalue.len);
						v->length = vvalue.len;
					}
					VALclear(&vvalue);
					if (!ok) {
						res = createException(SQL, "sql.partition",
											  SQLSTATE(42000) "Internal error while bootstrapping partitioned tables");
						return res;
					}
					_DELETE(ov.value);
				}
			} else if (isRangePartitionTable(mt)) {
				ValRecord vmin, vmax;
				ptr ok;

				vmin = vmax = (ValRecord) {.vtype = TYPE_void,};
				ok = VALinit(&vmin, TYPE_str, p->part.range.minvalue);
				if (ok)
					ok = VALinit(&vmax, TYPE_str, p->part.range.maxvalue);
				_DELETE(p->part.range.minvalue);
				_DELETE(p->part.range.maxvalue);
				if (ok) {
					if (strNil((const char *)VALget(&vmin)) &&
						strNil((const char *)VALget(&vmax))) {
						const void *nil_ptr = ATOMnilptr(localtype);
						size_t nil_len = ATOMlen(localtype, nil_ptr);

						p->part.range.minvalue = SA_NEW_ARRAY(tr->sa, char, nil_len);
						p->part.range.maxvalue = SA_NEW_ARRAY(tr->sa, char, nil_len);
						memcpy(p->part.range.minvalue, nil_ptr, nil_len);
						memcpy(p->part.range.maxvalue, nil_ptr, nil_len);
						p->part.range.minlength = nil_len;
						p->part.range.maxlength = nil_len;
					} else {
						ok = VALconvert(localtype, &vmin);
						if (ok)
							ok = VALconvert(localtype, &vmax);
						if (ok) {
							p->part.range.minvalue = SA_NEW_ARRAY(tr->sa, char, vmin.len);
							p->part.range.maxvalue = SA_NEW_ARRAY(tr->sa, char, vmax.len);
							memcpy(p->part.range.minvalue, VALget(&vmin), vmin.len);
							memcpy(p->part.range.maxvalue, VALget(&vmax), vmax.len);
							p->part.range.minlength = vmin.len;
							p->part.range.maxlength = vmax.len;
						}
					}
				}
				VALclear(&vmin);
				VALclear(&vmax);
				if (!ok) {
					res = createException(SQL, "sql.partition",
										  SQLSTATE(42000) "Internal error while bootstrapping partitioned tables");
					return res;
				}
			}
		}
	}
	return res;
}
