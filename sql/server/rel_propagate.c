/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_propagate.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_dump.h"
#include "rel_select.h"
#include "rel_updates.h"
#include "sql_mvc.h"
#include "sql_partition.h"

extern sql_rel *rel_list(sql_allocator *sa, sql_rel *l, sql_rel *r);

static sql_exp*
rel_generate_anti_expression(mvc *sql, sql_rel **anti_rel, sql_table *mt, sql_table *pt)
{
	sql_exp* res = NULL;

	*anti_rel = rel_basetable(sql, pt, pt->base.name);

	if (isPartitionedByColumnTable(mt)) {
		int colr = mt->part.pcol->colnr;
		res = list_fetch((*anti_rel)->exps, colr);
		res = exp_ref(sql->sa, res);
	} else if (isPartitionedByExpressionTable(mt)) {
		*anti_rel = rel_project(sql->sa, *anti_rel, NULL);
		if (!(res = rel_parse_val(sql, sa_message(sql->sa, "select %s;", mt->part.pexp->exp), sql->emode, (*anti_rel)->l)))
			return NULL;
	} else {
		assert(0);
	}
	(*anti_rel)->exps = new_exp_list(sql->sa);
	if (!exp_name(res))
		exp_label(sql->sa, res, ++sql->label);
	append((*anti_rel)->exps, res);
	res = exp_ref(sql->sa, res);
	return res;
}

static sql_rel*
rel_create_common_relation(mvc *sql, sql_rel *rel, sql_table *t)
{
	if (isPartitionedByColumnTable(t)) {
		return rel_dup(rel->r);
	} else if (isPartitionedByExpressionTable(t)) {
		sql_rel *inserts;
		list *l = new_exp_list(sql->sa);

		rel->r = rel_project(sql->sa, rel->r, l);
		set_processed((sql_rel*)rel->r);
		inserts = ((sql_rel*)(rel->r))->l;
		for (node *n = t->columns.set->h, *m = inserts->exps->h; n && m; n = n->next, m = m->next) {
			sql_column *col = n->data;
			sql_exp *before = m->data, *help;

			if (!exp_name(before))
				exp_label(sql->sa, before, ++sql->label);
			help = exp_ref(sql->sa, before);
			exp_setname(sql->sa, help, t->base.name, col->base.name);
			list_append(l, help);
		}
		return rel_dup(rel->r);
	} else {
		assert(0);
	}
	return NULL;
}

static sql_exp*
rel_generate_anti_insert_expression(mvc *sql, sql_rel **anti_rel, sql_table *t)
{
	sql_exp* res = NULL;

	if ((*anti_rel)->op != op_project && (*anti_rel)->op != op_basetable && (*anti_rel)->op != op_table) {
		sql_rel *inserts; //In a nested partition case the operation is a op_select, then a projection must be created
		list *l = new_exp_list(sql->sa);
		*anti_rel = rel_project(sql->sa, *anti_rel, l);

		inserts = (*anti_rel)->l;
		if (inserts->op != op_project && inserts->op != op_basetable && inserts->op != op_table)
			inserts = inserts->l;
		for (node *n = t->columns.set->h, *m = inserts->exps->h; n && m; n = n->next, m = m->next) {
			sql_column *col = n->data;
			sql_exp *before = m->data, *help;

			if (!exp_name(before))
				exp_label(sql->sa, before, ++sql->label);
			help = exp_ref(sql->sa, before);
			exp_setname(sql->sa, help, t->base.name, col->base.name);
			list_append(l, help);
		}
	}

	if (isPartitionedByColumnTable(t)) {
		int colr = t->part.pcol->colnr;
		res = list_fetch((*anti_rel)->exps, colr);
	} else if (isPartitionedByExpressionTable(t)) {
		*anti_rel = rel_project(sql->sa, *anti_rel, rel_projections(sql, *anti_rel, NULL, 1, 1));
		if (!(res = rel_parse_val(sql, sa_message(sql->sa, "select %s;", t->part.pexp->exp), sql->emode, (*anti_rel)->l)))
			return NULL;
		exp_label(sql->sa, res, ++sql->label);
		append((*anti_rel)->exps, res);
	} else {
		assert(0);
	}
	if (!exp_name(res))
		exp_label(sql->sa, res, ++sql->label);
	res = exp_ref(sql->sa, res);
	return res;
}

static void
generate_alter_table_error_message(char* buf, sql_table *mt)
{
	char *s1 = isRangePartitionTable(mt) ? "range":"list of values";
	if (isPartitionedByColumnTable(mt)) {
		sql_column* col = mt->part.pcol;
		snprintf(buf, BUFSIZ, "ALTER TABLE: there are values in the column %s outside the partition %s", col->base.name, s1);
	} else if (isPartitionedByExpressionTable(mt)) {
		snprintf(buf, BUFSIZ, "ALTER TABLE: there are values in the expression outside the partition %s", s1);
	} else {
		assert(0);
	}
}

static sql_exp *
generate_partition_limits(sql_query *query, sql_rel **r, symbol *s, sql_subtype tpe, bool nilok)
{
	mvc *sql = query->sql;
	if (!s) {
		return NULL;
	} else if (s->token == SQL_NULL ||
		   (!nilok &&
		    s->token == SQL_IDENT &&
		    s->data.lval->h->type == type_int &&
		    sql->args[s->data.lval->h->data.i_val]->isnull)) {
		return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: range bound cannot be null");
	} else if (s->token == SQL_MINVALUE) {
		atom *amin = atom_general(sql->sa, &tpe, NULL);
		if (!amin) {
			char *err = sql_subtype_string(&tpe);
			if (!err)
				return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: absolute minimum value not available for %s type", err);
			GDKfree(err);
			return NULL;
		}
		return exp_atom(sql->sa, amin);
	} else if (s->token == SQL_MAXVALUE) {
		atom *amax = atom_general(sql->sa, &tpe, NULL);
		if (!amax) {
			char *err = sql_subtype_string(&tpe);
			if (!err)
				return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: absolute maximum value not available for %s type", err);
			GDKfree(err);
			return NULL;
		}
		return exp_atom(sql->sa, amax);
	} else {
		exp_kind ek = {type_value, card_value, FALSE};
		sql_exp *e = rel_value_exp2(query, r, s, sql_sel | sql_values, ek);

		if (!e)
			return NULL;
		return rel_check_type(sql, &tpe, r ? *r : NULL, e, type_equal);
	}
}

static sql_rel*
create_range_partition_anti_rel(sql_query* query, sql_table *mt, sql_table *pt, bit with_nills, sql_exp *pmin, sql_exp *pmax, bool all_ranges)
{
	mvc *sql = query->sql;
	sql_rel *anti_rel;
	sql_exp *exception, *aggr, *anti_exp = NULL, *anti_le, *e1, *e2, *anti_nils;
	sql_subfunc *cf = sql_bind_func(sql->sa, sql->session->schema, "count", sql_bind_localtype("void"), NULL, F_AGGR);
	char buf[BUFSIZ];
	sql_subtype tpe;

	find_partition_type(&tpe, mt);

	anti_le = rel_generate_anti_expression(sql, &anti_rel, mt, pt);
	anti_nils = rel_unop_(sql, anti_rel, anti_le, NULL, "isnull", card_value);
	set_has_no_nil(anti_nils);
	if (pmin && pmax) {
		if (all_ranges) { /*if holds all values in range, don't generate the range comparison */
			assert(!with_nills);
		} else {
			sql_exp *range1, *range2;
			
			e1 = exp_copy(sql, pmin);
			if (subtype_cmp(exp_subtype(pmin), &tpe) != 0)
				e1 = exp_convert(sql->sa, e1, exp_subtype(e1), &tpe);

			e2 = exp_copy(sql, pmax);
			if (subtype_cmp(exp_subtype(e2), &tpe) != 0)
				e2 = exp_convert(sql->sa, e2, exp_subtype(e2), &tpe);

			range1 = exp_compare(sql->sa, exp_copy(sql, anti_le), e1, 3);
			range2 = exp_compare(sql->sa, exp_copy(sql, anti_le), e2, 1);
			anti_exp = exp_or(sql->sa, list_append(new_exp_list(sql->sa), range1),
							list_append(new_exp_list(sql->sa), range2), 0);
		}
		if (!with_nills) {
			anti_nils = exp_compare(sql->sa, anti_nils, exp_atom_bool(sql->sa, 1), cmp_equal);
			if (anti_exp)
				anti_exp = exp_or(sql->sa, list_append(new_exp_list(sql->sa), anti_exp),
								  list_append(new_exp_list(sql->sa), anti_nils), 0);
			else
				anti_exp = anti_nils;
		}
	} else {
		anti_exp = exp_compare(sql->sa, anti_nils, exp_atom_bool(sql->sa, 1), cmp_notequal);
	}

	anti_rel = rel_select(sql->sa, anti_rel, anti_exp);
	anti_rel = rel_groupby(sql, anti_rel, NULL);
	aggr = exp_aggr(sql->sa, NULL, cf, 0, 0, anti_rel->card, 0);
	(void) rel_groupby_add_aggr(sql, anti_rel, aggr);
	exp_label(sql->sa, aggr, ++sql->label);

	//generate the exception
	aggr = exp_ref(sql->sa, aggr);
	generate_alter_table_error_message(buf, mt);
	exception = exp_exception(sql->sa, aggr, buf);

	return rel_exception(sql->sa, NULL, anti_rel, list_append(new_exp_list(sql->sa), exception));
}

static sql_rel*
create_list_partition_anti_rel(sql_query* query, sql_table *mt, sql_table *pt, bit with_nills, list *anti_exps)
{
	mvc *sql = query->sql;
	sql_rel *anti_rel;
	sql_exp *exception, *aggr, *anti_exp, *anti_le, *anti_nils;
	sql_subfunc *cf = sql_bind_func(sql->sa, sql->session->schema, "count", sql_bind_localtype("void"), NULL, F_AGGR);
	char buf[BUFSIZ];
	sql_subtype tpe;

	find_partition_type(&tpe, mt);

	anti_le = rel_generate_anti_expression(sql, &anti_rel, mt, pt);
	anti_nils = rel_unop_(sql, anti_rel, anti_le, NULL, "isnull", card_value);

	set_has_no_nil(anti_nils);
	if (list_length(anti_exps) > 0) {
		anti_exp = exp_in(sql->sa, anti_le, anti_exps, cmp_notin);
		if (!with_nills) {
			anti_nils = exp_compare(sql->sa, anti_nils, exp_atom_bool(sql->sa, 1), cmp_equal);
			anti_exp = exp_or(sql->sa, append(new_exp_list(sql->sa), anti_exp),
							  append(new_exp_list(sql->sa), anti_nils), 0);
		}
	} else {
		assert(with_nills);
		anti_exp = exp_compare(sql->sa, anti_nils, exp_atom_bool(sql->sa, 1), cmp_notequal);
	}

	anti_rel = rel_select(sql->sa, anti_rel, anti_exp);
	anti_rel = rel_groupby(sql, anti_rel, NULL);
	aggr = exp_aggr(sql->sa, NULL, cf, 0, 0, anti_rel->card, 0);
	(void) rel_groupby_add_aggr(sql, anti_rel, aggr);
	exp_label(sql->sa, aggr, ++sql->label);

	//generate the exception
	aggr = exp_ref(sql->sa, aggr);
	generate_alter_table_error_message(buf, mt);
	exception = exp_exception(sql->sa, aggr, buf);

	return rel_exception(sql->sa, NULL, anti_rel, list_append(new_exp_list(sql->sa), exception));
}

static sql_rel *
propagate_validation_to_upper_tables(sql_query* query, sql_table *mt, sql_table *pt, sql_rel *rel)
{
	mvc *sql = query->sql;
	sql->caching = 0;
	for (sql_table *prev = mt, *it = prev->p ; it && prev ; prev = it, it = it->p) {
		sql_part *spt = find_sql_part(it, prev->base.name);
		if (spt) {
			if (isRangePartitionTable(it)) {
				int tpe = spt->tpe.type->localtype;
				int (*atomcmp)(const void *, const void *) = ATOMcompare(tpe);
				const void *nil = ATOMnilptr(tpe);
				sql_exp *e1 = NULL, *e2 = NULL;
				bool found_all = false;

				if (atomcmp(spt->part.range.minvalue, nil) != 0 && atomcmp(spt->part.range.maxvalue, nil) != 0) {
					e1 = create_table_part_atom_exp(sql, spt->tpe, spt->part.range.minvalue);
					e2 = create_table_part_atom_exp(sql, spt->tpe, spt->part.range.maxvalue);
				} else {
					assert(spt->with_nills);
					found_all = is_bit_nil(spt->with_nills);
				}
				if (!found_all || !spt->with_nills)
					rel = rel_list(sql->sa, rel, create_range_partition_anti_rel(query, it, pt, spt->with_nills, e1, e2, false));
			} else if (isListPartitionTable(it)) {
				list *exps = new_exp_list(sql->sa);
				for (node *n = spt->part.values->h ; n ; n = n->next) {
					sql_part_value *next = (sql_part_value*) n->data;
					sql_exp *e1 = create_table_part_atom_exp(sql, spt->tpe, next->value);
					list_append(exps, e1);
				}
				rel = rel_list(sql->sa, rel, create_list_partition_anti_rel(query, it, pt, spt->with_nills, exps));
			} else {
				assert(0);
			}
		} else { //the sql_part should exist
			assert(0);
		}
	}
	return rel;
}

sql_rel *
rel_alter_table_add_partition_range(sql_query* query, sql_table *mt, sql_table *pt, char *sname, char *tname, char *sname2,
									char *tname2, symbol* min, symbol* max, bit with_nills, int update)
{
	mvc *sql = query->sql;
	sql_rel *rel_psm = rel_create(sql->sa), *res;
	list *exps = new_exp_list(sql->sa);
	sql_exp *pmin, *pmax;
	sql_subtype tpe;
	bool all_ranges = false;

	if (!rel_psm || !exps)
		return NULL;

	find_partition_type(&tpe, mt);

	assert((!min && !max && with_nills) || (min && max));
	if (min && max) {
		pmin = generate_partition_limits(query, &rel_psm, min, tpe, false);
		pmax = generate_partition_limits(query, &rel_psm, max, tpe, false);
		if (!pmin || !pmax)
			return NULL;
		if (min->token == SQL_MINVALUE && max->token == SQL_MAXVALUE && with_nills)
			with_nills = bit_nil; /* holds all values in range */
		all_ranges = (min->token == SQL_MINVALUE && max->token == SQL_MAXVALUE);
	} else {
		pmin = exp_atom(sql->sa, atom_general(sql->sa, &tpe, NULL));
		pmax = exp_atom(sql->sa, atom_general(sql->sa, &tpe, NULL));
	}

	//generate the psm statement
	append(exps, exp_atom_clob(sql->sa, sname));
	append(exps, exp_atom_clob(sql->sa, tname));
	assert((sname2 && tname2) || (!sname2 && !tname2));
	if (sname2) {
		append(exps, exp_atom_clob(sql->sa, sname2));
		append(exps, exp_atom_clob(sql->sa, tname2));
	}
	append(exps, pmin);
	append(exps, pmax);
	append(exps, is_bit_nil(with_nills) ? exp_atom(sql->sa, atom_null_value(sql->sa, sql_bind_localtype("bit"))) : exp_atom_bool(sql->sa, with_nills));
	append(exps, exp_atom_int(sql->sa, update));
	rel_psm->l = NULL;
	rel_psm->r = NULL;
	rel_psm->op = op_ddl;
	rel_psm->flag = ddl_alter_table_add_range_partition;
	rel_psm->exps = exps;
	rel_psm->card = CARD_MULTI;
	rel_psm->nrcols = 0;

	if (!is_bit_nil(with_nills)) {
		res = create_range_partition_anti_rel(query, mt, pt, with_nills, (min && max) ? pmin : NULL, (min && max) ? pmax : NULL, all_ranges);
		res->l = rel_psm;
	} else {
		res = rel_psm;
	}

	return propagate_validation_to_upper_tables(query, mt, pt, res);
}

sql_rel *
rel_alter_table_add_partition_list(sql_query *query, sql_table *mt, sql_table *pt, char *sname, char *tname, char *sname2,
								   char *tname2, dlist* values, bit with_nills, int update)
{
	mvc *sql = query->sql;
	sql_rel *rel_psm = rel_create(sql->sa), *res;
	list *exps = new_exp_list(sql->sa), *anti_exps = new_exp_list(sql->sa), *lvals = new_exp_list(sql->sa);
	sql_subtype tpe;

	if (!rel_psm || !exps)
		return NULL;

	find_partition_type(&tpe, mt);

	if (values) {
		for (dnode *dn = values->h; dn ; dn = dn->next) { /* parse the atoms and generate the expressions */
			symbol* next = dn->data.sym;
			sql_exp *pnext = generate_partition_limits(query, &rel_psm, next, tpe, true);
			if (subtype_cmp(exp_subtype(pnext), &tpe) != 0)
				pnext = exp_convert(sql->sa, pnext, exp_subtype(pnext), &tpe);

			if (next->token == SQL_NULL)
				return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: a list value cannot be null");
			append(lvals, pnext);
			append(anti_exps, exp_copy(sql, pnext));
		}
	}

	//generate the psm statement
	append(exps, exp_atom_clob(sql->sa, sname));
	append(exps, exp_atom_clob(sql->sa, tname));
	assert((sname2 && tname2) || (!sname2 && !tname2));
	if (sname2) {
		append(exps, exp_atom_clob(sql->sa, sname2));
		append(exps, exp_atom_clob(sql->sa, tname2));
	}
	append(exps, exp_atom_bool(sql->sa, with_nills));
	append(exps, exp_atom_int(sql->sa, update));
	rel_psm->l = NULL;
	rel_psm->r = NULL;
	rel_psm->op = op_ddl;
	rel_psm->flag = ddl_alter_table_add_list_partition;
	rel_psm->exps = list_merge(exps, lvals, (fdup)NULL);
	rel_psm->card = CARD_MULTI;
	rel_psm->nrcols = 0;

	res = create_list_partition_anti_rel(query, mt, pt, with_nills, anti_exps);
	res->l = rel_psm;

	return propagate_validation_to_upper_tables(query, mt, pt, res);
}

static sql_rel* rel_change_base_table(mvc* sql, sql_rel* rel, sql_table* oldt, sql_table* newt);

static sql_exp*
exp_change_column_table(mvc *sql, sql_exp *e, sql_table* oldt, sql_table* newt)
{
	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (!e)
		return NULL;
	switch(e->type) {
		case e_psm: {
			if (e->flag & PSM_RETURN) {
				for (node *n = ((list*)e->r)->h ; n ; n = n->next)
					n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);
			} else if (e->flag & PSM_WHILE) {
				e->l = exp_change_column_table(sql, e->l, oldt, newt);
				for (node *n = ((list*)e->r)->h ; n ; n = n->next)
					n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);
			} else if (e->flag & PSM_IF) {
				e->l = exp_change_column_table(sql, e->l, oldt, newt);
				for (node *n = ((list*)e->r)->h ; n ; n = n->next)
					n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);
				if (e->f)
					for (node *n = ((list*)e->f)->h ; n ; n = n->next)
						n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);
			} else if (e->flag & PSM_REL) {
				rel_change_base_table(sql, e->l, oldt, newt);
			} else if (e->flag & PSM_EXCEPTION) {
				e->l = exp_change_column_table(sql, e->l, oldt, newt);
			}
		} break;
		case e_convert: {
			e->l = exp_change_column_table(sql, e->l, oldt, newt);
		} break;
		case e_atom:
			break;
		case e_func: {
			for (node *n = ((list*)e->l)->h ; n ; n = n->next)
				n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);
			if (e->r)
				for (node *n = ((list*)e->r)->h ; n ; n = n->next)
					n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);
		} 	break;
		case e_aggr: {
			if (e->l)
				for (node *n = ((list*)e->l)->h ; n ; n = n->next)
					n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);
		} 	break;
		case e_column: {
			if (!strcmp(e->l, oldt->base.name))
				e->l = sa_strdup(sql->sa, newt->base.name);
		} break;
		case e_cmp: {
			if (e->flag == cmp_in || e->flag == cmp_notin) {
				e->l = exp_change_column_table(sql, e->l, oldt, newt);
				for (node *n = ((list*)e->r)->h ; n ; n = n->next)
					n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);
			} else if (e->flag == cmp_or || e->flag == cmp_filter) {
				for (node *n = ((list*)e->l)->h ; n ; n = n->next)
					n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);
				for (node *n = ((list*)e->r)->h ; n ; n = n->next)
					n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);
			} else {
				if (e->l)
					e->l = exp_change_column_table(sql, e->l, oldt, newt);
				if (e->r)
					e->r = exp_change_column_table(sql, e->r, oldt, newt);
				if (e->f)
					e->f = exp_change_column_table(sql, e->f, oldt, newt);
			}
		} break;
	}
	if (exp_relname(e) && !strcmp(exp_relname(e), oldt->base.name))
		exp_setname(sql->sa, e, newt->base.name, NULL);
	return e;
}

static sql_rel*
rel_change_base_table(mvc* sql, sql_rel* rel, sql_table* oldt, sql_table* newt)
{
	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (!rel)
		return NULL;

	if (rel->exps)
		for (node *n = rel->exps->h ; n ; n = n->next)
			n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);

	switch (rel->op) {
		case op_ddl:
			break;
		case op_table:
		case op_basetable:
			if (rel->l == oldt)
				rel->l = newt;
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
			if (rel->l)
				rel->l = rel_change_base_table(sql, rel->l, oldt, newt);
			if (rel->r)
				rel->r = rel_change_base_table(sql, rel->r, oldt, newt);
			break;
		case op_groupby:
		case op_project:
		case op_select:
		case op_topn:
		case op_sample:
			if (rel->l)
				rel->l = rel_change_base_table(sql, rel->l, oldt, newt);
			break;
		case op_insert:
		case op_update:
		case op_delete:
		case op_truncate:
			if (rel->r)
				rel->r = rel_change_base_table(sql, rel->r, oldt, newt);
			break;
	}
	return rel;
}

static sql_rel *
rel_truncate_duplicate(mvc *sql, sql_rel *table, sql_rel *ori)
{
	sql_rel *r = rel_create(sql->sa);

	r->exps = exps_copy(sql, ori->exps);
	r->op = op_truncate;
	r->l = table;
	r->r = NULL;
	return r;
}

static sql_rel*
rel_generate_subdeletes(mvc *sql, sql_rel *rel, sql_table *t, int *changes)
{
	int just_one = 1;
	sql_rel *sel = NULL;

	for (node *n = t->members.set->h; n; n = n->next) {
		sql_part *pt = (sql_part *) n->data;
		sql_table *sub = find_sql_table(t->s, pt->base.name);
		sql_rel *s1, *dup = NULL;

		if (!update_allowed(sql, sub, sub->base.name, is_delete(rel->op) ? "DELETE": "TRUNCATE",
						   is_delete(rel->op) ? "delete": "truncate",  is_delete(rel->op) ? 1 : 2))
			return NULL;

		if (rel->r) {
			dup = rel_copy(sql, rel->r, 1);
			dup = rel_change_base_table(sql, dup, t, sub);
		}
		if (is_delete(rel->op))
			s1 = rel_delete(sql->sa, rel_basetable(sql, sub, sub->base.name), dup);
		else
			s1 = rel_truncate_duplicate(sql, rel_basetable(sql, sub, sub->base.name), rel);
		if (just_one == 0) {
			sel = rel_list(sql->sa, sel, s1);
		} else {
			sel = s1;
			just_one = 0;
		}
		(*changes)++;
	}
	return sel;
}

static sql_rel*
rel_generate_subupdates(mvc *sql, sql_rel *rel, sql_table *t, int *changes)
{
	int just_one = 1;
	sql_rel *sel = NULL;

	for (node *n = t->members.set->h; n; n = n->next) {
		sql_part *pt = (sql_part *) n->data;
		sql_table *sub = find_sql_table(t->s, pt->base.name);
		sql_rel *s1, *dup = NULL;
		list *uexps = exps_copy(sql, rel->exps);

		if (!update_allowed(sql, sub, sub->base.name, "UPDATE", "update", 0))
			return NULL;

		if (rel->r) {
			dup = rel_copy(sql, rel->r, 1);
			dup = rel_change_base_table(sql, dup, t, sub);
		}

		for (node *ne = uexps->h ; ne ; ne = ne->next)
			ne->data = exp_change_column_table(sql, (sql_exp*) ne->data, t, sub);

		s1 = rel_update(sql, rel_basetable(sql, sub, sub->base.name), dup, NULL, uexps);
		if (just_one == 0) {
			sel = rel_list(sql->sa, sel, s1);
		} else {
			sel = s1;
			just_one = 0;
		}
		(*changes)++;
	}

	return sel;
}

static sql_rel*
rel_generate_subinserts(sql_query *query, sql_rel *rel, sql_table *t, int *changes,
						const char *operation, const char *desc)
{
	mvc *sql = query->sql;
	int just_one = 1, found_nils = 0, found_all_range_values = 0;
	sql_rel *new_table = NULL, *sel = NULL, *anti_rel = NULL;
	sql_exp *anti_exp = NULL, *anti_le = NULL, *anti_nils = NULL, *accum = NULL, *aggr = NULL;
	list *anti_exps = new_exp_list(sql->sa);
	sql_subfunc *cf = sql_bind_func(sql->sa, sql->session->schema, "count", sql_bind_localtype("void"), NULL, F_AGGR);
	char buf[BUFSIZ];

	if (isPartitionedByColumnTable(t)) {
		anti_rel = rel_dup(rel->r);
	} else if (isPartitionedByExpressionTable(t)) {
		anti_rel = rel_create_common_relation(sql, rel, t);
	} else {
		assert(0);
	}
	anti_le = rel_generate_anti_insert_expression(sql, &anti_rel, t);

	for (node *n = t->members.set->h; n; n = n->next) {
		sql_part *pt = (sql_part *) n->data;
		sql_table *sub = find_sql_table(t->s, pt->base.name);
		sql_rel *s1 = NULL, *dup = NULL;
		sql_exp *le = NULL;

		if (!insert_allowed(sql, sub, sub->base.name, "INSERT", "insert"))
			return NULL;

		if (isPartitionedByColumnTable(t)) {
			dup = rel_dup(rel->r);
			le = rel_generate_anti_insert_expression(sql, &dup, t);
		} else if (isPartitionedByExpressionTable(t)) {
			dup = rel_dup(anti_rel);
			le = anti_le;
		} else {
			assert(0);
		}

		if (isRangePartitionTable(t)) {
			sql_exp *range = NULL, *full_range = NULL;
			int tpe = pt->tpe.type->localtype;
			int (*atomcmp)(const void *, const void *) = ATOMcompare(tpe);
			const void *nil = ATOMnilptr(tpe);

			if (atomcmp(pt->part.range.minvalue, nil) != 0 || atomcmp(pt->part.range.maxvalue, nil) != 0) {
				sql_exp *e1, *e2;
				e1 = create_table_part_atom_exp(sql, pt->tpe, pt->part.range.minvalue);
				e2 = create_table_part_atom_exp(sql, pt->tpe, pt->part.range.maxvalue);
				range = exp_compare2(sql->sa, le, e1, e2, cmp_gte|CMP_BETWEEN);
				full_range = range;
			} else {
				found_all_range_values |= (pt->with_nills != 1);
				found_nils |= is_bit_nil(pt->with_nills);
				if (pt->with_nills == false) { /* full range without nils */
					sql_exp *nils = rel_unop_(sql, dup, le, NULL, "isnull", card_value);
					nils = exp_compare(sql->sa, nils, exp_atom_bool(sql->sa, 1), cmp_notequal);
					full_range = range = nils; /* ugh */
				}
			}
			if (pt->with_nills == true) { /* handle the nulls case */
				sql_exp *nils = rel_unop_(sql, dup, le, NULL, "isnull", card_value);

				set_has_no_nil(nils);
				nils = exp_compare(sql->sa, nils, exp_atom_bool(sql->sa, 1), cmp_equal);
				if (full_range) {
					full_range = exp_or(sql->sa, list_append(new_exp_list(sql->sa), full_range),
										list_append(new_exp_list(sql->sa), nils), 0);
				} else {
					full_range = nils;
				}
				found_nils = 1;
			}
			if (accum && range) {
				accum = exp_or(sql->sa, list_append(new_exp_list(sql->sa), accum),
							   list_append(new_exp_list(sql->sa), exp_copy(sql, range)), 0);
			} else if (range) {
				accum = exp_copy(sql, range);
			}
			if (full_range)
				dup = rel_select(sql->sa, dup, full_range);
		} else if (isListPartitionTable(t)) {
			sql_exp *ein = NULL;

			if (list_length(pt->part.values)) { /* if the partition holds non-null values */
				list *exps = new_exp_list(sql->sa);
				for (node *nn = pt->part.values->h ; nn ; nn = nn->next) {
					sql_part_value *next = (sql_part_value*) nn->data;
					sql_exp *e1 = create_table_part_atom_exp(sql, pt->tpe, next->value);
					list_append(exps, e1);
					list_append(anti_exps, exp_copy(sql, e1));
				}
				ein = exp_in(sql->sa, le, exps, cmp_in);
			} else {
				assert(pt->with_nills);
			}
			if (pt->with_nills) { /* handle the nulls case */
				sql_exp *nils = rel_unop_(sql, dup, le, NULL, "isnull", card_value);

				set_has_no_nil(nils);
				nils = exp_compare(sql->sa, nils, exp_atom_bool(sql->sa, 1), cmp_equal);
				if (ein) {
					ein = exp_or(sql->sa, list_append(new_exp_list(sql->sa), ein),
								 list_append(new_exp_list(sql->sa), nils), 0);
				} else {
					ein = nils;
				}
				found_nils = 1;
			}
			dup = rel_select(sql->sa, dup, ein);
		} else {
			assert(0);
		}

		new_table = rel_basetable(sql, sub, sub->base.name);
		new_table->p = prop_create(sql->sa, PROP_USED, new_table->p); //don't create infinite loops in the optimizer

		if (isPartitionedByExpressionTable(t)) {
			sql_exp *del;
			dup = rel_project(sql->sa, dup, rel_projections(sql, dup, NULL, 1, 1));
			del = list_fetch(dup->exps, list_length(dup->exps) - 1);
			list_remove_data(dup->exps, del);
		}

		s1 = rel_insert(query->sql, new_table, dup);
		if (just_one == 0) {
			sel = rel_list(sql->sa, sel, s1);
		} else {
			sel = s1;
			just_one = 0;
		}
		(*changes)++;
	}

	if (!found_all_range_values || !found_nils) {
		//generate the exception
		if (isRangePartitionTable(t)) {
			if (accum) {
				set_anti(accum);
				anti_exp = accum;
			}
		} else if (isListPartitionTable(t)) {
			if (list_length(anti_exps))
				anti_exp = exp_in(sql->sa, anti_le, anti_exps, cmp_notin);
		} else {
			assert(0);
		}
		if (!found_nils) {
			assert(anti_exp);
			anti_nils = rel_unop_(sql, NULL, anti_le, NULL, "isnull", card_value);
			set_has_no_nil(anti_nils);
			anti_nils = exp_compare(sql->sa, anti_nils, exp_atom_bool(sql->sa, 1), cmp_equal);
			anti_exp = exp_or(sql->sa, list_append(new_exp_list(sql->sa), anti_exp),
							list_append(new_exp_list(sql->sa), anti_nils), 0);
		} else if (!anti_exp) {
			anti_nils = rel_unop_(sql, NULL, exp_copy(sql, anti_le), NULL, "isnull", card_value);
			set_has_no_nil(anti_nils);
			anti_exp = exp_compare(sql->sa, anti_nils, exp_atom_bool(sql->sa, 1), cmp_notequal);
		}
		//generate a count aggregation for the values not present in any of the partitions
		anti_rel = rel_select(sql->sa, anti_rel, anti_exp);
		anti_rel = rel_groupby(sql, anti_rel, NULL);
		aggr = exp_aggr(sql->sa, NULL, cf, 0, 0, anti_rel->card, 0);
		(void) rel_groupby_add_aggr(sql, anti_rel, aggr);
		exp_label(sql->sa, aggr, ++sql->label);

		aggr = exp_ref(sql->sa, aggr);
		snprintf(buf, BUFSIZ, "%s: the %s violates the partition %s of values", operation, desc,
				isRangePartitionTable(t) ? "range (NB higher limit exclusive)" : "list");

		sql_exp *exception = exp_exception(sql->sa, aggr, buf);
		sel = rel_exception(query->sql->sa, sel, anti_rel, list_append(new_exp_list(query->sql->sa), exception));
	}
	sel->p = prop_create(query->sql->sa, PROP_DISTRIBUTE, sel->p);
	return sel;
}

static sql_rel*
rel_propagate_insert(sql_query *query, sql_rel *rel, sql_table *t, int *changes)
{
	return rel_generate_subinserts(query, rel, t, changes, "INSERT", "insert");
}

static sql_rel*
rel_propagate_delete(mvc *sql, sql_rel *rel, sql_table *t, int *changes)
{
	rel = rel_generate_subdeletes(sql, rel, t, changes);
	if (rel) {
		rel = rel_exception(sql->sa, rel, NULL, NULL);
		rel->p = prop_create(sql->sa, PROP_DISTRIBUTE, rel->p);
	}
	return rel;
}

static bool
update_move_across_partitions(sql_rel *rel, sql_table *t)
{
	for (node *n = ((sql_rel*)rel->r)->exps->h; n; n = n->next) {
		sql_exp* exp = (sql_exp*) n->data;
		if (exp->type == e_column && exp->l && exp->r && !strcmp((char*)exp->l, t->base.name)) {
			char* colname = (char*)exp->r;

			if (isPartitionedByColumnTable(t)) {
				if (!strcmp(colname, t->part.pcol->base.name))
					return true;
			} else if (isPartitionedByExpressionTable(t)) {
				for (node *nn = t->part.pexp->cols->h; nn; nn = nn->next) {
					int next = *(int*) nn->data;
					sql_column *col = find_sql_column(t, colname);
					if (col && next == col->colnr)
						return true;
				}
			} else {
				assert(0);
			}
		}
	}
	return false;
}

static sql_rel*
rel_propagate_update(mvc *sql, sql_rel *rel, sql_table *t, int *changes)
{
	bool found_partition_col = update_move_across_partitions(rel, t);
	sql_rel *sel = NULL;

	if (!found_partition_col) { //easy scenario where the partitioned column is not being updated, just propagate
		sel = rel_generate_subupdates(sql, rel, t, changes);
		if (sel) {
			sel = rel_exception(sql->sa, sel, NULL, NULL);
			sel->p = prop_create(sql->sa, PROP_DISTRIBUTE, sel->p);
		}
	} else { //harder scenario, has to insert and delete across partitions.
		/*sql_exp *exception = NULL;
		sql_rel *inserts = NULL, *deletes = NULL, *anti_rel = NULL;

		deletes = rel_generate_subdeletes(sql, rel, t, changes);
		deletes = rel_exception(sql->sa, deletes, NULL, NULL);
		inserts = rel_generate_subinserts(query, rel, &anti_rel, &exception, t, changes, "UPDATE", "update");
		inserts = rel_exception(sql->sa, inserts, anti_rel, list_append(new_exp_list(sql->sa), exception));
		return rel_list(sql->sa, deletes, inserts);*/
		assert(0);
	}
	return sel;
}

static sql_rel*
rel_subtable_insert(sql_query *query, sql_rel *rel, sql_table *t, int *changes)
{
	mvc *sql = query->sql;
	sql_table *upper = t->p; //is part of a partition table and not been used yet
	sql_part *pt = find_sql_part(upper, t->base.name);
	sql_rel *anti_dup = rel_create_common_relation(sql, rel, upper), *left = rel->l;
	sql_exp *anti_exp = NULL, *anti_le = rel_generate_anti_insert_expression(sql, &anti_dup, upper), *aggr = NULL,
			*exception = NULL, *anti_nils = NULL;
	list *anti_exps = new_exp_list(sql->sa);
	sql_subfunc *cf = sql_bind_func(sql->sa, sql->session->schema, "count", sql_bind_localtype("void"), NULL, F_AGGR);
	char buf[BUFSIZ];
	bool found_nils = false, found_all_range_values = false;

	if (isRangePartitionTable(upper)) {
		int tpe = pt->tpe.type->localtype;
		int (*atomcmp)(const void *, const void *) = ATOMcompare(tpe);
		const void *nil = ATOMnilptr(tpe);

		if (pt->with_nills == true || is_bit_nil(pt->with_nills))
			found_nils = true;

		if (atomcmp(pt->part.range.minvalue, nil) == 0) {
			if (atomcmp(pt->part.range.maxvalue, nil) == 0) {
				found_all_range_values = pt->with_nills != 1;
				if (pt->with_nills == true) {
					anti_nils = rel_unop_(sql, anti_dup, exp_copy(sql, anti_le), NULL, "isnull", card_value);
					anti_exp = exp_compare(sql->sa, anti_nils, exp_atom_bool(sql->sa, 1), cmp_notequal);
				}
			} else {
				sql_exp *e2 = create_table_part_atom_exp(sql, pt->tpe, pt->part.range.maxvalue);
				anti_exp = exp_compare(sql->sa, exp_copy(sql, anti_le), e2, cmp_gte);
			}
		} else {
			if (atomcmp(pt->part.range.maxvalue, nil) == 0) {
				sql_exp *e1 = create_table_part_atom_exp(sql, pt->tpe, pt->part.range.minvalue);
				anti_exp = exp_compare(sql->sa, exp_copy(sql, anti_le), e1, cmp_lt);
			} else {
				sql_exp *e1 = create_table_part_atom_exp(sql, pt->tpe, pt->part.range.minvalue),
					*e2 = create_table_part_atom_exp(sql, pt->tpe, pt->part.range.maxvalue),
					*range1 = exp_compare(sql->sa, exp_copy(sql, anti_le), e1, cmp_lt),
					*range2 = exp_compare(sql->sa, exp_copy(sql, anti_le), e2, cmp_gte);
				anti_exp = exp_or(sql->sa, list_append(new_exp_list(sql->sa), range1),
						  list_append(new_exp_list(sql->sa), range2), 0);
			}
		}
		if (!pt->with_nills) { /* handle the nulls case */
			anti_nils = rel_unop_(sql, anti_dup, exp_copy(sql, anti_le), NULL, "isnull", card_value);
			anti_nils = exp_compare(sql->sa, anti_nils, exp_atom_bool(sql->sa, 1), cmp_equal);
			if (anti_exp)
				anti_exp = exp_or(sql->sa, list_append(new_exp_list(sql->sa), anti_exp),
					 	 list_append(new_exp_list(sql->sa), anti_nils), 0);
			else
				anti_exp = anti_nils;
		}
	} else if (isListPartitionTable(upper)) {
		if (list_length(pt->part.values)) { /* if the partition holds non-null values */
			for (node *n = pt->part.values->h ; n ; n = n->next) {
				sql_part_value *next = (sql_part_value*) n->data;
				sql_exp *e1 = create_table_part_atom_exp(sql, pt->tpe, next->value);
				list_append(anti_exps, exp_copy(sql, e1));
			}
			anti_exp = exp_in(sql->sa, exp_copy(sql, anti_le), anti_exps, cmp_notin);

			if (!pt->with_nills) { /* handle the nulls case */
				anti_nils = rel_unop_(sql, anti_dup, exp_copy(sql, anti_le), NULL, "isnull", card_value);
				anti_nils = exp_compare(sql->sa, anti_nils, exp_atom_bool(sql->sa, 1), cmp_equal);
				anti_exp = exp_or(sql->sa, list_append(new_exp_list(sql->sa), anti_exp),
								  list_append(new_exp_list(sql->sa), anti_nils), 0);
			}
		} else {
			assert(pt->with_nills);
			anti_nils = rel_unop_(sql, anti_dup, exp_copy(sql, anti_le), NULL, "isnull", card_value);
			anti_exp = exp_compare(sql->sa, anti_nils, exp_atom_bool(sql->sa, 1), cmp_notequal);
		}
	} else {
		assert(0);
	}

	if (!found_all_range_values || !found_nils) {
		//generate a count aggregation for the values not present in any of the partitions
		anti_dup = rel_select(sql->sa, anti_dup, anti_exp);
		anti_dup = rel_groupby(sql, anti_dup, NULL);
		aggr = exp_aggr(sql->sa, NULL, cf, 0, 0, anti_dup->card, 0);
		(void) rel_groupby_add_aggr(sql, anti_dup, aggr);
		exp_label(sql->sa, aggr, ++sql->label);

		//generate the exception
		aggr = exp_ref(sql->sa, aggr);
		snprintf(buf, BUFSIZ, "INSERT: table %s.%s is part of merge table %s.%s and the insert violates the "
				"partition %s of values", t->s->base.name, t->base.name, upper->s->base.name,
				upper->base.name, isRangePartitionTable(upper) ? "range" : "list");
		exception = exp_exception(sql->sa, aggr, buf);

		left->p = prop_create(sql->sa, PROP_USED, left->p);
		(*changes)++;

		rel = rel_exception(sql->sa, rel, anti_dup, list_append(new_exp_list(sql->sa), exception));
	}
	rel->p = prop_create(sql->sa, PROP_DISTRIBUTE, rel->p);
	return rel;
}

sql_rel *
rel_propagate(sql_query *query, sql_rel *rel, int *changes)
{
	mvc *sql = query->sql;
	bool isSubtable = false;
	sql_rel *l = rel->l, *propagate = rel;

	if (l->op == op_basetable) {
		sql_table *t = l->l;

		if (t->p && (isRangePartitionTable(t->p) || isListPartitionTable(t->p)) && !find_prop(l->p, PROP_USED)) {
			isSubtable = true;
			if (is_insert(rel->op)) { //insertion directly to sub-table (must do validation)
				sql->caching = 0;
				rel = rel_subtable_insert(query, rel, t, changes);
				propagate = rel->l;
			}
		}
		if (isMergeTable(t)) {
			assert(list_length(t->members.set) > 0);
			if (is_delete(propagate->op) || is_truncate(propagate->op)) { //propagate deletions to the partitions
				sql->caching = 0;
				rel = rel_propagate_delete(sql, rel, t, changes);
			} else if (isRangePartitionTable(t) || isListPartitionTable(t)) {
				if (is_insert(propagate->op)) { //on inserts create a selection for each partition
					sql->caching = 0;
					if (isSubtable) {
						rel->l = rel_propagate_insert(query, propagate, t, changes);
					} else {
						rel = rel_propagate_insert(query, rel, t, changes);
					}
				} else if (is_update(propagate->op)) { //for updates propagate like in deletions
					sql->caching = 0;
					rel = rel_propagate_update(sql, rel, t, changes);
				} else {
					assert(0);
				}
			} else {
				assert(0);
			}
		}
	}
	return rel;
}
