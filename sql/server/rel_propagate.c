/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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

	if(isPartitionedByColumnTable(mt)) {
		int colr = mt->part.pcol->colnr;
		res = list_fetch((*anti_rel)->exps, colr);
		res = exp_column(sql->sa, exp_relname(res), exp_name(res), exp_subtype(res), res->card, has_nil(res),
						 is_intern(res));
	} else if(isPartitionedByExpressionTable(mt)) {
		*anti_rel = rel_project(sql->sa, *anti_rel, NULL);
		if(!(res = rel_parse_val(sql, sa_message(sql->sa, "select %s;", mt->part.pexp->exp), sql->emode, (*anti_rel)->l)))
			return NULL;
		exp_label(sql->sa, res, ++sql->label);
	} else {
		assert(0);
	}
	(*anti_rel)->exps = new_exp_list(sql->sa);
	append((*anti_rel)->exps, res);
	res = exp_column(sql->sa, exp_relname(res), exp_name(res), exp_subtype(res), res->card, has_nil(res),
						 is_intern(res));
	return res;
}

static sql_rel*
rel_create_common_relation(mvc *sql, sql_rel *rel, sql_table *t)
{
	if(isPartitionedByColumnTable(t)) {
		return rel_dup(rel->r);
	} else if(isPartitionedByExpressionTable(t)) {
		sql_rel *inserts;
		list *l = new_exp_list(sql->sa);

		rel->r = rel_project(sql->sa, rel->r, l);
		set_processed((sql_rel*)rel->r);
		inserts = ((sql_rel*)(rel->r))->l;
		for (node *n = t->columns.set->h, *m = inserts->exps->h; n && m; n = n->next, m = m->next) {
			sql_column *col = n->data;
			sql_exp *before = m->data;
			sql_exp *help = exp_column(sql->sa, t->base.name, col->base.name, exp_subtype(before), before->card,
									   has_nil(before), is_intern(before));
			help->l = sa_strdup(sql->sa, exp_relname(before));
			help->r = sa_strdup(sql->sa, exp_name(before));
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

	if(isPartitionedByColumnTable(t)) {
		int colr = t->part.pcol->colnr;
		res = list_fetch((*anti_rel)->exps, colr);
	} else if(isPartitionedByExpressionTable(t)) {
		*anti_rel = rel_project(sql->sa, *anti_rel, rel_projections(sql, *anti_rel, NULL, 1, 1));
		if(!(res = rel_parse_val(sql, sa_message(sql->sa, "select %s;", t->part.pexp->exp), sql->emode, (*anti_rel)->l)))
			return NULL;
		exp_label(sql->sa, res, ++sql->label);
		append((*anti_rel)->exps, res);
	} else {
		assert(0);
	}
	res = exp_column(sql->sa, exp_relname(res), exp_name(res), exp_subtype(res), res->card, has_nil(res),
					 is_intern(res));
	return res;
}

static void
generate_alter_table_error_message(char* buf, sql_table *mt)
{
	char *s1 = isRangePartitionTable(mt) ? "range":"list of values";
	if(isPartitionedByColumnTable(mt)) {
		sql_column* col = mt->part.pcol;
		snprintf(buf, BUFSIZ, "ALTER TABLE: there are values in the column %s outside the partition %s", col->base.name, s1);
	} else if(isPartitionedByExpressionTable(mt)) {
		snprintf(buf, BUFSIZ, "ALTER TABLE: there are values in the expression outside the partition %s", s1);
	} else {
		assert(0);
	}
}

static sql_exp *
generate_partition_limits(mvc *sql, sql_rel **r, symbol *s, sql_subtype tpe)
{
	if(!s) {
		return NULL;
	} else if (s->token == SQL_NULL) {
		return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: range bound cannot be null");
	} else if (s->token == SQL_MINVALUE) {
		atom *amin = atom_absolute_min(sql->sa, &tpe);
		if(!amin) {
			char *err = sql_subtype_string(&tpe);
			if(!err)
				return sql_error(sql, 02, SQLSTATE(HY001) MAL_MALLOC_FAIL);
			sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: absolute minimum value not available for %s type", err);
			GDKfree(err);
			return NULL;
		}
		return exp_atom(sql->sa, amin);
	} else if (s->token == SQL_MAXVALUE) {
		atom *amax = atom_absolute_max(sql->sa, &tpe);
		if(!amax) {
			char *err = sql_subtype_string(&tpe);
			if(!err)
				return sql_error(sql, 02, SQLSTATE(HY001) MAL_MALLOC_FAIL);
			sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: absolute maximum value not available for %s type", err);
			GDKfree(err);
			return NULL;
		}
		return exp_atom(sql->sa, amax);
	} else {
		int is_last = 0;
		exp_kind ek = {type_value, card_value, FALSE};
		sql_exp *e = rel_value_exp2(sql, r, s, sql_sel, ek, &is_last);

		if (!e) {
			return NULL;
		}
		return rel_check_type(sql, &tpe, e, type_equal);
	}
}

sql_rel *
rel_alter_table_add_partition_range(mvc* sql, sql_table *mt, sql_table *pt, char *sname, char *tname, char *sname2,
									char *tname2, symbol* min, symbol* max, int with_nills, int update)
{
	sql_rel *rel_psm = rel_create(sql->sa), *anti_rel;
	list *exps = new_exp_list(sql->sa);
	sql_exp *exception, *aggr, *anti_exp, *anti_le, *e1, *e2, *anti_nils, *pmin, *pmax;
	sql_subaggr *cf = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
	char buf[BUFSIZ];
	sql_subtype tpe;

	if(!rel_psm || !exps)
		return NULL;

	find_partition_type(&tpe, mt);

	anti_le = rel_generate_anti_expression(sql, &anti_rel, mt, pt);
	anti_nils = rel_unop_(sql, anti_le, NULL, "isnull", card_value);

	assert((!min && !max && with_nills) || (min && max));
	if(min && max) {
		pmin = generate_partition_limits(sql, &rel_psm, min, tpe);
		pmax = generate_partition_limits(sql, &rel_psm, max, tpe);

		if(!pmin || !pmax)
			return NULL;

		e1 = exp_copy(sql->sa, pmin);
		if (subtype_cmp(exp_subtype(e1), &tpe) != 0)
			e1 = exp_convert(sql->sa, e1, &e1->tpe, &tpe);

		e2 = exp_copy(sql->sa, pmax);
		if (subtype_cmp(exp_subtype(e2), &tpe) != 0)
			e2 = exp_convert(sql->sa, e2, &e2->tpe, &tpe);

		anti_exp = exp_compare2(sql->sa, anti_le, e1, e2, 3);
		set_anti(anti_exp);
		if(!with_nills) {
			anti_nils = exp_compare(sql->sa, anti_nils, exp_atom_bool(sql->sa, 1), cmp_equal);
			anti_exp = exp_or(sql->sa, list_append(new_exp_list(sql->sa), anti_exp),
							  list_append(new_exp_list(sql->sa), anti_nils), 0);
		}
	} else {
		pmin = exp_atom(sql->sa, atom_general(sql->sa, &tpe, NULL));
		pmax = exp_atom(sql->sa, atom_general(sql->sa, &tpe, NULL));
		anti_exp = exp_compare(sql->sa, anti_nils, exp_atom_bool(sql->sa, 1), cmp_notequal);
	}

	anti_rel = rel_select(sql->sa, anti_rel, anti_exp);
	anti_rel = rel_groupby(sql, anti_rel, NULL);
	aggr = exp_aggr(sql->sa, NULL, cf, 0, 0, anti_rel->card, 0);
	(void) rel_groupby_add_aggr(sql, anti_rel, aggr);
	exp_label(sql->sa, aggr, ++sql->label);

	//generate the exception
	aggr = exp_column(sql->sa, exp_relname(aggr), exp_name(aggr), exp_subtype(aggr), aggr->card, has_nil(aggr),
					  is_intern(aggr));
	generate_alter_table_error_message(buf, mt);
	exception = exp_exception(sql->sa, aggr, buf);

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
	append(exps, exp_atom_int(sql->sa, with_nills));
	append(exps, exp_atom_int(sql->sa, update));
	rel_psm->l = NULL;
	rel_psm->r = NULL;
	rel_psm->op = op_ddl;
	rel_psm->flag = DDL_ALTER_TABLE_ADD_RANGE_PARTITION;
	rel_psm->exps = exps;
	rel_psm->card = CARD_MULTI;
	rel_psm->nrcols = 0;

	return rel_exception(sql->sa, rel_psm, anti_rel, list_append(new_exp_list(sql->sa), exception));
}

sql_rel *
rel_alter_table_add_partition_list(mvc *sql, sql_table *mt, sql_table *pt, char *sname, char *tname, char *sname2,
								   char *tname2, dlist* values, int with_nills, int update)
{
	sql_rel *rel_psm = rel_create(sql->sa), *anti_rel;
	list *exps = new_exp_list(sql->sa), *anti_exps = new_exp_list(sql->sa), *lvals = new_exp_list(sql->sa);
	sql_exp *exception, *aggr, *anti_exp, *anti_le, *anti_nils;
	sql_subaggr *cf = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
	char buf[BUFSIZ];
	sql_subtype tpe;

	if(!rel_psm || !exps)
		return NULL;

	find_partition_type(&tpe, mt);

	anti_le = rel_generate_anti_expression(sql, &anti_rel, mt, pt);
	anti_nils = rel_unop_(sql, anti_le, NULL, "isnull", card_value);

	if(values) {
		for (dnode *dn = values->h; dn ; dn = dn->next) { /* parse the atoms and generate the expressions */
			symbol* next = dn->data.sym;
			sql_exp *pnext = generate_partition_limits(sql, &rel_psm, next, tpe);
			if (subtype_cmp(exp_subtype(pnext), &tpe) != 0)
				pnext = exp_convert(sql->sa, pnext, exp_subtype(pnext), &tpe);

			if(next->token == SQL_NULL)
				return sql_error(sql, 02, SQLSTATE(42000) "ALTER TABLE: a list value cannot be null");
			append(lvals, pnext);
			append(anti_exps, exp_copy(sql->sa, pnext));
		}
	}

	if(list_length(anti_exps) > 0) {
		anti_exp = exp_in(sql->sa, anti_le, anti_exps, cmp_notin);
		if(!with_nills) {
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
	aggr = exp_column(sql->sa, exp_relname(aggr), exp_name(aggr), exp_subtype(aggr), aggr->card, has_nil(aggr),
					  is_intern(aggr));
	generate_alter_table_error_message(buf, mt);
	exception = exp_exception(sql->sa, aggr, buf);

	//generate the psm statement
	append(exps, exp_atom_clob(sql->sa, sname));
	append(exps, exp_atom_clob(sql->sa, tname));
	assert((sname2 && tname2) || (!sname2 && !tname2));
	if (sname2) {
		append(exps, exp_atom_clob(sql->sa, sname2));
		append(exps, exp_atom_clob(sql->sa, tname2));
	}
	append(exps, exp_atom_int(sql->sa, with_nills));
	append(exps, exp_atom_int(sql->sa, update));
	rel_psm->l = NULL;
	rel_psm->r = NULL;
	rel_psm->op = op_ddl;
	rel_psm->flag = DDL_ALTER_TABLE_ADD_LIST_PARTITION;
	rel_psm->exps = list_merge(exps, lvals, (fdup)NULL);
	rel_psm->card = CARD_MULTI;
	rel_psm->nrcols = 0;

	return rel_exception(sql->sa, rel_psm, anti_rel, list_append(new_exp_list(sql->sa), exception));
}

static sql_rel* rel_change_base_table(mvc* sql, sql_rel* rel, sql_table* oldt, sql_table* newt);

static sql_exp*
exp_change_column_table(mvc *sql, sql_exp *e, sql_table* oldt, sql_table* newt)
{
	if (!e)
		return NULL;
	switch(e->type) {
		case e_psm: {
			if (e->flag & PSM_RETURN) {
				for(node *n = ((list*)e->r)->h ; n ; n = n->next)
					n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);
			} else if (e->flag & PSM_WHILE) {
				e->l = exp_change_column_table(sql, e->l, oldt, newt);
				for(node *n = ((list*)e->r)->h ; n ; n = n->next)
					n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);
			} else if (e->flag & PSM_IF) {
				e->l = exp_change_column_table(sql, e->l, oldt, newt);
				for(node *n = ((list*)e->r)->h ; n ; n = n->next)
					n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);
				if (e->f)
					for(node *n = ((list*)e->f)->h ; n ; n = n->next)
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
			for(node *n = ((list*)e->l)->h ; n ; n = n->next)
				n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);
			if (e->r)
				for(node *n = ((list*)e->r)->h ; n ; n = n->next)
					n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);
		} 	break;
		case e_aggr: {
			if (e->l)
				for(node *n = ((list*)e->l)->h ; n ; n = n->next)
					n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);
		} 	break;
		case e_column: {
			if(!strcmp(e->l, oldt->base.name))
				e->l = sa_strdup(sql->sa, newt->base.name);
		} break;
		case e_cmp: {
			if (e->flag == cmp_in || e->flag == cmp_notin) {
				e->l = exp_change_column_table(sql, e->l, oldt, newt);
				for(node *n = ((list*)e->r)->h ; n ; n = n->next)
					n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);
			} else if (get_cmp(e) == cmp_or || get_cmp(e) == cmp_filter) {
				for(node *n = ((list*)e->l)->h ; n ; n = n->next)
					n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);
				for(node *n = ((list*)e->r)->h ; n ; n = n->next)
					n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);
			} else {
				if(e->l)
					e->l = exp_change_column_table(sql, e->l, oldt, newt);
				if(e->r)
					e->r = exp_change_column_table(sql, e->r, oldt, newt);
				if(e->f)
					e->f = exp_change_column_table(sql, e->f, oldt, newt);
			}
		} break;
	}
	if(e->rname && !strcmp(e->rname, oldt->base.name))
		e->rname = sa_strdup(sql->sa, newt->base.name);
	return e;
}

static sql_rel*
rel_change_base_table(mvc* sql, sql_rel* rel, sql_table* oldt, sql_table* newt)
{
	if(!rel)
		return NULL;

	if(rel->exps)
		for(node *n = rel->exps->h ; n ; n = n->next)
			n->data = exp_change_column_table(sql, (sql_exp*) n->data, oldt, newt);

	switch(rel->op) {
		case op_basetable:
			if(rel->l == oldt)
				rel->l = newt;
			if(rel->r)
				rel->r = rel_change_base_table(sql, rel->r, oldt, newt);
			break;
		case op_table:
		case op_topn:
		case op_sample:
		case op_project:
		case op_groupby:
		case op_select:
		case op_insert:
		case op_ddl:
		case op_update:
		case op_delete:
		case op_truncate:
		case op_union:
		case op_inter:
		case op_except:
		case op_join:
		case op_left:
		case op_right:
		case op_full:
		case op_semi:
		case op_anti:
		case op_apply:
			if(rel->l)
				rel->l = rel_change_base_table(sql, rel->l, oldt, newt);
			if(rel->r)
				rel->r = rel_change_base_table(sql, rel->r, oldt, newt);
	}
	return rel;
}

static sql_rel *
rel_truncate_duplicate(sql_allocator *sa, sql_rel *table, sql_rel *ori)
{
	sql_rel *r = rel_create(sa);

	r->exps = exps_copy(sa, ori->exps);
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

		if(rel->r) {
			dup = rel_copy(sql->sa, rel->r, 1);
			dup = rel_change_base_table(sql, dup, t, sub);
		}
		if(is_delete(rel->op))
			s1 = rel_delete(sql->sa, rel_basetable(sql, sub, sub->base.name), dup);
		else
			s1 = rel_truncate_duplicate(sql->sa, rel_basetable(sql, sub, sub->base.name), rel);
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
		list *uexps = exps_copy(sql->sa, rel->exps);

		if(rel->r) {
			dup = rel_copy(sql->sa, rel->r, 1);
			dup = rel_change_base_table(sql, dup, t, sub);
		}

		for(node *ne = uexps->h ; ne ; ne = ne->next)
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
rel_generate_subinserts(mvc *sql, sql_rel *rel, sql_rel **anti_rel, sql_exp **exception, sql_table *t, int *changes,
						const char *operation, const char *desc)
{
	int just_one = 1, found_nils = 0;
	sql_rel *new_table = NULL, *sel = NULL;
	sql_exp *anti_exp = NULL, *anti_le = NULL, *accum = NULL, *aggr = NULL;
	list *anti_exps = new_exp_list(sql->sa);
	sql_subaggr *cf = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
	char buf[BUFSIZ];

	if(isPartitionedByColumnTable(t)) {
		*anti_rel = rel_dup(rel->r);
	} else if(isPartitionedByExpressionTable(t)) {
		*anti_rel = rel_create_common_relation(sql, rel, t);
	} else {
		assert(0);
	}
	anti_le = rel_generate_anti_insert_expression(sql, anti_rel, t);

	for (node *n = t->members.set->h; n; n = n->next) {
		sql_part *pt = (sql_part *) n->data;
		sql_table *sub = find_sql_table(t->s, pt->base.name);
		sql_rel *s1 = NULL, *dup = NULL;
		sql_exp *le = NULL;

		if(isPartitionedByColumnTable(t)) {
			dup = rel_dup(rel->r);
			le = rel_generate_anti_insert_expression(sql, &dup, t);
		} else if(isPartitionedByExpressionTable(t)) {
			dup = rel_dup(*anti_rel);
			le = anti_le;
		} else {
			assert(0);
		}

		if(isRangePartitionTable(t)) {
			sql_exp *e1, *e2, *range;
			e1 = create_table_part_atom_exp(sql, pt->tpe, pt->part.range.minvalue);
			e2 = create_table_part_atom_exp(sql, pt->tpe, pt->part.range.maxvalue);
			range = exp_compare2(sql->sa, le, e1, e2, 3);

			if(accum) {
				accum = exp_or(sql->sa, list_append(new_exp_list(sql->sa), exp_copy(sql->sa, range)),
							   list_append(new_exp_list(sql->sa), accum), 1);
			} else {
				accum = exp_copy(sql->sa, range);
			}

			if(pt->with_nills) { /* handle the nulls case */
				sql_exp *nils = rel_unop_(sql, le, NULL, "isnull", card_value);
				nils = exp_compare(sql->sa, nils, exp_atom_bool(sql->sa, 1), cmp_equal);
				range = exp_or(sql->sa, list_append(new_exp_list(sql->sa), range),
							   list_append(new_exp_list(sql->sa), nils), 0);
				found_nils = 1;
			}
			dup = rel_select(sql->sa, dup, range);
		} else if(isListPartitionTable(t)) {
			sql_exp *ein;
			list *exps = new_exp_list(sql->sa);

			for(node *n = pt->part.values->h ; n ; n = n->next) {
				sql_part_value *next = (sql_part_value*) n->data;
				sql_exp *e1 = create_table_part_atom_exp(sql, next->tpe, next->value);
				list_append(exps, e1);
				list_append(anti_exps, exp_copy(sql->sa, e1));
			}

			ein = exp_in(sql->sa, le, exps, cmp_in);
			if(pt->with_nills) { /* handle the nulls case */
				sql_exp *nils = rel_unop_(sql, le, NULL, "isnull", card_value);
				nils = exp_compare(sql->sa, nils, exp_atom_bool(sql->sa, 1), cmp_equal);
				ein = exp_or(sql->sa, list_append(new_exp_list(sql->sa), ein),
							 list_append(new_exp_list(sql->sa), nils), 0);
				found_nils = 1;
			}
			dup = rel_select(sql->sa, dup, ein);
		} else {
			assert(0);
		}

		new_table = rel_basetable(sql, sub, sub->base.name);
		if(list_length(sub->members.set) == 0) //if this table has no partitions, set it as used
			new_table->p = prop_create(sql->sa, PROP_USED, new_table->p);

		if(isPartitionedByExpressionTable(t)) {
			sql_exp *del;
			dup = rel_project(sql->sa, dup, rel_projections(sql, dup, NULL, 1, 1));
			del = list_fetch(dup->exps, list_length(dup->exps) - 1);
			list_remove_data(dup->exps, del);
		}

		s1 = rel_insert(sql, new_table, dup);
		if (just_one == 0) {
			sel = rel_list(sql->sa, sel, s1);
		} else {
			sel = s1;
			just_one = 0;
		}
		(*changes)++;
	}

	//generate the exception
	if(isRangePartitionTable(t)) {
		if (list_length(t->members.set) == 1) //when there is just one partition must set the anti_exp
			set_anti(accum);
		anti_exp = accum;
	} else if(isListPartitionTable(t)) {
		anti_exp = exp_in(sql->sa, anti_le, anti_exps, cmp_notin);
	} else {
		assert(0);
	}
	if(!found_nils) {
		sql_exp *anti_nils = rel_unop_(sql, anti_le, NULL, "isnull", card_value);
		anti_nils = exp_compare(sql->sa, anti_nils, exp_atom_bool(sql->sa, 1), cmp_equal);
		anti_exp = exp_or(sql->sa, list_append(new_exp_list(sql->sa), anti_exp),
						  list_append(new_exp_list(sql->sa), anti_nils), 0);
	}
	//generate a count aggregation for the values not present in any of the partitions
	*anti_rel = rel_select(sql->sa, *anti_rel, anti_exp);
	*anti_rel = rel_groupby(sql, *anti_rel, NULL);
	aggr = exp_aggr(sql->sa, NULL, cf, 0, 0, (*anti_rel)->card, 0);
	(void) rel_groupby_add_aggr(sql, *anti_rel, aggr);
	exp_label(sql->sa, aggr, ++sql->label);

	aggr = exp_column(sql->sa, exp_relname(aggr), exp_name(aggr), exp_subtype(aggr), aggr->card,
					  has_nil(aggr), is_intern(aggr));
	snprintf(buf, BUFSIZ, "%s: the %s violates the partition %s of values", operation, desc,
			 isRangePartitionTable(t) ? "range" : "list");
	*exception = exp_exception(sql->sa, aggr, buf);

	return sel;
}

static sql_rel*
rel_propagate_insert(mvc *sql, sql_rel *rel, sql_table *t, int *changes)
{
	sql_exp* exception = NULL;
	sql_rel* anti_rel = NULL;
	sql_rel* res = rel_generate_subinserts(sql, rel, &anti_rel, &exception, t, changes, "INSERT", "insert");

	res = rel_exception(sql->sa, res, anti_rel, list_append(new_exp_list(sql->sa), exception));
	res->p = prop_create(sql->sa, PROP_DISTRIBUTE, res->p);
	return res;
}

static sql_rel*
rel_propagate_delete(mvc *sql, sql_rel *rel, sql_table *t, int *changes)
{
	rel = rel_generate_subdeletes(sql, rel, t, changes);
	rel = rel_exception(sql->sa, rel, NULL, NULL);
	rel->p = prop_create(sql->sa, PROP_DISTRIBUTE, rel->p);
	return rel;
}

static int
update_move_across_partitions(sql_rel *rel, sql_table *t)
{
	int found_partition_col = 0;

	if(isPartitionedByColumnTable(t)) {
		for (node *n = ((sql_rel*)rel->r)->exps->h; n; n = n->next) {
			sql_exp* exp = (sql_exp*) n->data;
			if(exp->type == e_column && exp->l && exp->r && !strcmp((char*)exp->l, t->base.name) &&
			   !strcmp((char*)exp->r, t->part.pcol->base.name)) {
				found_partition_col = 1;
			}
		}
	} else if(isPartitionedByExpressionTable(t)) {
		//TODO
	} else {
		assert(0);
	}

	return found_partition_col;
}

static sql_rel*
rel_propagate_update(mvc *sql, sql_rel *rel, sql_table *t, int *changes)
{
	int found_partition_col = update_move_across_partitions(rel, t);
	sql_rel *sel = NULL;

	if(!found_partition_col) { //easy scenario where the partitioned column is not being updated, just propagate
		sel = rel_generate_subupdates(sql, rel, t, changes);
		sel = rel_exception(sql->sa, sel, NULL, NULL);
		sel->p = prop_create(sql->sa, PROP_DISTRIBUTE, sel->p);
	} else { //harder scenario, has to insert and delete across partitions.
		/*sql_exp *exception = NULL;
		sql_rel *inserts = NULL, *deletes = NULL, *anti_rel = NULL;

		deletes = rel_generate_subdeletes(sql, rel, t, changes);
		deletes = rel_exception(sql->sa, deletes, NULL, NULL);
		inserts = rel_generate_subinserts(sql, rel, &anti_rel, &exception, t, changes, "UPDATE", "update");
		inserts = rel_exception(sql->sa, inserts, anti_rel, list_append(new_exp_list(sql->sa), exception));
		return rel_list(sql->sa, deletes, inserts);*/
		assert(0);
	}
	return sel;
}

static sql_rel*
rel_subtable_insert(mvc *sql, sql_rel *rel, sql_table *t, int *changes)
{
	sql_table *upper = t->p; //is part of a partition table and not been used yet
	sql_part *pt = find_sql_part(upper, t->base.name);
	sql_rel *anti_dup = rel_create_common_relation(sql, rel, upper), *left = rel->l;
	sql_exp *anti_exp = NULL, *anti_le = rel_generate_anti_insert_expression(sql, &anti_dup, upper), *aggr = NULL,
			*exception = NULL;
	list *anti_exps = new_exp_list(sql->sa);
	sql_subaggr *cf = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
	char buf[BUFSIZ];

	if(isRangePartitionTable(upper)) {
		sql_exp *e1 = create_table_part_atom_exp(sql, pt->tpe, pt->part.range.minvalue),
				*e2 = create_table_part_atom_exp(sql, pt->tpe, pt->part.range.maxvalue);
		anti_exp = exp_compare2(sql->sa, exp_copy(sql->sa, anti_le), exp_copy(sql->sa, e1), exp_copy(sql->sa, e2), 3);
		set_anti(anti_exp);
	} else if(isListPartitionTable(upper)) {
		for(node *n = pt->part.values->h ; n ; n = n->next) {
			sql_part_value *next = (sql_part_value*) n->data;
			sql_exp *e1 = create_table_part_atom_exp(sql, next->tpe, next->value);
			list_append(anti_exps, exp_copy(sql->sa, e1));
		}
		anti_exp = exp_in(sql->sa, exp_copy(sql->sa, anti_le), anti_exps, cmp_notin);
	} else {
		assert(0);
	}
	if(!pt->with_nills) { /* handle the nulls case */
		sql_exp *anti_nils = rel_unop_(sql, exp_copy(sql->sa, anti_le), NULL, "isnull", card_value);
		anti_nils = exp_compare(sql->sa, anti_nils, exp_atom_bool(sql->sa, 1), cmp_equal);
		anti_exp = exp_or(sql->sa, list_append(new_exp_list(sql->sa), anti_exp),
						  list_append(new_exp_list(sql->sa), anti_nils), 0);
	}

	//generate a count aggregation for the values not present in any of the partitions
	anti_dup = rel_select(sql->sa, anti_dup, anti_exp);
	anti_dup = rel_groupby(sql, anti_dup, NULL);
	aggr = exp_aggr(sql->sa, NULL, cf, 0, 0, anti_dup->card, 0);
	(void) rel_groupby_add_aggr(sql, anti_dup, aggr);
	exp_label(sql->sa, aggr, ++sql->label);

	//generate the exception
	aggr = exp_column(sql->sa, exp_relname(aggr), exp_name(aggr), exp_subtype(aggr), aggr->card,
					  has_nil(aggr), is_intern(aggr));
	snprintf(buf, BUFSIZ, "INSERT: table %s.%s is part of merge table %s.%s and the insert violates the "
						  "partition %s of values", t->s->base.name,  t->base.name, upper->s->base.name,
			 upper->base.name, isRangePartitionTable(upper) ? "range" : "list");
	exception = exp_exception(sql->sa, aggr, buf);

	left->p = prop_create(sql->sa, PROP_USED, left->p);
	(*changes)++;

	rel = rel_exception(sql->sa, rel, anti_dup, list_append(new_exp_list(sql->sa), exception));
	rel->p = prop_create(sql->sa, PROP_DISTRIBUTE, rel->p);
	return rel;
}

sql_rel *
rel_propagate(mvc *sql, sql_rel *rel, int *changes) //TODO sql->caching = 0;
{
	sql_rel *l = rel->l;

	if(l->op == op_basetable) {
		sql_table *t = l->l;

		if((isRangePartitionTable(t) || isListPartitionTable(t)) && !find_prop(l->p, PROP_USED)) {
			assert(list_length(t->members.set) > 0);
			if(is_delete(rel->op) || is_truncate(rel->op)) { //propagate deletions to the partitions
				return rel_propagate_delete(sql, rel, t, changes);
			} else if(is_insert(rel->op)) { //on inserts create a selection for each partition
				return rel_propagate_insert(sql, rel, t, changes);
			} else if(is_update(rel->op)) { //for updates create both a insertion and deletion for each partition
				return rel_propagate_update(sql, rel, t, changes);
			} else {
				assert(0);
			}
		} else if(t->p) {
			sql_part *pt = find_sql_part(t->p, t->base.name);
			if(!pt) {
				t->p = NULL;
			} else if((isRangePartitionTable(t->p) || isListPartitionTable(t->p)) && !find_prop(l->p, PROP_USED)) {
				if(is_insert(rel->op)) { //insertion directly to sub-table (must do validation)
					return rel_subtable_insert(sql, rel, t, changes);
				}
			}
		}
	}
	return rel;
}
