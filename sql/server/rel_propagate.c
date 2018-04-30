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
#include "rel_schema.h"
#include "sql_mvc.h"
#include "mtime.h"

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

static sql_exp *
create_table_part_atom_exp(mvc *sql, sht tpe, ptr value)
{
	switch (tpe) {
		case TYPE_bit: {
			bit bval = *((bit*) value);
			return exp_atom_bool(sql->sa, bval ? 1 : 0);
		}
		case TYPE_bte: {
			bte bbval = *((bte *) value);
			return exp_atom_bte(sql->sa, bbval);
		}
		case TYPE_sht: {
			sht sval = *((sht*) value);
			return exp_atom_sht(sql->sa, sval);
		}
		case TYPE_int: {
			int ival = *((int*) value);
			return exp_atom_int(sql->sa, ival);
		}
		case TYPE_lng: {
			lng lval = *((lng*) value);
			return exp_atom_lng(sql->sa, lval);
		}
		case TYPE_flt: {
			flt fval = *((flt*) value);
			return exp_atom_flt(sql->sa, fval);
		}
		case TYPE_dbl: {
			dbl dval = *((dbl*) value);
			return exp_atom_dbl(sql->sa, dval);
		}
		case TYPE_str:
			return exp_atom_clob(sql->sa, sa_strdup(sql->sa, value));
#ifdef HAVE_HGE
		case TYPE_hge: {
			hge hval = *((hge*) value);
			return exp_atom_hge(sql->sa, hval);
		}
#endif
		default:
			assert(0);
	}
}

static sql_rel *
rel_ddl_distribute(sql_allocator *sa, sql_rel *l, sql_rel *r, list *exps)
{
	sql_rel *rel = rel_create(sa);
	if(!rel)
		return NULL;
	rel->l = l;
	rel->r = r;
	rel->exps = exps;
	rel->op = op_ddl;
	rel->flag = DDL_DISTRIBUTE;
	return rel;
}

static sql_rel*
rel_propagate_delete(mvc *sql, sql_rel *rel, sql_table *t, int *changes)
{
	int just_one = 1;
	sql_rel *sel = NULL;

	for (node *n = t->members.set->h; n; n = n->next) {
		sql_part *pt = (sql_part *) n->data;
		sql_table *sub = find_sql_table(t->s, pt->base.name);
		sql_rel *s1, *dup = NULL;

		if(rel->r) {
			/* if (frame_find_var(sql, sub->base.name)) TODO ask Niels about this
				return sql_error(sql, 01, SQLSTATE(42000) "The name '%s' is already declared", sub->base.name);*/
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
	return rel_ddl_distribute(sql->sa, sel, NULL, NULL);
}

static sql_rel*
rel_propagate_insert(mvc *sql, sql_rel *rel, sql_table *t, int *changes)
{
	int colr = t->pcol->colnr, just_one = 1, found_nils = 0;
	sql_rel *anti_dup = rel_dup(rel->r) /* the anti relation */, *new_table = NULL, *sel = NULL;
	sql_exp *anti_exp = NULL, *anti_le = list_fetch(anti_dup->exps, colr), *accum = NULL, *aggr = NULL,
			*exception = NULL;
	list *anti_exps = new_exp_list(sql->sa);
	sql_subaggr *cf = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
	anti_le = exp_column(sql->sa, exp_relname(anti_le), exp_name(anti_le), exp_subtype(anti_le),
						 anti_le->card, has_nil(anti_le), is_intern(anti_le));
	char buf[BUFSIZ];

	for (node *n = t->members.set->h; n; n = n->next) {
		sql_part *pt = (sql_part *) n->data;
		sql_table *sub = find_sql_table(t->s, pt->base.name);
		sql_rel *s1, *dup = rel_dup(rel->r);
		sql_exp *le = list_fetch(dup->exps, colr);
		le = exp_column(sql->sa, exp_relname(le), exp_name(le), exp_subtype(le), le->card, has_nil(le), is_intern(le));

		if(isRangePartitionTable(t)) {
			sql_exp *e1, *e2;

			e1 = create_table_part_atom_exp(sql, pt->tpe, pt->part.range.minvalue);
			e2 = create_table_part_atom_exp(sql, pt->tpe, pt->part.range.maxvalue);
			dup = rel_compare_exp_(sql, dup, le, e1, e2, 3, 0);

			if(accum) {
				sql_exp *nr = exp_compare2(sql->sa, anti_le, exp_copy(sql->sa, e1), exp_copy(sql->sa, e2), 3);
				accum = exp_or(sql->sa, list_append(new_exp_list(sql->sa), accum),
							   list_append(new_exp_list(sql->sa), nr), 1);
			} else {
				accum = exp_compare2(sql->sa, anti_le, exp_copy(sql->sa, e1), exp_copy(sql->sa, e2), 3);
			}

			if(pt->with_nills) { /* handle the nulls case */
				sql_rel *extra;
				sql_exp *nils = rel_unop_(sql, le, NULL, "isnull", card_value),
						*anti_nils = rel_unop_(sql, anti_le, NULL, "isnull", card_value);
				nils = exp_compare(sql->sa, nils, exp_atom_bool(sql->sa, 1), cmp_equal);

				if(accum) {
					sql_exp *nr = exp_compare(sql->sa, anti_nils, exp_atom_bool(sql->sa, 1), cmp_notequal);
					accum = exp_or(sql->sa, list_append(new_exp_list(sql->sa), accum),
								   list_append(new_exp_list(sql->sa), nr), 1);
				} else {
					accum = exp_compare(sql->sa, anti_nils, exp_atom_bool(sql->sa, 1), cmp_notequal);
				}
				extra = rel_select(sql->sa, rel->r, nils);
				dup = rel_or(sql, NULL, dup, extra, NULL, NULL, NULL);
			}
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

		s1 = rel_insert(sql, new_table, dup);
		if (just_one == 0) {
			sel = rel_list(sql->sa, sel, s1);
		} else {
			sel = s1;
			just_one = 0;
		}
		(*changes)++;
	}

	if(isRangePartitionTable(t)) {
		if (list_length(t->members.set) == 1) //when there is just one partition must set the anti_exp
			set_anti(accum);
		anti_exp = accum;
	} else if(isListPartitionTable(t)) {
		anti_exp = exp_in(sql->sa, anti_le, anti_exps, cmp_notin);
		if(!found_nils) {
			sql_exp *anti_nils = rel_unop_(sql, anti_le, NULL, "isnull", card_value);
			anti_nils = exp_compare(sql->sa, anti_nils, exp_atom_bool(sql->sa, 1), cmp_equal);
			anti_exp = exp_or(sql->sa, list_append(new_exp_list(sql->sa), anti_exp),
							  list_append(new_exp_list(sql->sa), anti_nils), 0);
		}
	} else {
		assert(0);
	}
	//generate a count aggregation for the values not present in any of the partitions
	anti_dup = rel_select(sql->sa, anti_dup, anti_exp);
	anti_dup = rel_project(sql->sa, anti_dup, rel_projections(sql, anti_dup, NULL, 1, 1));
	anti_dup = rel_groupby(sql, anti_dup, NULL);
	aggr = exp_aggr(sql->sa, NULL, cf, 0, 0, anti_dup->card, 0);
	(void) rel_groupby_add_aggr(sql, anti_dup, aggr);
	exp_label(sql->sa, aggr, ++sql->label);

	//generate the exception
	aggr = exp_column(sql->sa, exp_relname(aggr), exp_name(aggr), exp_subtype(aggr), aggr->card,
					  has_nil(aggr), is_intern(aggr));
	snprintf(buf, BUFSIZ, "INSERT: the insert violates the partition %s of values",
			 isRangePartitionTable(t) ? "range" : "list");
	exception = exp_exception(sql->sa, aggr, buf);
	return rel_ddl_distribute(sql->sa, sel, anti_dup, list_append(new_exp_list(sql->sa), exception));
}

static sql_rel*
rel_propagate_update(mvc *sql, sql_rel *rel, sql_table *t, int *changes)
{
	int just_one = 1;
	sql_rel *sel = NULL;
	/*int found_part_col = 0;

	for (node *n = ((sql_rel*)rel->r)->exps->h; n; n = n->next) {
		sql_exp* exp = (sql_exp*) n->data;
		if(exp->type == e_column) {
			sql_exp* l = (sql_exp*) exp->l;
			if(!strcmp((char*)l->l, t->base.name) && !strcmp((char*)l->r, t->pcol->base.name))
				found_part_col = 1;
		}
	}*/

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

		//easy scenario where the partitioned column is not being updated
		s1 = rel_update(sql, rel_basetable(sql, sub, sub->base.name), dup, NULL, uexps);
		if (just_one == 0) {
			sel = rel_list(sql->sa, sel, s1);
		} else {
			sel = s1;
			just_one = 0;
		}
		(*changes)++;
	}
	return rel_ddl_distribute(sql->sa, sel, NULL, NULL);
}

static sql_rel*
rel_subtable_insert(mvc *sql, sql_rel *rel, sql_table *t, int *changes)
{
	sql_table *upper = t->p; //is part of a partition table and not been used yet
	int colr = upper->pcol->colnr;
	sql_part *pt = find_sql_part(upper, t->base.name);
	sql_rel *anti_dup = rel_dup(rel->r) /* the anti relation */, *right = rel->r, *left = rel->l;
	sql_exp *le = list_fetch(right->exps, colr), *anti_exp = NULL,
			*anti_le = list_fetch(anti_dup->exps, colr), *aggr = NULL, *exception = NULL;
	list *anti_exps = new_exp_list(sql->sa);
	sql_subaggr *cf = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
	char buf[BUFSIZ];

	le = exp_column(sql->sa, exp_relname(le), exp_name(le), exp_subtype(le), le->card, has_nil(le), is_intern(le));
	anti_le = exp_column(sql->sa, exp_relname(anti_le), exp_name(anti_le), exp_subtype(anti_le),
						 anti_le->card, has_nil(anti_le), is_intern(anti_le));

	if(isRangePartitionTable(upper)) {
		sql_exp *e1 = create_table_part_atom_exp(sql, pt->tpe, pt->part.range.minvalue),
				*e2 = create_table_part_atom_exp(sql, pt->tpe, pt->part.range.maxvalue);
		anti_exp = exp_compare2(sql->sa, anti_le, exp_copy(sql->sa, e1), exp_copy(sql->sa, e2), 3);
		set_anti(anti_exp);
	} else if(isListPartitionTable(upper)) {
		for(node *n = pt->part.values->h ; n ; n = n->next) {
			sql_part_value *next = (sql_part_value*) n->data;
			sql_exp *e1 = create_table_part_atom_exp(sql, next->tpe, next->value);
			list_append(anti_exps, exp_copy(sql->sa, e1));
		}
		anti_exp = exp_in(sql->sa, anti_le, anti_exps, cmp_notin);
	} else {
		assert(0);
	}
	if(!pt->with_nills) { /* handle the nulls case */
		sql_exp *anti_nils = rel_unop_(sql, anti_le, NULL, "isnull", card_value);
		anti_nils = exp_compare(sql->sa, anti_nils, exp_atom_bool(sql->sa, 1), cmp_equal);
		anti_exp = exp_or(sql->sa, list_append(new_exp_list(sql->sa), anti_exp),
						  list_append(new_exp_list(sql->sa), anti_nils), 0);
	}

	//generate a count aggregation for the values not present in any of the partitions
	anti_dup = rel_select(sql->sa, anti_dup, anti_exp);
	anti_dup = rel_project(sql->sa, anti_dup, rel_projections(sql, anti_dup, NULL, 1, 1));
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
	return rel_ddl_distribute(sql->sa, rel, anti_dup, list_append(new_exp_list(sql->sa), exception));
}

sql_rel *
rel_propagate(mvc *sql, sql_rel *rel, int *changes)
{
	sql_rel *l = rel->l;

	if(l->op == op_basetable) {
		sql_table *t = l->l;

		if(isRangePartitionTable(t) || isListPartitionTable(t)) {
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
		} else if(t->p && (isRangePartitionTable(t->p) || isListPartitionTable(t->p)) && !find_prop(l->p, PROP_USED)) {
			if(is_insert(rel->op)) { //insertion directly to sub-table (must do validation)
				return rel_subtable_insert(sql, rel, t, changes);
			} else if(is_update(rel->op)) { //do the proper validation
				//TODO
			} else if(!(is_delete(rel->op) || is_truncate(rel->op))) { //no validation needed in deletes
				assert(0);
			}
		}
	}
	return rel;
}
