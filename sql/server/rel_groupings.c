/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"

#include "rel_groupings.h"
#include "rel_prop.h"
#include "rel_exp.h"
#include "rel_rel.h"
#include "sql_relation.h"
#include "sql_mvc.h"

sql_rel *
rel_generate_groupings(mvc *sql, sql_rel *rel)
{
	if (!rel)
		return rel;

	switch (rel->op) {
		case op_basetable:
		case op_table:
		case op_ddl:
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
			rel->l = rel_generate_groupings(sql, rel->l);
			rel->r = rel_generate_groupings(sql, rel->r);
			break;
		case op_project:
		case op_select:
		case op_topn:
		case op_sample:
			rel->l = rel_generate_groupings(sql, rel->l);
			break;
		case op_insert:
		case op_update:
		case op_delete:
		case op_truncate:
			rel->l = rel_generate_groupings(sql, rel->l);
			rel->r = rel_generate_groupings(sql, rel->r);
			break;
		case op_groupby: {
			prop *found;
			rel->l = rel_generate_groupings(sql, rel->l);

			/* ROLLUP, CUBE, GROUPING SETS cases */
			if ((found = find_prop(rel->p, PROP_GROUPINGS))) {
				list *sets = (list*) found->value;
				sql_rel *unions = NULL;

				for (node *n = sets->h ; n ; n = n->next) {
					sql_rel *nrel;
					list *l = (list*) n->data, *exps = sa_list(sql->sa), *pexps = sa_list(sql->sa);

					l = list_flaten(l);
					nrel = rel_groupby(sql, unions ? rel_dup(rel->l) : rel->l, l);

					for (node *m = rel->exps->h ; m ; m = m->next) {
						sql_exp *e = (sql_exp*) m->data, *ne = NULL;

						if (e->type == e_aggr && !strcmp(((sql_subaggr*) e->f)->aggr->base.name, "grouping")) {
							/* replace grouping aggregate calls with constants */
							sql_subtype tpe = ((sql_arg*) ((sql_subaggr*) e->f)->aggr->res->h->data)->type;
							list *groups = (list*) e->l;
							atom *a = atom_int(sql->sa, &tpe, 0);
#ifdef HAVE_HGE
							hge counter = (hge) list_length(groups) - 1;
#else
							lng counter = (lng) list_length(groups) - 1;
#endif
							assert(groups && list_length(groups) > 0);

							for (node *nn = groups->h ; nn ; nn = nn->next) {
								sql_exp *exp = (sql_exp*) nn->data;
								if (!exps_find_exp(l, exp)) {
									switch (ATOMstorage(a->data.vtype)) {
										case TYPE_bte:
											a->data.val.btval += (bte) (1 << counter);
											break;
										case TYPE_sht:
											a->data.val.shval += (sht) (1 << counter);
											break;
										case TYPE_int:
											a->data.val.ival += (int) (1 << counter);
											break;
										case TYPE_lng:
											a->data.val.lval += (lng) (1 << counter);
											break;
#ifdef HAVE_HGE
										case TYPE_hge:
											a->data.val.hval += (hge) (1 << counter);
											break;
#endif
										default:
											assert(0);
									}
								}
								counter--;
							}

							ne = exp_atom(sql->sa, a);
							exp_setname(sql->sa, ne, e->alias.rname, e->alias.name);
						} else if (e->type == e_column && !exps_find_exp(l, e)) { 
							/* do not include in the output of the group by, but add to the project as null */
							ne = exp_atom(sql->sa, atom_null_value(sql->sa, &(e->tpe)));
							exp_setname(sql->sa, ne, e->alias.rname, e->alias.name);
						} else {
							ne = exp_ref(sql->sa, e);
							append(exps, e);
						}
						append(pexps, ne);
					}
					nrel->exps = exps;
					nrel = rel_project(sql->sa, nrel, pexps);
					set_grouping_totals(nrel);

					if (!unions)
						unions = nrel;
					else {
						unions = rel_setop(sql->sa, unions, nrel, op_union);
						unions->exps = rel_projections(sql, rel, NULL, 1, 0);
						set_processed(unions);
					}
					if (!unions)
						return unions;
				}
				return unions;
			}
		} break;
	}
	return rel;
}
