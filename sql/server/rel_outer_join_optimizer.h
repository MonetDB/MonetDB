#include "monetdb_config.h"
#include "sql_relation.h"
#include "rel_exp.h"

#define _TODO assert(0)

static inline sql_exp*
replace_inner_column_references_with_null(mvc *sql, sql_exp* e, sql_rel* i /*inner relation*/) {

    sql_exp* t/*to-be-transformed*/ = exp_copy(sql, e);

    _TODO;

    return t;
}

static inline bool
check_if_selection_expression_is_null_rejecting(mvc *sql, sql_exp* e, sql_rel* i /*inner relation*/) {
    assert(e->type == e_cmp);

    sql_exp* t = replace_inner_column_references_with_null(sql, e, i);

	return exp_is_false(sql, e);
}

static sql_rel *
rel_outer2inner_join(mvc *sql, sql_rel *rel, int *changes)
{
	list *exps = NULL;
	sql_rel *r = NULL;
	node *n;

	exps = rel->exps;
	r = rel->l;

	/* push select through join */
	if (is_select(rel->op) && exps && r && r->op == op_left && !(rel_is_ref(r))) {
		if ()
		for (n = exps->h; n; n = n->next) { 
			sql_exp *e = n->data;
			if (e->type == e_cmp && !e->f && !is_complex_exp(e->flag)) {
				sql_rel *nr = NULL;
				sql_exp *re = e->r, *ne = rel_find_exp(r, re);

				if (ne && ne->card >= CARD_AGGR) 
					re->card = ne->card;

				if (re->card >= CARD_AGGR) {
					nr = rel_push_join(sql, r, e->l, re, NULL, e, 0);
				} else {
					nr = rel_push_select(sql, r, e->l, e, 0);
				}
				if (nr)
					rel->l = nr;
				/* only pushed down selects are counted */
				if (r == rel->l) {
					(*changes)++;
				} else { /* Do not introduce an extra select */
					sql_rel *r = rel->l;

					rel->l = r->l;
					r->l = NULL;
					list_append(rel->exps, e);
					rel_destroy(r);
				}
				assert(r == rel->l);
			} else {
				list_append(rel->exps, e);
			} 
		}
		return rel;
	}
	return rel;
}
