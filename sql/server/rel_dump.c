/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#define LINESIZE 160
#define TABSTOP 2

#include "rel_dump.h"
#include "rel_rel.h"
#include "rel_basetable.h"
#include "rel_exp.h"
#include "rel_updates.h"
#include "rel_select.h"
#include "rel_remote.h"
#include "rel_rewriter.h"
#include "rel_optimizer.h"
#include "sql_privileges.h"

static void
print_indent(mvc *sql, stream *fout, int depth, int decorate)
{
	char buf[LINESIZE+1];
	int i;

	(void)sql;
	if (!decorate) {
		mnstr_printf(fout, "\n");
		return ;
	}
	depth *= TABSTOP;
	if (depth > LINESIZE)
		depth = LINESIZE;
	for (i = 0; i < depth; i++){
		if ((i % TABSTOP) == 0)
			buf[i] = '|';
		else
			buf[i] = ' ';
	}
	buf[i] = 0;
	mnstr_printf(fout, "\n=%s", buf);
}

static void
cmp_print(mvc *sql, stream *fout, int cmp)
{
	char *r = NULL;

	(void)sql;
	switch(cmp) {
	case cmp_gt: 		r = ">"; break;
	case cmp_gte: 		r = ">="; break;
	case cmp_lte: 		r = "<="; break;
	case cmp_lt: 		r = "<"; break;
	case cmp_equal: 	r = "="; break;
	case cmp_notequal: 	r = "!="; break;

	case cmp_filter: 	r = "filter"; break;
	case cmp_or: 		r = "or"; break;
	case cmp_in: 		r = "in"; break;
	case cmp_notin: 	r = "notin"; break;

	case mark_in: 		r = "any ="; break;
	case mark_notin: 	r = "all <>"; break;

	case cmp_all:
	case cmp_project:
	case cmp_joined:
	case cmp_left_project:
				r = "inner"; break;
	}
	mnstr_printf(fout, " %s ", r);
}

static const char *
dump_escape_ident(sql_allocator *sa, const char *s)
{
	char *res = NULL;
	if (s) {
		size_t l = strlen(s);
		char *r = SA_NEW_ARRAY(sa, char, (l * 2) + 1);

		res = r;
		while (*s) {
			if (*s == '"' || *s == '\\')
				*r++ = '\\';
			*r++ = *s++;
		}
		*r = '\0';
	}
	return res;
}

static char *
dump_sql_subtype(sql_allocator *sa, sql_subtype *t)
{
	char buf[BUFSIZ];

	if (t->digits && t->scale)
		snprintf(buf, BUFSIZ, "%s(%u,%u)", t->type->base.name, t->digits, t->scale);
	else if (t->digits)
		snprintf(buf, BUFSIZ, "%s(%u)", t->type->base.name, t->digits);
	else
		snprintf(buf, BUFSIZ, "%s", t->type->base.name);
	return sa_strdup(sa, buf);
}

static void exps_print(mvc *sql, stream *fout, list *exps, int depth, list *refs, int alias, int brackets);

static void rel_print_rel(mvc *sql, stream  *fout, sql_rel *rel, int depth, list *refs, int decorate);

void
exp_print(mvc *sql, stream *fout, sql_exp *e, int depth, list *refs, int comma, int alias)
{
	(void)sql;
	if (!e)
		return;
	/*mnstr_printf(fout, "%p ", e);*/
	switch(e->type) {
	case e_psm: {
		if (e->flag & PSM_SET) {
			const char *rname = exp_relname(e);
			int level = GET_PSM_LEVEL(e->flag);
			if (rname)
				mnstr_printf(fout, "\"%s\".", dump_escape_ident(sql->ta, rname));
			mnstr_printf(fout, "\"%s\" = ",  dump_escape_ident(sql->ta, exp_name(e)));
			exp_print(sql, fout, e->l, depth, refs, 0, 0);
			mnstr_printf(fout, " FRAME %d ", level);
			alias = 0;
		} else if (e->flag & PSM_VAR) {
			// todo output table def (from e->f)
			const char *rname = exp_relname(e);
			char *type_str = e->f ? NULL : dump_sql_subtype(sql->ta, exp_subtype(e));
			int level = GET_PSM_LEVEL(e->flag);
			mnstr_printf(fout, "declare ");
			if (rname)
				mnstr_printf(fout, "\"%s\".", dump_escape_ident(sql->ta, rname));
			mnstr_printf(fout, "\"%s\" %s FRAME %d ", dump_escape_ident(sql->ta, exp_name(e)), type_str ? type_str : "", level);
			alias = 0;
		} else if (e->flag & PSM_RETURN) {
			int level = GET_PSM_LEVEL(e->flag);
			mnstr_printf(fout, "return ");
			exp_print(sql, fout, e->l, depth, refs, 0, 0);
			mnstr_printf(fout, " FRAME %d ", level);
			alias = 0;
		} else if (e->flag & PSM_WHILE) {
			mnstr_printf(fout, "while ");
			exp_print(sql, fout, e->l, depth, refs, 0, 0);
			exps_print(sql, fout, e->r, depth, refs, 0, 0);
			alias = 0;
		} else if (e->flag & PSM_IF) {
			mnstr_printf(fout, "if ");
			exp_print(sql, fout, e->l, depth, refs, 0, 0);
			exps_print(sql, fout, e->r, depth, refs, 0, 0);
			if (e->f)
				exps_print(sql, fout, e->f, depth, refs, 0, 0);
			alias = 0;
		} else if (e->flag & PSM_REL) {
			rel_print_rel(sql, fout, e->l, depth+10, refs, 1);
		} else if (e->flag & PSM_EXCEPTION) {
			mnstr_printf(fout, "except ");
			exp_print(sql, fout, e->l, depth, refs, 0, 0);
			mnstr_printf(fout, " error %s", (const char *) e->r);
			alias = 0;
		}
	 	break;
	}
	case e_convert: {
		char *to_type = dump_sql_subtype(sql->ta, exp_subtype(e));
		mnstr_printf(fout, "%s[", to_type);
		exp_print(sql, fout, e->l, depth, refs, 0, 0);
		mnstr_printf(fout, "]");
	 	break;
	}
	case e_atom: {
		if (e->l) {
			atom *a = e->l;
			if (atom_type(a)->type->localtype == TYPE_ptr) {
				sql_table *t = a->data.val.pval;
				mnstr_printf(fout, "%s(\"%s\")",
					isMergeTable(t)?"merge table":
					isReplicaTable(t)?"replica table":"table",
					dump_escape_ident(sql->ta, t->base.name));
			} else {
				char *t = dump_sql_subtype(sql->ta, atom_type(a));
				if (a->isnull)
					mnstr_printf(fout, "%s NULL", t);
				else {
					char *s = ATOMformat(a->data.vtype, VALptr(&a->data));
					if (s && *s == '"')
						mnstr_printf(fout, "%s %s", t, s);
					else if (s)
						mnstr_printf(fout, "%s \"%s\"", t, s);
					GDKfree(s);
				}
			}
		} else { /* variables */
			if (e->r) { /* named parameters and declared variables */
				sql_var_name *vname = (sql_var_name*) e->r;
				if (vname->sname)
					mnstr_printf(fout, "\"%s\".", dump_escape_ident(sql->ta, vname->sname));
				mnstr_printf(fout, "\"%s\"", dump_escape_ident(sql->ta, vname->name));
			} else if (e->f) {	/* values list */
				list *l = e->f;
				exps_print(sql, fout, l, depth, refs, 0, 0);
			} else { /* numbered arguments */
				mnstr_printf(fout, "A%u", e->flag);
			}
		}
	} 	break;
	case e_func: {
		sql_subfunc *f = e->f;
		mnstr_printf(fout, "\"%s\".\"%s\"",
				f->func->s?dump_escape_ident(sql->ta, f->func->s->base.name):"sys",
				dump_escape_ident(sql->ta, f->func->base.name));
		exps_print(sql, fout, e->l, depth, refs, 0, 1);
		if (e->r) { /* list of optional lists */
			list *l = e->r;
			for(node *n = l->h; n; n = n->next)
				exps_print(sql, fout, n->data, depth, refs, 0, 1);
		}
		if (e->flag && is_compare_func(f))
			mnstr_printf(fout, " %s", e->flag==1?"ANY":"ALL");
	} 	break;
	case e_aggr: {
		sql_subfunc *a = e->f;
		mnstr_printf(fout, "\"%s\".\"%s\"",
				a->func->s?dump_escape_ident(sql->ta, a->func->s->base.name):"sys",
				dump_escape_ident(sql->ta, a->func->base.name));
		if (need_distinct(e))
			mnstr_printf(fout, " unique ");
		if (need_no_nil(e))
			mnstr_printf(fout, " no nil ");
		if (zero_if_empty(e))
			mnstr_printf(fout, " zero if empty ");
		if (e->l)
			exps_print(sql, fout, e->l, depth, refs, 0, 1);
		else
			mnstr_printf(fout, "()");
	} 	break;
	case e_column:
		if (is_freevar(e))
			mnstr_printf(fout, "!!!FREE!!! ");
		if (e->l)
			mnstr_printf(fout, "\"%s\".", dump_escape_ident(sql->ta, (char*)e->l));
		mnstr_printf(fout, "\"%s\"", dump_escape_ident(sql->ta, (char*)e->r));
		if (exp_relname(e) && exp_name(e) && e->l && e->r &&
			strcmp(exp_relname(e), e->l) == 0 &&
			strcmp(exp_name(e), e->r) == 0)
			alias = 0;
		if (!exp_relname(e) && exp_name(e) && !e->l && strcmp(exp_name(e), e->r)==0)
			alias = 0;
	 	break;
	case e_cmp:
		if (e->flag == cmp_in || e->flag == cmp_notin) {
			mnstr_printf(fout, "(");
			exp_print(sql, fout, e->l, depth+1, refs, 0, 0);
			mnstr_printf(fout, ")");
			if (is_anti(e))
				mnstr_printf(fout, " !");
			cmp_print(sql, fout, e->flag);
			exps_print(sql, fout, e->r, depth, refs, 0, 1);
		} else if (e->flag == cmp_or) {
			exps_print(sql, fout, e->l, depth, refs, 0, 1);
			if (is_anti(e))
				mnstr_printf(fout, " !");
			cmp_print(sql, fout, e->flag);
			exps_print(sql, fout, e->r, depth, refs, 0, 1);
		} else if (e->flag == cmp_filter) {
			sql_subfunc *f = e->f;

			exps_print(sql, fout, e->l, depth, refs, 0, 1);
			if (is_anti(e))
				mnstr_printf(fout, " !");
			mnstr_printf(fout, " FILTER \"%s\".\"%s\"",
					f->func->s?dump_escape_ident(sql->ta, f->func->s->base.name):"sys",
					dump_escape_ident(sql->ta, f->func->base.name));
			exps_print(sql, fout, e->r, depth, refs, 0, 1);
		} else if (e->f) {
			mnstr_printf(fout, "(");
			exp_print(sql, fout, e->r, depth+1, refs, 0, 0);
			mnstr_printf(fout, ")");
			if (is_anti(e))
				mnstr_printf(fout, " !");
			cmp_print(sql, fout, swap_compare(range2lcompare(e->flag)));
			mnstr_printf(fout, "(");
			exp_print(sql, fout, e->l, depth+1, refs, 0, 0);
			mnstr_printf(fout, ")");
			if (is_anti(e))
				mnstr_printf(fout, " !");
			cmp_print(sql, fout, range2rcompare(e->flag));
			mnstr_printf(fout, "(");
			exp_print(sql, fout, e->f, depth+1, refs, 0, 0);
			mnstr_printf(fout, ")");
			if (is_symmetric(e))
				mnstr_printf(fout, " SYM");
		} else {
			mnstr_printf(fout, "(");
			exp_print(sql, fout, e->l, depth+1, refs, 0, 0);
			mnstr_printf(fout, ")");
			if (is_anti(e))
				mnstr_printf(fout, " !");
			if (is_semantics(e))
				mnstr_printf(fout, " *");
			cmp_print(sql, fout, e->flag);

			mnstr_printf(fout, "(");
			exp_print(sql, fout, e->r, depth+1, refs, 0, 0);
			mnstr_printf(fout, ")");
		}
	 	break;
	default:
		;
	}
	if (e->type != e_atom && e->type != e_cmp && is_ascending(e))
		mnstr_printf(fout, " ASC");
	if (e->type != e_atom && e->type != e_cmp && nulls_last(e))
		mnstr_printf(fout, " NULLS LAST");
	if (e->type != e_atom && e->type != e_cmp && !has_nil(e))
		mnstr_printf(fout, " NOT NULL");
	if (e->type != e_atom && e->type != e_cmp && is_unique(e))
		mnstr_printf(fout, " UNIQUE");
	if (e->p && e->type != e_atom && !exp_is_atom(e)) { /* don't show properties on value lists */
		for (prop *p = e->p; p; p = p->p) {
			/* Don't show min/max/unique est on atoms, or when running tests with forcemito */
			if ((GDKdebug & FORCEMITOMASK) == 0 || (p->kind != PROP_MIN && p->kind != PROP_MAX && p->kind != PROP_NUNIQUES)) {
				char *pv = propvalue2string(sql->ta, p);
				mnstr_printf(fout, " %s %s", propkind2string(p), pv);
			}
		}
	}
	if (exp_name(e) && alias) {
		mnstr_printf(fout, " as ");
		if (exp_relname(e))
			mnstr_printf(fout, "\"%s\".", dump_escape_ident(sql->ta, exp_relname(e)));
		mnstr_printf(fout, "\"%s\"", dump_escape_ident(sql->ta, exp_name(e)));
	}
	if (comma)
		mnstr_printf(fout, ", ");
}

static void
exps_print(mvc *sql, stream *fout, list *exps, int depth, list *refs, int alias, int brackets)
{
	node *en;

	if (brackets)
		mnstr_printf(fout, "(");
	else
		mnstr_printf(fout, " [ ");
	if (exps)
		for (en = exps->h; en; en = en->next)
			exp_print(sql, fout, en->data, depth+1, refs, (en->next!=NULL), alias);
	if (brackets)
		mnstr_printf(fout, ")");
	else
		mnstr_printf(fout, " ]");
}

static int
find_ref( list *refs, sql_rel *rel )
{
	node *n;
	int nr = 1;

	for(n=refs->h; n; n = n->next, nr++){
		if (n->data == rel)
			return nr;
	}
	return 0;
}

static void
rel_print_rel(mvc *sql, stream  *fout, sql_rel *rel, int depth, list *refs, int decorate)
{
	char *r = NULL;

	if (!rel)
		return;

	if (rel_is_ref(rel)) {
		int nr = list_length(refs) + 1;
		int cnt = rel->ref.refcnt;
		mnstr_printf(fout, "\n%cREF %d (%d)", decorate?'=':' ', nr, cnt);
	}

	print_indent(sql, fout, depth, decorate);

	if (is_single(rel))
		mnstr_printf(fout, "single ");

	switch (rel->op) {
	case op_basetable: {
		sql_table *t = rel->l;
		const char *sname = t->s ? t->s->base.name : NULL; /* All tables, but declared ones on the stack have schema */
		const char *tname = t->base.name;

		if (isRemote(t)) {
			const char *uri = t->query;

			sname = mapiuri_schema( uri, sql->sa, sname);
			tname = mapiuri_table( uri, sql->sa, tname);
		}
		if (sname)
			mnstr_printf(fout, "%s(\"%s\".\"%s\")",
				isRemote(t)&&decorate?"REMOTE":
				isReplicaTable(t)?"REPLICA":"table",
				dump_escape_ident(sql->ta, sname), dump_escape_ident(sql->ta, tname));
		else
			mnstr_printf(fout, "%s(\"%s\")",
				isRemote(t)&&decorate?"REMOTE":
				isReplicaTable(t)?"REPLICA":"table",
				dump_escape_ident(sql->ta, tname));
		if (rel->exps)
			exps_print(sql, fout, rel->exps, depth, refs, 1, 0);
		else
			rel_base_dump_exps(fout, rel);
	} 	break;
	case op_table:
		mnstr_printf(fout, "table (");

		if (rel->r)
			exp_print(sql, fout, rel->r, depth, refs, 1, 0);
		if (rel->l) {
			if (rel->flag == TRIGGER_WRAPPER)
				mnstr_printf(fout, "rel_dump not yet implemented for trigger input");
			else
				rel_print_rel(sql, fout, rel->l, depth+1, refs, decorate);
		}
		print_indent(sql, fout, depth, decorate);
		mnstr_printf(fout, ")");
		if (rel->exps)
			exps_print(sql, fout, rel->exps, depth, refs, 1, 0);
		break;
	case op_ddl:
		mnstr_printf(fout, "ddl");
		if (rel->l)
			rel_print_rel(sql, fout, rel->l, depth+1, refs, decorate);
		if (rel->r)
			rel_print_rel(sql, fout, rel->r, depth+1, refs, decorate);
		if (rel->exps && (rel->flag == ddl_psm || rel->flag == ddl_exception || rel->flag == ddl_list))
			exps_print(sql, fout, rel->exps, depth, refs, 1, 0);
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
		r = "join";
		if (rel->op == op_left)
			r = "left outer join";
		else if (rel->op == op_right)
			r = "right outer join";
		else if (rel->op == op_full)
			r = "full outer join";
		else if (rel->op == op_semi)
			r = "semijoin";
		else if (rel->op == op_anti)
			r = "antijoin";
		else if (rel->op == op_union)
			r = "union";
		else if (rel->op == op_inter)
			r = "intersect";
		else if (rel->op == op_except)
			r = "except";
		else if (!rel->exps && rel->op == op_join)
			r = "crossproduct";

		if (is_dependent(rel))
			mnstr_printf(fout, "dependent ");
		if (need_distinct(rel))
			mnstr_printf(fout, "distinct ");
		mnstr_printf(fout, "%s (", r);
		if (rel->l) {
			if (rel_is_ref(rel->l)) {
				int nr = find_ref(refs, rel->l);
				print_indent(sql, fout, depth+1, decorate);
				mnstr_printf(fout, "& REF %d ", nr);
			} else
				rel_print_rel(sql, fout, rel->l, depth+1, refs, decorate);
		}
		mnstr_printf(fout, ",");
		if (rel->r) {
			if (rel_is_ref(rel->r)) {
				int nr = find_ref(refs, rel->r);
				print_indent(sql, fout, depth+1, decorate);
				mnstr_printf(fout, "& REF %d  ", nr);
			} else
				rel_print_rel(sql, fout, rel->r, depth+1, refs, decorate);
		}
		print_indent(sql, fout, depth, decorate);
		mnstr_printf(fout, ")");
		exps_print(sql, fout, rel->exps, depth, refs, 1, 0);
		if (is_join(rel->op) && rel->attr) /* mark joins */
			exps_print(sql, fout, rel->attr, depth, refs, 1, 0);
		break;
	case op_project:
	case op_select:
	case op_groupby:
	case op_topn:
	case op_sample:
		r = "project";
		if (rel->op == op_select)
			r = "select";
		if (rel->op == op_groupby)
			r = "group by";
		if (rel->op == op_topn)
			r = "top N";
		if (rel->op == op_sample)
			r = "sample";

		if (rel->l) {
			if (need_distinct(rel))
				mnstr_printf(fout, "distinct ");
			mnstr_printf(fout, "%s (", r);
			if (rel_is_ref(rel->l)) {
				int nr = find_ref(refs, rel->l);
				print_indent(sql, fout, depth+1, decorate);
				mnstr_printf(fout, "& REF %d ", nr);
			} else
				rel_print_rel(sql, fout, rel->l, depth+1, refs, decorate);
			print_indent(sql, fout, depth, decorate);
			mnstr_printf(fout, ")");
		}
		if (rel->op == op_groupby)  /* group by columns */
			exps_print(sql, fout, rel->r, depth, refs, 1, 0);
		exps_print(sql, fout, rel->exps, depth, refs, 1, 0);
		if (rel->r && rel->op == op_project) /* order by columns */
			exps_print(sql, fout, rel->r, depth, refs, 1, 0);
		break;
	case op_insert:
	case op_update:
	case op_delete:
	case op_truncate:
	case op_merge: {

		if (rel->op == op_insert)
			mnstr_printf(fout, "insert(");
		else if (rel->op == op_update)
			mnstr_printf(fout, "update(");
		else if (rel->op == op_delete)
			mnstr_printf(fout, "delete(");
		else if (rel->op == op_merge)
			mnstr_printf(fout, "merge(");
		else if (rel->op == op_truncate) {
			assert(list_length(rel->exps) == 2);
			sql_exp *first = (sql_exp*) rel->exps->h->data, *second = (sql_exp*) rel->exps->h->next->data;
			int restart_sequences = ((atom*)first->l)->data.val.ival,
				drop_action = ((atom*)second->l)->data.val.ival;
			mnstr_printf(fout, "truncate %s identity, %s(", restart_sequences ? "restart" : "continue",
												   drop_action ? "cascade" : "restrict");
		}

		if (rel->l) {
			if (rel_is_ref(rel->l)) {
				int nr = find_ref(refs, rel->l);
				print_indent(sql, fout, depth+1, decorate);
				mnstr_printf(fout, "& REF %d ", nr);
			} else
				rel_print_rel(sql, fout, rel->l, depth+1, refs, decorate);
		}
		if (rel->r) {
			if (rel_is_ref(rel->r)) {
				int nr = find_ref(refs, rel->r);
				print_indent(sql, fout, depth+1, decorate);
				mnstr_printf(fout, "& REF %d ", nr);
			} else
				rel_print_rel(sql, fout, rel->r, depth+1, refs, decorate);
		}
		print_indent(sql, fout, depth, decorate);
		mnstr_printf(fout, ")");
		if (rel->op != op_truncate && rel->op != op_merge && rel->exps)
			exps_print(sql, fout, rel->exps, depth, refs, 1, 0);
	} 	break;
	default:
		assert(0);
	}
	if (rel->p) {
		for (prop *p = rel->p; p; p = p->p) {
			if (p->kind != PROP_COUNT || (GDKdebug & FORCEMITOMASK) == 0) {
				char *pv = propvalue2string(sql->ta, p);
				mnstr_printf(fout, " %s %s", propkind2string(p), pv);
			}
		}
	}
}

void
rel_print_refs(mvc *sql, stream* fout, sql_rel *rel, int depth, list *refs, int decorate)
{
	if (!rel)
		return;
	switch (rel->op) {
	case op_basetable:
	case op_table:
		break;
	case op_ddl:
		if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l) {
				rel_print_refs(sql, fout, rel->l, depth, refs, decorate);
				if (rel_is_ref(rel->l) && !find_ref(refs, rel->l)) {
					rel_print_rel(sql, fout, rel->l, depth, refs, decorate);
					list_append(refs, rel->l);
				}
			}
			if (rel->r) {
				rel_print_refs(sql, fout, rel->r, depth, refs, decorate);
				if (rel_is_ref(rel->r) && !find_ref(refs, rel->r)) {
					rel_print_rel(sql, fout, rel->r, depth, refs, decorate);
					list_append(refs, rel->r);
				}
			}
		}
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
			rel_print_refs(sql, fout, rel->l, depth, refs, decorate);
		if (rel->r)
			rel_print_refs(sql, fout, rel->r, depth, refs, decorate);
		if (rel->l && rel_is_ref(rel->l) && !find_ref(refs, rel->l)) {
			rel_print_rel(sql, fout, rel->l, depth, refs, decorate);
			list_append(refs, rel->l);
		}
		if (rel->r && rel_is_ref(rel->r) && !find_ref(refs, rel->r)) {
			rel_print_rel(sql, fout, rel->r, depth, refs, decorate);
			list_append(refs, rel->r);
		}
		break;
	case op_project:
	case op_select:
	case op_groupby:
	case op_topn:
	case op_sample:
		if (rel->l)
			rel_print_refs(sql, fout, rel->l, depth, refs, decorate);
		if (rel->l && rel_is_ref(rel->l) && !find_ref(refs, rel->l)) {
			rel_print_rel(sql, fout, rel->l, depth, refs, decorate);
			list_append(refs, rel->l);
		}
		break;
	case op_insert:
	case op_update:
	case op_delete:
	case op_truncate:
	case op_merge:
		if (rel->l)
			rel_print_refs(sql, fout, rel->l, depth, refs, decorate);
		if (rel->l && rel_is_ref(rel->l) && !find_ref(refs, rel->l)) {
			rel_print_rel(sql, fout, rel->l, depth, refs, decorate);
			list_append(refs, rel->l);
		}
		if (rel->r)
			rel_print_refs(sql, fout, rel->r, depth, refs, decorate);
		if (rel->r && rel_is_ref(rel->r) && !find_ref(refs, rel->r)) {
			rel_print_rel(sql, fout, rel->r, depth, refs, decorate);
			list_append(refs, rel->r);
		}
		break;
	}
}

void
rel_print_(mvc *sql, stream *fout, sql_rel *rel, int depth, list *refs, int decorate)
{
	rel_print_rel(sql, fout, rel, depth, refs, decorate);
	if (sql->runs) {
		for (int i = 0 ; i < NSQLREWRITERS ; i++) {
			sql_optimizer_run *run = &(sql->runs[i]);

			if (run->name) { /* if name is set, then the optimizer did run */
				print_indent(sql, fout, depth, decorate);
				mnstr_printf(fout, "%-36s %3d actions " LLFMT " usec",
							 run->name, run->nchanges, run->time);
			}
		}
	}
}

static void
skipWS( char *r, int *pos)
{
	while(r[*pos] && (isspace((unsigned char) r[*pos]) || r[*pos] == '|'))
		(*pos)++;
}

static void
skipUntilWS( char *r, int *pos)
{
	while(r[*pos] && (!isspace((unsigned char) r[*pos]) || r[*pos] == '|'))
		(*pos)++;
}

static void
skipIdent( char *r, int *pos)
{
	if (r[*pos] == '"') {
		(*pos)++;
		while (r[*pos] && r[*pos] != '"') {
			/* We send escaped '"' and '\' characters */
			if (r[*pos] == '\\' && (r[*pos + 1] == '"' || r[*pos + 1] == '\\'))
				(*pos)+=2;
			else
				(*pos)++;
		}
	} else {
		while(r[*pos] && (isalnum((unsigned char) r[*pos]) || r[*pos] == '_' || r[*pos] == '%'))
			(*pos)++;
	}
}

static void
convertIdent(char *r)
{
	int i = 0, j = 0;
	while (r[i] && r[i] != '"') {
		/* We send escaped '"' and '\' characters */
		if (r[i] == '\\' && (r[i + 1] == '"' || r[i + 1] == '\\')) {
			r[j++] = r[i + 1];
			i+=2;
		} else {
			r[j++] = r[i++];
		}
	}
	r[j] = '\0';
}

static void
skipIdentOrSymbol( char *r, int *pos)
{
	if (r[*pos] == '"') {
		skipIdent(r, pos);
	} else {
		while(r[*pos] && (isalnum((unsigned char) r[*pos]) ||
				  r[*pos] == '=' ||
				  r[*pos] == '_' || r[*pos] == '%' ||
				  r[*pos] == '<' || r[*pos] == '>' ||
				  r[*pos] == '/' || r[*pos] == '*' ||
				  r[*pos] == '-' || r[*pos] == '+' ||
				  r[*pos] == '~' || r[*pos] == '^' ))
			(*pos)++;
	}
}

static int
readInt( char *r, int *pos)
{
	int res = 0;

	while (isdigit((unsigned char) r[*pos])) {
		res *= 10;
		res += r[*pos]-'0';
		(*pos)++;
	}
	return res;
}

static void *
readAtomString(int localtype, char *r, int *pos)
{
	void *res = NULL;
	size_t nbytes = 0;
	int firstpos = 0, rtype = ATOMstorage(localtype) == TYPE_str ? TYPE_str : localtype;

	/* TODO I had issues with the 'external' flag on the JSONfromString function, maybe something is missing there? */
	assert(r[*pos] == '"'); /* skip first '"' */
	(*pos)++;

	firstpos = *pos;
	if (rtype == TYPE_str) /* string reads require double quotes at the beginning */
		firstpos--;
	while (r[*pos] && r[*pos] != '"') { /* compute end of atom string */
		if (r[*pos] == '\\')
			(*pos)+=2;
		else
			(*pos)++;
	}
	if (!r[*pos])
		return NULL;

	assert(r[*pos] == '"'); /* skip second '"' */
	if (rtype != TYPE_str) /* string reads require double quotes at the end */
		r[*pos] = '\0';
	(*pos)++;

	if (ATOMfromstr(rtype, &res, &nbytes, r + firstpos, true) < 0) {
		GDKfree(res);
		return NULL;
	}
	return res;
}

static sql_exp* exp_read(mvc *sql, sql_rel *lrel, sql_rel *rrel, list *top_exps, char *r, int *pos, int grp);

static sql_exp*
read_prop(mvc *sql, sql_exp *exp, char *r, int *pos, bool *found)
{
	/* PROPs */
	if (strncmp(r+*pos, "JOINIDX",  strlen("JOINIDX")) == 0) {
		char *sname, *tname, *iname;
		sql_schema *s = NULL;
		prop *p;

		(*pos)+= (int) strlen("JOINIDX");
		skipWS(r, pos);
		/* schema.table.index */
		sname = r+*pos + 1;
		skipIdent(r,pos);
		convertIdent(sname);
		(*pos)++;
		if (r[*pos] != '.')
			return sql_error(sql, -1, SQLSTATE(42000) "JOINIDX: missing '.'\n");
		(*pos)++;
		tname = r+*pos + 1;
		skipIdent(r,pos);
		convertIdent(tname);
		(*pos)++;
		if (r[*pos] != '.')
			return sql_error(sql, -1, SQLSTATE(42000) "JOINIDX: missing '.'\n");
		(*pos)++;
		iname = r+*pos + 1;
		skipIdent(r,pos);
		convertIdent(iname);
		(*pos)++;

		(void) tname;
		s = mvc_bind_schema(sql, sname);
		if (sname && !s)
			return sql_error(sql, -1, SQLSTATE(42000) "Schema %s missing\n", sname);
		if (!find_prop(exp->p, PROP_JOINIDX)) {
			p = exp->p = prop_create(sql->sa, PROP_JOINIDX, exp->p);
			if (!(p->value.pval = mvc_bind_idx(sql, s, iname)))
				return sql_error(sql, -1, SQLSTATE(42000) "Index %s missing\n", iname);
		}
		skipWS(r,pos);
		if (found)
			*found = true;
	}
	return exp;
}

static list*
read_exps(mvc *sql, sql_rel *lrel, sql_rel *rrel, list *top_exps, char *r, int *pos, char bracket, int grp, int top)
{
	list *exps = new_exp_list(sql->sa);
	sql_exp *e;
	char ebracket = (bracket == '[')?']':')';

	if (r[*pos] == bracket) {
		skipWS( r, pos);

		(*pos)++;
		skipWS( r, pos);
		e = exp_read(sql, lrel, rrel, top ? exps : top_exps, r, pos, grp);
		if (!e && r[*pos] != ebracket) {
			return sql_error(sql, -1, SQLSTATE(42000) "Missing closing %c\n", ebracket);
		} else if (!e) {
			(*pos)++;
			skipWS(r, pos);
			return sql->errstr[0] ? NULL : exps; /* A function call might not have any input expressions, so return empty exps on that case */
		}
		append(exps, e);
		skipWS( r, pos);
		if (!read_prop(sql, e, r, pos, NULL))
			return NULL;
		while (r[*pos] == ',') {

			(*pos)++;
			skipWS( r, pos);
			e = exp_read(sql, lrel, rrel, top ? exps : top_exps, r, pos, grp);
			if (!e)
				return NULL;
			append(exps, e);
			skipWS( r, pos);
			if (!read_prop(sql, e, r, pos, NULL))
				return NULL;
		}
		if (r[*pos] != ebracket)
			return sql_error(sql, -1, SQLSTATE(42000) "Missing closing %c\n", ebracket);
		(*pos)++;
		skipWS( r, pos);
	}
	return exps;
}

static sql_exp*
exp_read_min_or_max(mvc *sql, sql_exp *exp, char *r, int *pos, const char *prop_str, rel_prop kind)
{
	atom *a;
	sql_subtype *tpe = exp_subtype(exp);

	(*pos)+= (int) strlen(prop_str);
	skipWS(r, pos);

	if (strncmp(r+*pos, "NULL",  strlen("NULL")) == 0) {
		(*pos)+= (int) strlen("NULL");
		a = atom_general(sql->sa, tpe, NULL);
	} else {
		void *ptr = readAtomString(tpe->type->localtype, r, pos);
		if (!ptr)
			return sql_error(sql, -1, SQLSTATE(42000) "Invalid atom string\n");
		a = atom_general_ptr(sql->sa, tpe, ptr);
		GDKfree(ptr);
	}
	if (!find_prop(exp->p, kind)) {
		prop *p = exp->p = prop_create(sql->sa, kind, exp->p);
		p->value.pval = a;
	}
	skipWS(r, pos);
	return exp;
}

static sql_exp*
exp_read_nuniques(mvc *sql, sql_exp *exp, char *r, int *pos)
{
	void *ptr = NULL;
	size_t nbytes = 0;
	ssize_t res = 0;
	sql_subtype *tpe = sql_bind_localtype("dbl");

	(*pos)+= (int) strlen("NUNIQUES");
	skipWS(r, pos);

	if ((res = ATOMfromstr(tpe->type->localtype, &ptr, &nbytes, r + *pos, true)) < 0) {
		GDKfree(ptr);
		return sql_error(sql, -1, SQLSTATE(42000) "Invalid atom string\n");
	}

	if (!find_prop(exp->p, PROP_NUNIQUES)) {
		prop *p = exp->p = prop_create(sql->sa, PROP_NUNIQUES, exp->p);
		p->value.dval = *(dbl*)ptr;
	}
	(*pos) += (int) res; /* it should always fit */
	GDKfree(ptr);
	skipWS(r, pos);
	return exp;
}

static sql_exp*
read_exp_properties(mvc *sql, sql_exp *exp, char *r, int *pos)
{
	bool found = true;
	while (found) {
		found = false;

		if (strncmp(r+*pos, "COUNT",  strlen("COUNT")) == 0) {
			(*pos)+= (int) strlen("COUNT");
			if (!find_prop(exp->p, PROP_COUNT))
				exp->p = prop_create(sql->sa, PROP_COUNT, exp->p);
			skipWS(r,pos);
			found = true;
		} else if (strncmp(r+*pos, "HASHIDX",  strlen("HASHIDX")) == 0) {
			(*pos)+= (int) strlen("HASHIDX");
			if (!find_prop(exp->p, PROP_HASHIDX))
				exp->p = prop_create(sql->sa, PROP_HASHIDX, exp->p);
			skipWS(r,pos);
			found = true;
		} else if (strncmp(r+*pos, "HASHCOL",  strlen("HASHCOL")) == 0) {
			(*pos)+= (int) strlen("HASHCOL");
			if (!find_prop(exp->p, PROP_HASHCOL))
				exp->p = prop_create(sql->sa, PROP_HASHCOL, exp->p);
			skipWS(r,pos);
			found = true;
		} else if (strncmp(r+*pos, "MIN",  strlen("MIN")) == 0) {
			if (!exp_read_min_or_max(sql, exp, r, pos, "MIN", PROP_MIN))
				return NULL;
			found = true;
		} else if (strncmp(r+*pos, "MAX",  strlen("MAX")) == 0) {
			if (!exp_read_min_or_max(sql, exp, r, pos, "MAX", PROP_MAX))
				return NULL;
			found = true;
		} else if (strncmp(r+*pos, "NUNIQUES",  strlen("NUNIQUES")) == 0) {
			if (!exp_read_nuniques(sql, exp, r, pos))
				return NULL;
			found = true;
		}
		if (!read_prop(sql, exp, r, pos, &found))
			return NULL;
	}
	return exp;
}

static sql_exp*
parse_atom(mvc *sql, char *r, int *pos, sql_subtype *tpe)
{
	if (strncmp(r+*pos, "NULL",  strlen("NULL")) == 0) {
		(*pos)+= (int) strlen("NULL");
		return exp_atom(sql->sa, atom_general(sql->sa, tpe, NULL));
	} else {
		void *ptr = readAtomString(tpe->type->localtype, r, pos);
		if (!ptr)
			return sql_error(sql, -1, SQLSTATE(42000) "Invalid atom string\n");
		sql_exp *res = exp_atom(sql->sa, atom_general_ptr(sql->sa, tpe, ptr));
		GDKfree(ptr);
		return res;
	}
}

static sql_exp*
function_error_string(mvc *sql, const char *schema, const char *fname, list *exps, bool found, sql_ftype type)
{
	char *arg_list = NULL, *F = NULL, *fn = NULL;

	FUNC_TYPE_STR(type, F, fn)

	(void) F;
	if (!list_empty(exps)) {
		for (node *n = exps->h; n ; n = n->next) {
			sql_subtype *t = exp_subtype(n->data);
			char *tpe = t ? sql_subtype_string(sql->ta, t) : "?";

			if (arg_list) {
				arg_list = sa_message(sql->ta, "%s, %s", arg_list, tpe);
			} else {
				arg_list = tpe;
			}
		}
	}
	return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "%s %s %s%s%s'%s'(%s)",
					found ? "Insufficient privileges for" : "No such", fn, schema ? "'":"", schema ? schema : "",
					schema ? "'.":"", fname, arg_list ? arg_list : "");
}

static unsigned int /* keep updating the label count */
try_update_label_count(mvc *sql, const char *label)
{
	if (label && label[0] == '%' && isdigit(label[1])) {
		char *eptr = NULL;
		unsigned int value = (unsigned int) strtol(label + 1, &eptr, 10);
		if (eptr && eptr[0] == '\0') {
			sql->label = MAX(sql->label, value);
			return value;
		}
	}
	return 0;
}

static sql_exp*
exp_read(mvc *sql, sql_rel *lrel, sql_rel *rrel, list *top_exps, char *r, int *pos, int grp)
{
	int old, d=0, s=0, unique = 0, no_nils = 0, quote = 0, zero_if_empty = 0;
	char *tname = NULL, *cname = NULL, *var_cname = NULL, *e, *b = r + *pos;
	sql_exp *exp = NULL;
	list *exps = NULL;
	sql_type *t = NULL;
	sql_subtype tpe;

	quote = (r[*pos] == '"');
	b += quote;
	skipIdent(r, pos);
	e = r+*pos;
	(*pos) += quote;
	skipWS(r, pos);
	switch(r[*pos]) {
	case '.':
		*e = 0;
		(*pos)++;
		tname = b;
		convertIdent(tname);
		cname = r + *pos + quote;
		skipIdentOrSymbol(r, pos);
		e = r+*pos;
		if (quote) {
			old = ' ';
			convertIdent(cname);
		} else {
			old = *e;
		}
		*e = 0;

		tname = sa_strdup(sql->sa, tname);
		cname = sa_strdup(sql->sa, cname);
		*e = old;
		skipWS(r, pos);
		if (r[*pos] != '(') { /* if there's a function/aggregate call next don't attempt to bind columns */
			if (top_exps) {
				exp = exps_bind_column2(top_exps, tname, cname, NULL);
				if (exp)
					exp = exp_alias_or_copy(sql, tname, cname, lrel, exp);
			}
			if (!exp && lrel) {
				exp = rel_bind_column2(sql, lrel, tname, cname, 0);
				if (!exp && rrel)
					exp = rel_bind_column2(sql, rrel, tname, cname, 0);
			} else if (!exp) {
				exp = exp_column(sql->sa, tname, cname, NULL, CARD_ATOM, 1, 0, cname[0] == '%');
			}
		}
		break;
	/* atom */
	case '(':
		if (b == (r+*pos)) { /* comparison expression */
			int anti = 0, sym = 0, semantics = 0;
			comp_type ctype = cmp_all, ctype2 = cmp_all;
			list *lexps = NULL, *rexps = NULL, *fexps = NULL;
			char *sname = NULL, *fname = NULL;

			if (!(lexps = read_exps(sql, lrel, rrel, top_exps, r, pos, '(', 0, 0)))
				return NULL;
			skipWS(r, pos);
			if (r[*pos] == '!') {
				anti = 1;
				(*pos)++;
				skipWS(r, pos);
			}
			if (r[*pos] == '*') {
				semantics = 1;
				(*pos)++;
				skipWS(r, pos);
			}

			switch(r[*pos]) {
			case 'a':
				if (strncmp(r+*pos, "any =",  strlen("any =")) == 0) {
					(*pos)+= (int) strlen("any =");
					ctype = mark_in;
				} else if (strncmp(r+*pos, "all <>",  strlen("all <>")) == 0) {
					(*pos)+= (int) strlen("all <>");
					ctype = mark_notin;
				}
				break;
			case 'n':
				if (strncmp(r+*pos, "notin",  strlen("notin")) == 0) {
					(*pos)+= (int) strlen("notin");
					ctype = cmp_notin;
				}
				break;
			case 'F':
				if (strncmp(r+*pos, "FILTER",  strlen("FILTER")) == 0) {
					(*pos)+= (int) strlen("FILTER");
					ctype = cmp_filter;
					skipWS(r, pos);
					sname = r+*pos + 1;
					skipIdent(r, pos);
					convertIdent(sname);
					(*pos)+=2;
					fname = r+*pos + 1;
					skipIdent(r, pos);
					convertIdent(fname);
					(*pos)++;
				}
				break;
			case 'i':
				if (strncmp(r+*pos, "in",  strlen("in")) == 0) {
					(*pos)+= (int) strlen("in");
					ctype = cmp_in;
				}
				break;
			case 'o':
				if (strncmp(r+*pos, "or",  strlen("or")) == 0) {
					(*pos)+= (int) strlen("or");
					ctype = cmp_or;
				}
				break;
			case '!':
				ctype = cmp_notequal;
				(*pos)++;
				if (r[(*pos)] == '=')
					(*pos)++;
				break;
			case '=':
				ctype = cmp_equal;
				(*pos)++;
				break;
			case '<':
				ctype = cmp_lt;
				(*pos)++;
				if (r[(*pos)] == '=') {
					ctype = cmp_lte;
					(*pos)++;
				}
				break;
			case '>':
				ctype = cmp_gt;
				(*pos)++;
				if (r[(*pos)] == '=') {
					ctype = cmp_gte;
					(*pos)++;
				}
				break;
			default:
				return sql_error(sql, -1, SQLSTATE(42000) "Type: missing comparison type\n");
			}

			skipWS(r, pos);
			if (!(rexps = read_exps(sql, lrel, rrel, top_exps, r, pos, '(', 0, 0)))
				return NULL;
			skipWS(r, pos);

			switch (ctype) {
				case cmp_gt:
				case cmp_gte:
				case cmp_lte:
				case cmp_lt:
				case cmp_equal:
				case cmp_notequal:
				case mark_in:
				case mark_notin:
					if (r[*pos] == '!' || r[*pos] == '<' || r[*pos] == '>') { /* BETWEEN case */
						if (r[*pos] == '!') { /* ignore next anti */
							(*pos)++;
							skipWS(r, pos);
						}
						switch(r[*pos]) {
						case '<':
							ctype2 = cmp_lt;
							(*pos)++;
							if (r[(*pos)] == '=') {
								ctype2 = cmp_lte;
								(*pos)++;
							}
							break;
						case '>':
							ctype2 = cmp_gt;
							(*pos)++;
							if (r[(*pos)] == '=') {
								ctype2 = cmp_gte;
								(*pos)++;
							}
							break;
						default:
							return sql_error(sql, -1, SQLSTATE(42000) "Type: missing comparison type\n");
						}
						skipWS(r, pos);
						if (!(fexps = read_exps(sql, lrel, rrel, top_exps, r, pos, '(', 0, 0)))
							return NULL;
						skipWS(r, pos);
						if (strncmp(r+*pos, "SYM",  strlen("SYM")) == 0) {
							(*pos)+= (int) strlen("SYM");
							skipWS(r, pos);
							sym = 1;
						}
						exp = exp_compare2(sql->sa, rexps->h->data, lexps->h->data, fexps->h->data, compare2range(swap_compare(ctype), ctype2), sym);
					} else {
						exp = exp_compare(sql->sa, lexps->h->data, rexps->h->data, ctype);
						if (semantics)
							set_semantics(exp);
					}
					if (anti)
						set_anti(exp);
					assert(list_length(lexps) == 1 && list_length(rexps) == 1 && (!fexps || list_length(fexps) == 1));
					break;
				case cmp_in:
				case cmp_notin:
					assert(list_length(lexps) == 1);
					exp = exp_in(sql->sa, lexps->h->data, rexps, ctype);
					if (anti)
						set_anti(exp);
					break;
				case cmp_filter: {
					sql_subfunc *f = NULL;
					list *tl = sa_list(sql->sa);

					if (!list_empty(lexps)) {
						for (node *n = lexps->h; n; n = n->next){
							sql_exp *e = n->data;

							list_append(tl, exp_subtype(e));
						}
					}
					if (!list_empty(rexps)) {
						for (node *n = rexps->h; n; n = n->next){
							sql_exp *e = n->data;

							list_append(tl, exp_subtype(e));
						}
					}

					if (sname && !mvc_bind_schema(sql, sname))
						return sql_error(sql, ERR_NOTFOUND, SQLSTATE(3F000) "No such schema '%s'\n", sname);
					if (!(f = sql_bind_func_(sql, sname, fname, tl, F_FILT, true)))
						return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "Filter: missing function '%s'.'%s'\n", sname, fname);
					if (!execute_priv(sql, f->func))
						return sql_error(sql, -1, SQLSTATE(42000) "Filter: no privilege to call filter function '%s'.'%s'\n", sname, fname);
					exp = exp_filter(sql->sa, lexps, rexps, f, anti);
				} break;
				case cmp_or:
					exp = exp_or(sql->sa, lexps, rexps, anti);
					break;
				default:
					return sql_error(sql, -1, SQLSTATE(42000) "Type: missing comparison type\n");
			}
			break;
		}
		/* fall through */
	case '[':
		tname = b;
		if (tname && *tname == '[') { /* list of values */
			if (!(exps = read_exps(sql, lrel, rrel, top_exps, r, pos, '[', 0, 0)))
				return NULL;
			exp = exp_values(sql->sa, exps);
		} else {
			old = *e;
			*e = 0;
			if (old != '[') {
				(*pos)++;
				d = readInt(r,pos);
				if (r[*pos] != ')' && r[*pos] != ',')
					return sql_error(sql, -1, SQLSTATE(42000) "Type: missing ')' or ','\n");
				if (r[*pos] == ',') {
					(*pos)++;
					s = readInt(r,pos);
				}
				if (r[*pos] != ')')
					return sql_error(sql, -1, SQLSTATE(42000) "Type: missing ')'\n");
				(*pos)++;
			}
			convertIdent(tname);
			if (!sql_find_subtype(&tpe, tname, d, s)) {
				if (!(t = mvc_bind_type(sql, tname))) /* try an external type */
					return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "SQL type %s(%d, %d) not found\n", tname, d, s);
				sql_init_subtype(&tpe, t, d, s);
			}
			skipWS(r, pos);
			*e = old;
			if (r[*pos] == '[') { /* convert */
				(*pos)++;
				skipWS(r, pos);
				if (!(exp = exp_read(sql, lrel, rrel, top_exps, r, pos, 0)))
					return NULL;
				if (r[*pos] != ']')
					return sql_error(sql, -1, SQLSTATE(42000) "Convert: missing ']'\n");
				(*pos)++;
				skipWS(r, pos);
				exp = exp_convert(sql->sa, exp, exp_subtype(exp), &tpe);
			} else {
				if (!(exp = parse_atom(sql, r, pos, &tpe)))
					return NULL;
				skipWS(r, pos);
			}
		}
		break;
	case '\"':
	case 'N': /* for NULL values, but 'NOT NULL' and 'NULLS LAST' cannot match here */
		if (r[*pos] == '\"' || (strncmp(r+*pos, "NULL", strlen("NULL")) == 0 && r[*pos+4] != 'S')) {
			*e = 0;
			tname = b;
			convertIdent(tname);
			if (!sql_find_subtype(&tpe, tname, 0, 0)) {
				if (!(t = mvc_bind_type(sql, tname))) /* try an external type */
					return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "SQL type %s not found\n", tname);
				sql_init_subtype(&tpe, t, 0, 0);
			}
			if (!(exp = parse_atom(sql, r, pos, &tpe)))
				return NULL;
			skipWS(r, pos);
		}
		break;
	default:
		(void)sql;
	}

	/* func or aggr */
	if (grp) {
		skipWS(r, pos);
		if (r[*pos] == 'u') {
			unique = 1;
			(*pos)+= (int) strlen("unique");
			skipWS(r, pos);
		}
		if (r[*pos] == 'n') {
			no_nils = 1;
			(*pos)+= (int) strlen("no nil");
			skipWS(r, pos);
		}
		if (r[*pos] == 'z') {
			zero_if_empty = 1;
			(*pos)+= (int) strlen("zero if empty");
			skipWS(r, pos);
		}
	}
	if (r[*pos] == '(') {
		sql_subfunc *f = NULL;

		if (!(exps = read_exps(sql, lrel, rrel, top_exps, r, pos, '(', 0, 0)))
			return NULL;
		tname = b;
		*e = 0;
		convertIdent(tname);
		if (tname && !mvc_bind_schema(sql, tname))
			return sql_error(sql, ERR_NOTFOUND, SQLSTATE(3F000) "No such schema '%s'\n", tname);
		if (grp) {
			if (exps && exps->h) {
				list *ops = sa_list(sql->sa);
				for( node *n = exps->h; n; n = n->next)
					append(ops, exp_subtype(n->data));
				f = sql_bind_func_(sql, tname, cname, ops, F_AGGR, true);
			} else {
				f = sql_bind_func(sql, tname, cname, sql_bind_localtype("void"), NULL, F_AGGR, true); /* count(*) */
			}
			if (!f)
				return function_error_string(sql, tname, cname, exps, false, F_AGGR);
			if (!execute_priv(sql, f->func))
				return function_error_string(sql, tname, cname, exps, true, F_AGGR);
			exp = exp_aggr(sql->sa, exps, f, unique, no_nils, CARD_ATOM, 1);
			if (zero_if_empty)
				set_zero_if_empty(exp);
		} else {
			int nops = list_length(exps);
			if (!strcmp(tname, "sys") && (!strcmp(cname, "case") || !strcmp(cname, "casewhen") || !strcmp(cname, "coalesce") || !strcmp(cname, "nullif"))) {
				/* these functions are bound on a different way */
				if ((f = sql_find_func(sql, NULL, cname, 2, F_FUNC, true, NULL))) {
					if (!execute_priv(sql, f->func))
						return function_error_string(sql, tname, cname, exps, true, F_FUNC);
					sql_exp *res = exps->t->data;
					sql_subtype *restype = exp_subtype(res);
					f->res->h->data = sql_create_subtype(sql->sa, restype->type, restype->digits, restype->scale);
				}
			} else {
				list *ops = sa_list(sql->sa);
				for( node *n = exps->h; n; n = n->next)
					append(ops, exp_subtype(n->data));

				f = sql_bind_func_(sql, tname, cname, ops, F_FUNC, true);
				if (!f) {
					sql->session->status = 0; /* if the function was not found clean the error */
					sql->errstr[0] = '\0';
					f = sql_bind_func_(sql, tname, cname, ops, F_ANALYTIC, true);
				}
				if (!f && nops > 1) { /* window functions without frames get 2 extra arguments */
					sql->session->status = 0; /* if the function was not found clean the error */
					sql->errstr[0] = '\0';
					list_remove_node(ops, NULL, ops->t);
					list_remove_node(ops, NULL, ops->t);
					f = sql_bind_func_(sql, tname, cname, ops, F_ANALYTIC, true);
				}
				if (!f && nops > 4) { /* window functions with frames get 5 extra arguments */
					sql->session->status = 0; /* if the function was not found clean the error */
					sql->errstr[0] = '\0';
					for (int i = 0 ; i < 3 ; i++)
						list_remove_node(ops, NULL, ops->t);
					f = sql_bind_func_(sql, tname, cname, ops, F_ANALYTIC, true);
				}

				if (f && !execute_priv(sql, f->func))
					return function_error_string(sql, tname, cname, exps, true, F_FUNC);
				/* apply scale fixes if needed */
				if (f && f->func->type != F_ANALYTIC) {
					if (list_length(exps) == 2) {
						sql_exp *l = exps->h->data;
						sql_exp *r = exps->h->next->data;

						/* Find converted value type for division and update function output type */
						if (f->func->fix_scale == SCALE_DIV) {
							sql_subtype *lt = exp_subtype(l);
							sql_subtype *rt = exp_subtype(r);

							if (lt->type->scale == SCALE_FIX && rt->scale && strcmp(sql_func_imp(f->func), "/") == 0) {
								sql_subtype *res = f->res->h->data;
								unsigned int scale = lt->scale - rt->scale;
								unsigned int digits = (lt->digits > rt->digits) ? lt->digits : rt->digits;

#ifdef HAVE_HGE
								if (res->type->radix == 10 && digits > 38)
									digits = 38;
								if (res->type->radix == 2 && digits > 128)
									digits = 128;
#else
								if (res->type->radix == 10 && digits > 18)
									digits = 18;
								if (res->type->radix == 2 && digits > 64)
									digits = 64;
#endif

								sql_find_subtype(res, lt->type->base.name, digits, scale);
							}
						} else if (f->func->fix_scale == SCALE_MUL) {
							exp_sum_scales(f, l, r);
						} else if (f->func->fix_scale == DIGITS_ADD) {
							sql_subtype *t1 = exp_subtype(l);
							sql_subtype *t2 = exp_subtype(r);
							sql_subtype *res = f->res->h->data;

							if (t1->digits && t2->digits) {
								res->digits = t1->digits + t2->digits;
								if (res->digits < t1->digits || res->digits < t2->digits || res->digits >= (unsigned int) INT32_MAX)
									return sql_error(sql, -1, SQLSTATE(42000) "Output number of digits for %s%s%s is too large\n", tname ? tname : "", tname ? "." : "", cname);
							} else {
								res->digits = 0;
							}
						}
					} else if (list_length(exps) > 2) {
						if (!f->func->vararg && !(exps = check_arguments_and_find_largest_any_type(sql, lrel, exps, f, 0)))
							return NULL;
					}
				}
			}
			if (f) {
				exp = exp_op(sql->sa, list_empty(exps) ? NULL : exps, f);
				if (is_compare_func(f)) { /* has to parse any/all */
					skipWS(r,pos);
					/* [ ANY|ALL ] */
					if (strncmp(r+*pos, "ANY",  strlen("ANY")) == 0) {
						(*pos)+= (int) strlen("ANY");
						skipWS(r, pos);
						exp->flag = 1;
					}
					if (strncmp(r+*pos, "ALL",  strlen("ALL")) == 0) {
						(*pos)+= (int) strlen("ALL");
						skipWS(r, pos);
						exp->flag = 2;
					}
				}
			} else {
				return function_error_string(sql, tname, cname, exps, false, F_FUNC);
			}
		}
	}

	if (!exp && lrel && b != e) { /* simple ident */
		int amb = 0, mul = 0;

		old = *e;
		*e = 0;
		convertIdent(b);
		var_cname = sa_strdup(sql->sa, b);
		if (top_exps) {
			exp = exps_bind_column(top_exps, var_cname, &amb, &mul, 1);
			if (exp)
				exp = exp_alias_or_copy(sql, exp_relname(exp), var_cname, lrel, exp);
		}
		(void)amb;
		(void)mul;
		assert(amb == 0 && mul == 0);
		if (!exp && lrel)
			exp = rel_bind_column(sql, lrel, var_cname, 0, 1);
		if (!exp && rrel)
			exp = rel_bind_column(sql, rrel, var_cname, 0, 1);
		*e = old;
		skipWS(r,pos);
	}

	if (!exp && (cname || var_cname)) { /* Try a variable */
		sql_var *var = NULL;
		sql_subtype *tpe = NULL;
		int level = 0;
		sql_arg *a = NULL;
		bool has_tname = cname && tname && strcmp(tname, cname) != 0;

		if (find_variable_on_scope(sql, has_tname ? tname : NULL, cname ? cname : var_cname, &var, &a, &tpe, &level, "SELECT")) {
			if (var) /* if variable is known from the stack or a global var */
				exp = exp_param_or_declared(sql->sa, var->sname ? sa_strdup(sql->sa, var->sname) : NULL, sa_strdup(sql->sa, var->name), &(var->var.tpe), level);
			if (a) /* if variable is a parameter */
				exp = exp_param_or_declared(sql->sa, NULL, sa_strdup(sql->sa, cname), &(a->type), level);
		}
	}

	if (!exp) {
		if (cname) {
			bool has_tname = tname && strcmp(tname, cname) != 0;
			return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "Identifier %s%s%s doesn't exist\n", has_tname ? tname : "", has_tname ? "." : "", cname);
		} else if (var_cname) {
			return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "Identifier %s doesn't exist\n", var_cname);
		}
		return NULL;
	}

	/* [ ASC ] */
	if (strncmp(r+*pos, "ASC",  strlen("ASC")) == 0) {
		(*pos)+= (int) strlen("ASC");
		skipWS(r, pos);
		set_ascending(exp);
	}
	/* [ NULLS LAST ] */
	if (strncmp(r+*pos, "NULLS LAST",  strlen("NULLS LAST")) == 0) {
		(*pos)+= (int) strlen("NULLS LAST");
		skipWS(r, pos);
		set_nulls_last(exp);
	}
	/* [ NOT NULL ] */
	if (strncmp(r+*pos, "NOT NULL",  strlen("NOT NULL")) == 0) {
		(*pos)+= (int) strlen("NOT NULL");
		skipWS(r, pos);
		set_has_no_nil(exp);
	}
	/* [ UNIQUE ] */
	if (strncmp(r+*pos, "UNIQUE",  strlen("UNIQUE")) == 0) {
		(*pos)+= (int) strlen("UNIQUE");
		skipWS(r, pos);
		set_unique(exp);
	}

	if (!(exp = read_exp_properties(sql, exp, r, pos)))
		return NULL;

	/* as alias */
	if (strncmp(r+*pos, "as", 2) == 0) {
		unsigned int rlabel = 0, nlabel = 0;
		(*pos)+=2;
		skipWS(r, pos);

		tname = r+*pos+1;
		skipIdent(r, pos);
		convertIdent(tname);
		(*pos)++;
		if (r[*pos] != '.') {
			cname = tname;
			tname = NULL;
			exp_setname(sql->sa, exp, NULL, cname);
			skipWS(r, pos);
		} else {
			(*pos)++;
			cname = r+*pos+1;
			skipIdent(r, pos);
			convertIdent(cname);
			(*pos)++;
			skipWS(r, pos);
			exp_setname(sql->sa, exp, tname, cname);
		}
		rlabel = try_update_label_count(sql, tname);
		nlabel = try_update_label_count(sql, cname);
		if (rlabel && rlabel == nlabel)
			exp->alias.label = rlabel;
	}
	return exp;
}

static int
rel_set_types(mvc *sql, sql_rel *rel)
{
	list *iexps = rel_projections( sql, rel->l, NULL, 0, 1);
	node *n, *m;

	if (!iexps || list_length(iexps) > list_length(rel->exps))
		return -1;
	for(n=iexps->h, m=rel->exps->h; n && m; n = n->next, m = m->next) {
		sql_exp *e = m->data;

		if (!e->tpe.type)
			e->tpe = *exp_subtype( n->data );
	}
	return 0;
}

static sql_rel*
rel_read_count(mvc *sql, sql_rel *rel, char *r, int *pos)
{
	void *ptr = NULL;
	size_t nbytes = 0;
	ssize_t res = 0;
	sql_subtype *tpe = sql_bind_localtype("oid");

	(*pos)+= (int) strlen("COUNT");
	skipWS(r, pos);

	if ((res = ATOMfromstr(tpe->type->localtype, &ptr, &nbytes, r + *pos, true)) < 0) {
		GDKfree(ptr);
		return sql_error(sql, -1, SQLSTATE(42000) "Invalid atom string\n");
	}

	set_count_prop(sql->sa, rel, *(BUN*)ptr);
	(*pos) += (int) res; /* it should always fit */
	GDKfree(ptr);
	skipWS(r, pos);
	return rel;
}

static sql_rel*
read_rel_properties(mvc *sql, sql_rel *rel, char *r, int *pos)
{
	bool found = true;
	while (found) {
		found = false;

		if (strncmp(r+*pos, "COUNT",  strlen("COUNT")) == 0) {
			if (!rel_read_count(sql, rel, r, pos))
				return NULL;
			found = true;
		} else if (strncmp(r+*pos, "REMOTE", strlen("REMOTE")) == 0) { /* Remote tables under remote tables not supported, so remove REMOTE property */
			(*pos)+= (int) strlen("REMOTE");
			skipWS(r, pos);
			skipUntilWS(r, pos);
			skipWS(r, pos);
			found = true;
		} else if (strncmp(r+*pos, "USED", strlen("USED")) == 0) {
			(*pos)+= (int) strlen("USED");
			if (!find_prop(rel->p, PROP_USED))
				rel->p = prop_create(sql->sa, PROP_USED, rel->p);
			skipWS(r, pos);
			found = true;
		} else if (strncmp(r+*pos, "GROUPINGS", strlen("GROUPINGS")) == 0) {
			(*pos)+= (int) strlen("GROUPINGS");
			if (!find_prop(rel->p, PROP_GROUPINGS))
				rel->p = prop_create(sql->sa, PROP_GROUPINGS, rel->p);
			skipWS(r, pos);
			found = true;
		}
	}
	return rel;
}

sql_rel*
rel_read(mvc *sql, char *r, int *pos, list *refs)
{
	sql_rel *rel = NULL, *nrel, *lrel, *rrel;
	list *exps, *gexps;
	int distinct = 0, dependent = 0, single = 0;
	operator_type j = op_basetable;

	skipWS(r,pos);
	if (r[*pos] == 'R') {
		*pos += (int) strlen("REF");

		skipWS(r, pos);
		(void)readInt(r,pos);
		skipWS(r, pos);
		(*pos)++; /* ( */
		(void)readInt(r,pos); /* skip nr refs */
		(*pos)++; /* ) */
		if (!(rel = rel_read(sql, r, pos, refs)))
			return NULL;
		append(refs, rel);
		skipWS(r,pos);
	}
	if (r[*pos] == '&') {
		int nr;
		(*pos)++;
		skipWS(r, pos);
		*pos += (int) strlen("REF");
		skipWS(r, pos);
		nr = readInt(r,pos); /* skip nr refs */
		return rel_dup(list_fetch(refs, nr-1));
	}

	if (r[*pos] == 'i' && r[*pos+1] == 'n' && r[*pos+2] == 's') {
		sql_table *t;

		*pos += (int) strlen("insert");
		skipWS(r, pos);
		(*pos)++; /* ( */
		if (!(lrel = rel_read(sql, r, pos, refs))) /* to be inserted relation */
			return NULL;
		skipWS(r,pos);
		if (!(rrel = rel_read(sql, r, pos, refs))) /* the inserts relation */
			return NULL;
		skipWS(r,pos);
		(*pos)++; /* ) */

		t = get_table(lrel);
		if (!insert_allowed(sql, t, t->base.name, "INSERT", "insert"))
			return NULL;

		if (!(rel = rel_insert(sql, lrel, rrel)) || !(rel = read_rel_properties(sql, rel, r, pos)))
			return NULL;
	}

	if (r[*pos] == 'd' && r[*pos+1] == 'e' && r[*pos+2] == 'l') {
		sql_table *t;

		*pos += (int) strlen("delete");
		skipWS(r, pos);
		(*pos)++; /* ( */
		if (!(lrel = rel_read(sql, r, pos, refs))) /* to be deleted relation */
			return NULL;
		skipWS(r,pos);
		if (!(rrel = rel_read(sql, r, pos, refs))) /* the deletes relation */
			return NULL;
		skipWS(r,pos);
		(*pos)++; /* ) */

		t = get_table(lrel);
		if (!update_allowed(sql, t, t->base.name, "DELETE", "delete", 1))
			return NULL;

		if (!(rel = rel_delete(sql->sa, lrel, rrel)) || !(rel = read_rel_properties(sql, rel, r, pos)))
			return NULL;
	}

	if (r[*pos] == 't' && r[*pos+1] == 'r' && r[*pos+2] == 'u') {
		sql_table *t;
		int restart_sequences = 0, drop_action = 0;

		*pos += (int) strlen("truncate ");
		if (r[*pos] == 'r') {
			restart_sequences = 1;
			*pos += (int) strlen("restart identity, ");
		} else {
			*pos += (int) strlen("continue identity, ");
		}
		if (r[*pos] == 'c') {
			drop_action = 1;
			*pos += (int) strlen("cascade");
		} else {
			*pos += (int) strlen("restrict");
		}
		skipWS(r, pos);
		(*pos)++; /* ( */
		if (!(lrel = rel_read(sql, r, pos, refs))) /* to be truncated relation */
			return NULL;
		skipWS(r,pos);
		(*pos)++; /* ) */

		t = get_table(lrel);
		if (!update_allowed(sql, t, t->base.name, "TRUNCATE", "truncate", 2))
			return NULL;

		if (!(rel = rel_truncate(sql->sa, lrel, restart_sequences, drop_action)) || !(rel = read_rel_properties(sql, rel, r, pos)))
			return NULL;
	}

	if (r[*pos] == 'u' && r[*pos+1] == 'p' && r[*pos+2] == 'd') {
		sql_table *t;
		list *nexps = new_exp_list(sql->sa);

		*pos += (int) strlen("update");
		skipWS(r, pos);
		(*pos)++; /* ( */
		if (!(lrel = rel_read(sql, r, pos, refs))) /* to be updated relation */
			return NULL;
		skipWS(r,pos);
		if (!(rrel = rel_read(sql, r, pos, refs))) /* the updates relation */
			return NULL;
		skipWS(r,pos);
		(*pos)++; /* ) */

		t = get_table(lrel);
		if (!update_allowed(sql, t, t->base.name, "UPDATE", "update", 0) )
			return NULL;

		if (!(exps = read_exps(sql, lrel, rrel, NULL, r, pos, '[', 0, 1))) /* columns to be updated */
			return NULL;

		for (node *n = rel->exps->h ; n ; n = n->next) {
			sql_exp *e = (sql_exp *) n->data;
			const char *cname = exp_name(e);

			if (cname[0] != '%') { /* Skip TID column */
				sql_column *c = mvc_bind_column(sql, t, cname);

				if (!c)
					return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42S22) "UPDATE: no such column '%s.%s'\n", t->base.name, cname);
				if (!(e = update_check_column(sql, t, c, e, rrel, c->base.name, "UPDATE")))
					return NULL;
			}
			list_append(nexps, e);
		}

		if (!(rel = rel_update(sql, lrel, rrel, NULL, nexps)) || !(rel = read_rel_properties(sql, rel, r, pos)))
			return NULL;
	}

	if (r[*pos] == 'm' && r[*pos+1] == 'e' && r[*pos+2] == 'r')
		return sql_error(sql, -1, SQLSTATE(42000) "Merge statements not supported in remote plans\n");

	if (r[*pos] == 'd' && r[*pos+1] == 'i') {
		*pos += (int) strlen("distinct");
		skipWS(r, pos);
		distinct = 1;
	}
	if (r[*pos] == 's' && r[*pos+1] == 'i') {
		*pos += (int) strlen("single");
		skipWS(r, pos);
		single = 1;
	}
	if (r[*pos] == 'd' && r[*pos+1] == 'e') {
		*pos += (int) strlen("dependent");
		skipWS(r, pos);
		dependent = 1;
	}

	switch(r[*pos]) {
	case 't':
		if (r[*pos+1] == 'a') {
			sql_schema *s = NULL;
			sql_table *t = NULL;
			char *sname, *tname;
			*pos += (int) strlen("table");
			skipWS(r, pos);
			if (r[*pos] != '(')
				return sql_error(sql, -1, SQLSTATE(42000) "Table: missing '('\n");
			(*pos)++;
			skipWS(r, pos);
			sname = r+*pos + 1;
			skipIdent(r, pos);
			convertIdent(sname);
			(*pos)++;
			if (r[*pos] != '.')
				return sql_error(sql, -1, SQLSTATE(42000) "Table: missing '.' in table name\n");
			(*pos)++;
			tname = r+*pos + 1;
			skipIdent(r, pos);
			convertIdent(tname);
			(*pos)++;
			skipWS(r, pos);
			if (r[*pos] == '(') { /* table returning function */
				node *m;
				sql_exp *tudf, *next;
				list *inputs, *outputs;
				sql_subfunc *sf;
				int x = *pos, y; /* save current position, after parsing the input relation we have to parse the input parameters */
				bool inside_identifier = false;

				while (r[*pos] && (inside_identifier || r[*pos] != '\n')) { /* the input parameters must be parsed after the input relation, skip them for now  */
					if (inside_identifier && r[*pos] == '\\' && (r[*pos + 1] == '"' || r[*pos + 1] == '\\')) {
						(*pos)+=2;
					} else if (r[*pos] == '"') {
						inside_identifier = !inside_identifier;
						(*pos)++;
					} else {
						(*pos)++;
					}
				}
				if (r[*pos] != '\n')
					return sql_error(sql, -1, SQLSTATE(42000) "Table returning function: missing ']' for output parameters\n");

				skipWS(r, pos); /* now parse the input relation */
				if (!(lrel = rel_read(sql, r, pos, refs)))
					return NULL;
				y = *pos; /* later we have to return here to parse the output identifiers */
				*pos = x;
				if (!(inputs = read_exps(sql, lrel, NULL, NULL, r, pos, '(', 0, 1)))
					return NULL;

				if (!mvc_bind_schema(sql, sname))
					return sql_error(sql, ERR_NOTFOUND, SQLSTATE(3F000) "No such schema '%s'\n", sname);
				if (!(tudf = find_table_function(sql, sname, tname, list_empty(inputs) ? NULL : inputs, list_empty(inputs) ? NULL : exp_types(sql->sa, inputs), F_UNION)))
					return NULL;
				sf = tudf->f;
				if (tudf->type != e_func || sf->func->type != F_UNION)
					return sql_error(sql, 02, SQLSTATE(42000) "'%s' does not return a table\n", exp_func_name(tudf));

				*pos = y; /* now at the end of the input relation */
				skipWS(r, pos);
				if (r[*pos] != ')')
					return sql_error(sql, -1, SQLSTATE(42000) "Table returning function: missing ')' at the end of the input relation\n");
				(*pos)++;
				skipWS(r, pos);

				/* Parse identifiers manually, we cannot use read_exps because the labels may not match */
				if (r[*pos] != '[')
					return sql_error(sql, -1, SQLSTATE(42000) "Table returning function: missing '[' for output parameters\n");
				(*pos)++;
				skipWS(r, pos);
				m = sf->func->res->h;
				outputs = new_exp_list(sql->sa);
				while (r[*pos] && r[*pos] != ']' && m) {
					sql_arg *a = m->data;
					unsigned int rlabel, nlabel;
					char *nrname, *ncname;

					if (r[*pos] != '"')
						return sql_error(sql, -1, SQLSTATE(42000) "Table returning function: missing identifier for output parameters\n");
					nrname = r+*pos + 1;
					skipIdent(r, pos);
					if (r[*pos] != '"')
						return sql_error(sql, -1, SQLSTATE(42000) "Table returning function: missing identifier for output parameters\n");
					convertIdent(nrname);
					(*pos)++;
					if (r[*pos] != '.')
						return sql_error(sql, -1, SQLSTATE(42000) "Table returning function: missing '.' for output parameters\n");
					(*pos)++; /* skip '.' */
					if (r[*pos] != '"')
						return sql_error(sql, -1, SQLSTATE(42000) "Table returning function: missing identifier for output parameters\n");
					ncname = r+*pos + 1;
					skipIdent(r, pos);
					if (r[*pos] != '"')
						return sql_error(sql, -1, SQLSTATE(42000) "Table returning function: missing identifier for output parameters\n");
					convertIdent(ncname);
					(*pos)++;
					if (r[*pos] == ',')
						(*pos)++;

					next = exp_column(sql->sa, nrname, ncname, &a->type, CARD_MULTI, 1, 0, 0);
					rlabel = try_update_label_count(sql, nrname);
					nlabel = try_update_label_count(sql, ncname);
					if (rlabel && rlabel == nlabel)
						next->alias.label = rlabel;
					set_basecol(next);
					append(outputs, next);
					m = m->next;
					skipWS(r, pos);
				}
				if (r[*pos] != ']')
					return sql_error(sql, -1, SQLSTATE(42000) "Table returning function: missing ']' for output parameters\n");
				(*pos)++;
				skipWS(r, pos);
				if (list_length(outputs) != list_length(sf->func->res))
					return sql_error(sql, -1, SQLSTATE(42000) "Table returning function: the number of output parameters don't match the table ones relation outputs: %d != function outputs: %d\n",
									 list_length(outputs), list_length(sf->func->res));
				rel = rel_table_func(sql->sa, lrel, tudf, outputs, TABLE_FROM_RELATION);
				set_processed(rel);
			} else {
				if (r[*pos] != ')')
					sql_error(sql, -1, SQLSTATE(42000) "Table: missing ')'\n");
				(*pos)++;
				skipWS(r, pos);
				if (!(s = mvc_bind_schema(sql, sname)))
					return sql_error(sql, ERR_NOTFOUND, SQLSTATE(3F000) "No such schema '%s'\n", sname);
				if (!(t = mvc_bind_table(sql, s, tname)))
					return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42S02) "Table missing '%s.%s'\n", sname, tname);
				if (isMergeTable(t))
					return sql_error(sql, -1, SQLSTATE(42000) "Merge tables not supported under remote connections\n");
				if (isRemote(t))
					return sql_error(sql, -1, SQLSTATE(42000) "Remote tables not supported under remote connections\n");
				if (isReplicaTable(t))
					return sql_error(sql, -1, SQLSTATE(42000) "Replica tables not supported under remote connections\n");
				rel = rel_basetable(sql, t, tname);
				if (!table_privs(sql, t, PRIV_SELECT))  {
					rel_base_disallow(rel);
					if (rel_base_has_column_privileges(sql, rel) == 0)
						return sql_error(sql, -1, SQLSTATE(42000) "Access denied for %s to table '%s.%s'\n",
									 get_string_global_var(sql, "current_user"), s->base.name, tname);
				}
				rel_base_use_all(sql, rel);
				rel = rewrite_basetable(sql, rel);

				if (!r[*pos])
					return rel;

				/* scan aliases */
				if (!(exps = read_exps(sql, rel, NULL, NULL, r, pos, '[', 0, 1)))
					return NULL;
				rel->exps = exps;
			}
		} else { /* top N */
			*pos += (int) strlen("top N");
			skipWS(r, pos);
			if (r[*pos] != '(')
				return sql_error(sql, -1, SQLSTATE(42000) "Top N: missing '('\n");
			(*pos)++;
			skipWS(r, pos);
			if (!(nrel = rel_read(sql, r, pos, refs)))
				return NULL;
			if (r[*pos] != ')')
				return sql_error(sql, -1, SQLSTATE(42000) "Top N: missing ')'\n");
			(*pos)++;
			skipWS(r, pos);
			if (!(exps = read_exps(sql, nrel, NULL, NULL, r, pos, '[', 0, 1)))
				return NULL;
			rel = rel_topn(sql->sa, nrel, exps);
			set_processed(rel);
		}
		break;
	case 'p':
		*pos += (int) strlen("project");
		skipWS(r, pos);

		if (r[*pos] != '(')
			return sql_error(sql, -1, SQLSTATE(42000) "Project: missing '('\n");
		(*pos)++;
		skipWS(r, pos);
		if (!(nrel = rel_read(sql, r, pos, refs)))
			return NULL;
		skipWS(r, pos);
		if (r[*pos] != ')')
			return sql_error(sql, -1, SQLSTATE(42000) "Project: missing ')'\n");
		(*pos)++;
		skipWS(r, pos);

		if (!(exps = read_exps(sql, nrel, NULL, NULL, r, pos, '[', 0, 1)))
			return NULL;
		rel = rel_project(sql->sa, nrel, exps);
		set_processed(rel);
		/* order by ? */
		/* first projected expressions, then left relation projections */
		if (r[*pos] == '[' && !(rel->r = read_exps(sql, rel, nrel, NULL, r, pos, '[', 0, 1)))
			return NULL;
		break;
	case 'g':
		*pos += (int) strlen("group by");
		skipWS(r, pos);

		if (r[*pos] != '(')
			return sql_error(sql, -1, SQLSTATE(42000) "Group by: missing '('\n");
		(*pos)++;
		skipWS(r, pos);
		if (!(nrel = rel_read(sql, r, pos, refs)))
			return NULL;
		skipWS(r, pos);
		if (r[*pos] != ')')
			return sql_error(sql, -1, SQLSTATE(42000) "Group by: missing ')'\n");
		(*pos)++;
		skipWS(r, pos);

		if (!(gexps = read_exps(sql, nrel, NULL, NULL, r, pos, '[', 0, 1)))
			return NULL;
		skipWS(r, pos);
		rel = rel_groupby(sql, nrel, gexps);
		rel->exps = new_exp_list(sql->sa); /* empty projection list for now */
		set_processed(rel); /* don't search beyond the group by */
		/* first group projected expressions, then group by columns, then left relation projections */
		if (!(exps = read_exps(sql, rel, nrel, NULL, r, pos, '[', 1, 1)))
			return NULL;
		rel->exps = exps;
		rel->nrcols = list_length(exps);
		break;
	case 's':
	case 'a':
		if (r[*pos+1] == 'a') {
			*pos += (int) strlen("sample");
			skipWS(r, pos);
			if (r[*pos] != '(')
				return sql_error(sql, -1, SQLSTATE(42000) "Sample: missing '('\n");
			(*pos)++;
			skipWS(r, pos);
			if (!(nrel = rel_read(sql, r, pos, refs)))
				return NULL;
			if (r[*pos] != ')')
				return sql_error(sql, -1, SQLSTATE(42000) "Sample: missing ')'\n");
			(*pos)++;
			skipWS(r, pos);
			if (!(exps = read_exps(sql, nrel, NULL, NULL, r, pos, '[', 0, 1)))
				return NULL;
			rel = rel_sample(sql->sa, nrel, exps);
			set_processed(rel);
		} else if (r[*pos+2] == 'l') {
			*pos += (int) strlen("select");
			skipWS(r, pos);
			if (r[*pos] != '(')
				return sql_error(sql, -1, SQLSTATE(42000) "Select: missing '('\n");
			(*pos)++;
			skipWS(r, pos);
			if (!(nrel = rel_read(sql, r, pos, refs)))
				return NULL;
			skipWS(r, pos);
			if (r[*pos] != ')')
				return sql_error(sql, -1, SQLSTATE(42000) "Select: missing ')'\n");
			(*pos)++;
			skipWS(r, pos);

			if (!(exps = read_exps(sql, nrel, NULL, NULL, r, pos, '[', 0, 1)))
				return NULL;
			rel = rel_select_copy(sql->sa, nrel, exps);
			set_processed(rel);
			/* semijoin or antijoin */
		} else if (r[*pos+1] == 'e' || r[*pos+1] == 'n') {
			if (r[*pos+1] == 'n') {
				j = op_anti;
				*pos += (int) strlen("antijoin");
			} else {
				j = op_semi;
				*pos += (int) strlen("semijoin");
			}

			skipWS(r, pos);
			if (r[*pos] != '(')
				return sql_error(sql, -1, SQLSTATE(42000) "%s: missing '('\n", (j == op_semi)?"Semijoin":"Antijoin");
			(*pos)++;
			skipWS(r, pos);
			if (!(lrel = rel_read(sql, r, pos, refs)))
				return NULL;
			skipWS(r, pos);

			if (r[*pos] != ',')
				return sql_error(sql, -1, SQLSTATE(42000) "%s: missing ','\n", (j == op_semi)?"Semijoin":"Antijoin");
			(*pos)++;
			skipWS(r, pos);
			if (!(rrel = rel_read(sql, r, pos, refs)))
				return NULL;

			skipWS(r, pos);
			if (r[*pos] != ')')
				return sql_error(sql, -1, SQLSTATE(42000) "%s: missing ')'\n", (j == op_semi)?"Semijoin":"Antijoin");
			(*pos)++;
			skipWS(r, pos);

			if (!(exps = read_exps(sql, lrel, rrel, NULL, r, pos, '[', 0, 1)))
				return NULL;
			rel = rel_crossproduct(sql->sa, lrel, rrel, j);
			rel->exps = exps;
			set_processed(rel);
		}
		break;
	case 'l':
		*pos += (int) strlen("left outer join");
		j = op_left;
		/* fall through */
	case 'r':
		if (j == op_basetable) {
			*pos += (int) strlen("right outer join");
			j = op_right;
		}
		/* fall through */
	case 'f':
		if (j == op_basetable) {
			*pos += (int) strlen("full outer join");
			j = op_full;
		}
		/* fall through */
	case 'c':
		if (j == op_basetable) {
			*pos += (int) strlen("crossproduct");
			j = op_join;
		}
		/* fall through */
	case 'j':
		if (j == op_basetable) {
			*pos += (int) strlen("join");
			j = op_join;
		}
		skipWS(r, pos);

		if (r[*pos] != '(')
			return sql_error(sql, -1, SQLSTATE(42000) "Join: missing '('\n");
		(*pos)++;
		skipWS(r, pos);
		if (!(lrel = rel_read(sql, r, pos, refs)))
			return NULL;
		skipWS(r, pos);

		if (r[*pos] != ',')
			return sql_error(sql, -1, SQLSTATE(42000) "Join: missing ','\n");
		(*pos)++;
		skipWS(r, pos);
		if (!(rrel = rel_read(sql, r, pos, refs)))
			return NULL;

		skipWS(r, pos);
		if (r[*pos] != ')')
			return sql_error(sql, -1, SQLSTATE(42000) "Join: missing ')'\n");
		(*pos)++;
		skipWS(r, pos);

		if (!(exps = read_exps(sql, lrel, rrel, NULL, r, pos, '[', 0, 1)))
			return NULL;
		rel = rel_crossproduct(sql->sa, lrel, rrel, j);
		rel->exps = exps;
		set_processed(rel);
		break;
	case 'u':
		if (j == op_basetable) {
			*pos += (int) strlen("union");
			j = op_union;
		}
		/* fall through */
	case 'i':
		if (j == op_basetable) {
			*pos += (int) strlen("intersect");
			j = op_inter;
		}
		/* fall through */
	case 'e':
		if (j == op_basetable) {
			*pos += (int) strlen("except");
			j = op_except;
		}
		skipWS(r, pos);

		if (r[*pos] != '(')
			return sql_error(sql, -1, SQLSTATE(42000) "Setop: missing '('\n");
		(*pos)++;
		skipWS(r, pos);
		if (!(lrel = rel_read(sql, r, pos, refs)))
			return NULL;
		skipWS(r, pos);

		if (r[*pos] != ',')
			return sql_error(sql, -1, SQLSTATE(42000) "Setop: missing ','\n");
		(*pos)++;
		skipWS(r, pos);
		if (!(rrel = rel_read(sql, r, pos, refs)))
			return NULL;

		skipWS(r, pos);
		if (r[*pos] != ')')
			return sql_error(sql, -1, SQLSTATE(42000) "Setop: missing ')'\n");
		(*pos)++;
		skipWS(r, pos);

		if (!(exps = read_exps(sql, lrel, rrel, NULL, r, pos, '[', 0, 1)))
			return NULL;
		rel = rel_setop(sql->sa, lrel, rrel, j);
		rel_setop_set_exps(sql, rel, exps, false);
		if (rel_set_types(sql, rel) < 0)
			return sql_error(sql, -1, SQLSTATE(42000) "Setop: number of expressions don't match\n");
		set_processed(rel);
		break;
	case '[': /* projection of list of values */
		if (!(exps = read_exps(sql, NULL, NULL, NULL, r, pos, '[', 0, 1)))
			return NULL;
		rel = rel_project(sql->sa, NULL, exps);
		/* order by ? */
		if (r[*pos] == '[' && !(rel->r = read_exps(sql, NULL, rel, NULL, r, pos, '[', 0, 1)))
			return NULL;
		set_processed(rel);
		break;
	case 'd':
		/* 'ddl' not supported */
	default:
		return sql_error(sql, -1, SQLSTATE(42000) "Could not determine the input relation\n");
	}

	if (!rel)
		return sql_error(sql, -1, SQLSTATE(42000) "Could not determine the input relation\n");
	if (distinct)
		set_distinct(rel);
	if (single)
		set_single(rel);
	if (dependent)
		set_dependent(rel);

	/* sometimes, properties are sent */
	if (!(rel = read_rel_properties(sql, rel, r, pos)))
		return NULL;
	return rel;
}
