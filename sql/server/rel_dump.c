/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#define LINESIZE 160
#define TABSTOP 2

#include "rel_dump.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_remote.h"
#include "mal_errors.h"		/* for SQLSTATE() */

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
	char *r;

	(void)sql;
	switch(cmp) {
	case cmp_gt: 		r = ">"; break;
	case cmp_gte: 		r = ">="; break;
	case cmp_lte: 		r = "<="; break;
	case cmp_lt: 		r = "<"; break;
	case cmp_equal: 	r = "="; break;
	case cmp_notequal: 	r = "!="; break;
	case cmp_all: 		r = "all"; break;
	case cmp_or: 		r = "or"; break;
	case cmp_in: 		r = "in"; break;
	case cmp_notin: 	r = "notin"; break;
	case cmp_filter: 	r = "filter"; break;
	default:
		r = "";
	}
	mnstr_printf(fout, " %s ", r);
}

static void exps_print(mvc *sql, stream *fout, list *exps, int depth, list *refs, int alias, int brackets);

static void
exp_print(mvc *sql, stream *fout, sql_exp *e, int depth, list *refs, int comma, int alias) 
{
	(void)sql;
	if (!e)
		return;
	//mnstr_printf(fout, "%p ", e);
	switch(e->type) {
	case e_psm: {
		if (e->flag & PSM_SET) {
			/* todo */
		} else if (e->flag & PSM_VAR) {
			/* todo */
		} else if (e->flag & PSM_RETURN) {
			mnstr_printf(fout, "return ");
			exp_print(sql, fout, e->l, depth, refs, 0, 0);
		} else if (e->flag & PSM_WHILE) {
			mnstr_printf(fout, "while ");
			exp_print(sql, fout, e->l, depth, refs, 0, 0);
			exps_print(sql, fout, e->r, depth, refs, alias, 0);
		} else if (e->flag & PSM_IF) {
			mnstr_printf(fout, "if ");
			exp_print(sql, fout, e->l, depth, refs, 0, 0);
			exps_print(sql, fout, e->r, depth, refs, alias, 0);
			if (e->f)
				exps_print(sql, fout, e->f, depth, refs, alias, 0);
		} else if (e->flag & PSM_REL) {
			rel_print_(sql, fout, e->l, depth+1, refs, 1);
		}
	 	break;
	}
	case e_convert: {
		char *to_type = sql_subtype_string(&e->tpe);
		mnstr_printf(fout, "%s[", to_type);
		exp_print(sql, fout, e->l, depth, refs, 0, 0);
		mnstr_printf(fout, "]");
		_DELETE(to_type);
	 	break;
	}
	case e_atom: {
		if (e->l) {
			atom *a = e->l;
			if (atom_type(a)->type->localtype == TYPE_ptr) {
				sql_table *t = a->data.val.pval;
				mnstr_printf(fout, "%s(%s)", 
					isStream(t)?"stream":
					isMergeTable(t)?"merge table":
					isReplicaTable(t)?"replica table":"table",
					t->base.name);
			} else {
				char *t = sql_subtype_string(atom_type(a));
				char *s = atom2string(sql->sa, a);
				mnstr_printf(fout, "%s \"%s\"", t, s);
				_DELETE(t);
			}
		} else { /* variables */
			if (e->r) { /* named parameters */
				char *name = e->r;
				mnstr_printf(fout, "%s", name);
			} else if (e->f) {	/* values list */
				list *l = e->f;
				exps_print(sql, fout, l, depth, refs, 0, 0);
			} else { /* numbered arguments */
				mnstr_printf(fout, "A%d", e->flag);
			}
		}
	} 	break;
	case e_func: {
		sql_subfunc *f = e->f;
		mnstr_printf(fout, "%s.%s", 
				f->func->s?f->func->s->base.name:"sys", 
				f->func->base.name);
		exps_print(sql, fout, e->l, depth, refs, alias, 1);
		if (e->r)
			exps_print(sql, fout, e->r, depth, refs, alias, 1);
	} 	break;
	case e_aggr: {
		sql_subaggr *a = e->f;
		mnstr_printf(fout, "%s.%s", 
				a->aggr->s?a->aggr->s->base.name:"sys", 
				a->aggr->base.name);
		if (need_distinct(e))
			mnstr_printf(fout, " unique ");
		if (need_no_nil(e))
			mnstr_printf(fout, " no nil ");
		if (zero_if_empty(e))
			mnstr_printf(fout, " zero if empty ");
		if (e->l)
			exps_print(sql, fout, e->l, depth, refs, alias, 1);
		else
			mnstr_printf(fout, "()");
	} 	break;
	case e_column: 
		if (e->l)
			mnstr_printf(fout, "\"%s\".", (char*)e->l);
		mnstr_printf(fout, "\"%s\"", (char*)e->r);
		if (e->rname && e->name && e->l && e->r &&
			strcmp(e->rname, e->l) == 0 &&
			strcmp(e->name, e->r) == 0) 
			alias = 0;
		if (!e->rname && e->name && strcmp(e->name, e->r)==0)
			alias = 0;
	 	break;
	case e_cmp: 
		if (e->flag == cmp_in || e->flag == cmp_notin) {
			exp_print(sql, fout, e->l, depth, refs, 0, alias);
			cmp_print(sql, fout, get_cmp(e));
			exps_print(sql, fout, e->r, depth, refs, alias, 1);
		} else if (get_cmp(e) == cmp_or) {
			exps_print(sql, fout, e->l, depth, refs, alias, 1);
			cmp_print(sql, fout, get_cmp(e));
			exps_print(sql, fout, e->r, depth, refs, alias, 1);
		} else if (get_cmp(e) == cmp_filter) {
			sql_subfunc *f = e->f;

			exps_print(sql, fout, e->l, depth, refs, alias, 1);
			if (is_anti(e))
				mnstr_printf(fout, " !");
			mnstr_printf(fout, " FILTER %s ", f->func->base.name);
			exps_print(sql, fout, e->r, depth, refs, alias, 1);
		} else if (e->f) {
			exp_print(sql, fout, e->r, depth+1, refs, 0, 0);
			if (is_anti(e))
				mnstr_printf(fout, " ! ");
			cmp_print(sql, fout, swap_compare(range2lcompare(e->flag)) );
			exp_print(sql, fout, e->l, depth+1, refs, 0, 0);
			if (is_anti(e))
				mnstr_printf(fout, " ! ");
			cmp_print(sql, fout, range2rcompare(e->flag) );
			exp_print(sql, fout, e->f, depth+1, refs, 0, 0);
			if (e->flag & CMP_SYMMETRIC)
				mnstr_printf(fout, " SYM ");
		} else {
			exp_print(sql, fout, e->l, depth+1, refs, 0, 0);
			if (is_anti(e))
				mnstr_printf(fout, " ! ");
			cmp_print(sql, fout, get_cmp(e));

			exp_print(sql, fout, e->r, depth+1, refs, 0, 0);
		}
	 	break;
	default:
		;
	}
	if (e->type != e_atom && is_ascending(e))
		mnstr_printf(fout, " ASC");
	if (e->type != e_atom && e->type != e_cmp && !has_nil(e))
		mnstr_printf(fout, " NOT NULL");
	if (e->p) {
		prop *p = e->p;
		char *pv;

		for (; p; p = p->p) {
			pv = propvalue2string(p);
			mnstr_printf(fout, " %s %s", propkind2string(p), pv);
			GDKfree(pv);
		}
	}
	if (e->name && alias) {
		mnstr_printf(fout, " as ");
		if (e->rname)
			mnstr_printf(fout, "\"%s\".", e->rname);
		mnstr_printf(fout, "\"%s\"", e->name);
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

const char *
op2string(operator_type op) 
{
	switch (op) {
	case op_basetable:
		return "basetable";
	case op_table:
		return "table";
	case op_ddl:
		return "ddl";
	case op_project:
		return "project";
	case op_select: 
		return "select";
	case op_apply: 
		return "apply";
	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 
		return "join";
	case op_semi: 
		return "semi";
	case op_anti: 
		return "anti";
	case op_union: 
	case op_inter: 
	case op_except: 
		return "set op";
	case op_groupby: 
		return "group by";
	case op_topn: 
		return "topn";
	case op_sample:
		return "sample";
	case op_insert: 
	case op_update: 
	case op_delete:
	case op_truncate:
		return "modify op";
	default:
		return "unknown";
	}
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

void
rel_print_(mvc *sql, stream  *fout, sql_rel *rel, int depth, list *refs, int decorate) 
{ 
	char *r = NULL;

	if (!rel)
		return;

	if (rel_is_ref(rel)) {
		int nr = list_length(refs) + 1;
		int cnt = rel->ref.refcnt;
		mnstr_printf(fout, "\n%cREF %d (%d)", decorate?'=':' ', nr, cnt);
	}


	switch (rel->op) {
	case op_basetable: {
		sql_table *t = rel->l;
		sql_column *c = rel->r;
		print_indent(sql, fout, depth, decorate);

		if (!t && c) {
			mnstr_printf(fout, "dict(%s.%s)", c->t->base.name, c->base.name);
		} else {
			const char *sname = t->s?t->s->base.name:NULL;
			const char *tname = t->base.name;

			if (isRemote(t)) {
				const char *uri = t->query;

				sname = mapiuri_schema( uri, sql->sa, sname);
				tname = mapiuri_table( uri, sql->sa, tname);
			}
			if (sname)
				mnstr_printf(fout, "%s(%s.%s)", 
					isStream(t)?"stream":
					isRemote(t)&&decorate?"REMOTE":
					isReplicaTable(t)?"REPLICA":"table",
					sname, tname);
			else
		  		mnstr_printf(fout, "%s(%s)", 
					isStream(t)?"stream":
					isRemote(t)&&decorate?"REMOTE":
					isReplicaTable(t)?"REPLICA":"table",
					tname);
		}	
		if (rel->exps) 
			exps_print(sql, fout, rel->exps, depth, refs, 1, 0);
	} 	break;
	case op_table:
		print_indent(sql, fout, depth, decorate);
		mnstr_printf(fout, "table ");

		if (rel->r)
			exp_print(sql, fout, rel->r, depth, refs, 1, 0);
		if (rel->l)
			rel_print_(sql, fout, rel->l, depth+1, refs, decorate);
		if (rel->exps) 
			exps_print(sql, fout, rel->exps, depth, refs, 1, 0);
		break;
	case op_ddl:
		print_indent(sql, fout, depth, decorate);
		mnstr_printf(fout, "ddl");
		if (rel->l)
			rel_print_(sql, fout, rel->l, depth+1, refs, decorate);
		if (rel->r)
			rel_print_(sql, fout, rel->r, depth+1, refs, decorate);
		if (rel->exps && rel->flag == DDL_PSM) 
			exps_print(sql, fout, rel->exps, depth, refs, 1, 0);
		break;
	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 
	case op_apply: 
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
		else if (rel->op == op_apply) {
			r = "apply";
			if (rel->flag == APPLY_JOIN)
				r = "apply join";
			else if (rel->flag == APPLY_LOJ)
				r = "apply left outer join";
			else if (rel->flag == APPLY_EXISTS)
				r = "apply exists";
			else if (rel->flag == APPLY_NOTEXISTS)
				r = "apply not exists";
		}
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
		print_indent(sql, fout, depth, decorate);
		if (need_distinct(rel))
			mnstr_printf(fout, "distinct ");
		mnstr_printf(fout, "%s (", r);
		if (rel_is_ref(rel->l)) {
			int nr = find_ref(refs, rel->l);
			print_indent(sql, fout, depth+1, decorate);
			mnstr_printf(fout, "& REF %d ", nr);
		} else
			rel_print_(sql, fout, rel->l, depth+1, refs, decorate);
		mnstr_printf(fout, ",");
		if (rel_is_ref(rel->r)) {
			int nr = find_ref(refs, rel->r);
			print_indent(sql, fout, depth+1, decorate);
			mnstr_printf(fout, "& REF %d  ", nr);
		} else
			rel_print_(sql, fout, rel->r, depth+1, refs, decorate);
		print_indent(sql, fout, depth, decorate);
		mnstr_printf(fout, ")");
		exps_print(sql, fout, rel->exps, depth, refs, 1, 0);
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
		print_indent(sql, fout, depth, decorate);
		if (rel->l) {
			if (need_distinct(rel))
				mnstr_printf(fout, "distinct ");
			mnstr_printf(fout, "%s (", r);
			if (rel_is_ref(rel->l)) {
				int nr = find_ref(refs, rel->l);
				print_indent(sql, fout, depth+1, decorate);
				mnstr_printf(fout, "& REF %d ", nr);
			} else
				rel_print_(sql, fout, rel->l, depth+1, refs, decorate);
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
	case op_truncate: {

		print_indent(sql, fout, depth, decorate);
		if (rel->op == op_insert)
			mnstr_printf(fout, "insert(");
		else if (rel->op == op_update)
			mnstr_printf(fout, "update(");
		else if (rel->op == op_delete)
			mnstr_printf(fout, "delete(");
		else if (rel->op == op_truncate)
			mnstr_printf(fout, "truncate(");

		if (rel_is_ref(rel->l)) {
			int nr = find_ref(refs, rel->l);
			print_indent(sql, fout, depth+1, decorate);
			mnstr_printf(fout, "& REF %d ", nr);
		} else
			rel_print_(sql, fout, rel->l, depth+1, refs, decorate);

		if (rel->r) {
			if (rel_is_ref(rel->r)) {
				int nr = find_ref(refs, rel->r);
				print_indent(sql, fout, depth+1, decorate);
				mnstr_printf(fout, "& REF %d ", nr);
			} else
				rel_print_(sql, fout, rel->r, depth+1, refs, decorate);
		}
		print_indent(sql, fout, depth, decorate);
		mnstr_printf(fout, ")");
		if (rel->exps)
			exps_print(sql, fout, rel->exps, depth, refs, 1, 0);
	} 	break;
	default:
		assert(0);
	}
	if (rel->p) {
		prop *p = rel->p;
		char *pv;

		for (; p; p = p->p) {
			pv = propvalue2string(p);
			mnstr_printf(fout, " %s %s", propkind2string(p), pv);
			GDKfree(pv);
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
	case op_ddl:
		break;
	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 
	case op_apply: 
	case op_semi: 
	case op_anti: 
	case op_union: 
	case op_inter: 
	case op_except: 
		rel_print_refs(sql, fout, rel->l, depth, refs, decorate);
		rel_print_refs(sql, fout, rel->r, depth, refs, decorate);
		if (rel_is_ref(rel->l) && !find_ref(refs, rel->l)) {
			rel_print_(sql, fout, rel->l, depth, refs, decorate);
			list_append(refs, rel->l);
		}
		if (rel_is_ref(rel->r) && !find_ref(refs, rel->r)) {
			rel_print_(sql, fout, rel->r, depth, refs, decorate);
			list_append(refs, rel->r);
		}
		break;
	case op_project:
	case op_select: 
	case op_groupby: 
	case op_topn: 
	case op_sample: 
		rel_print_refs(sql, fout, rel->l, depth, refs, decorate);
		if (rel->l && rel_is_ref(rel->l) && !find_ref(refs, rel->l)) {
			rel_print_(sql, fout, rel->l, depth, refs, decorate);
			list_append(refs, rel->l);
		}
		break;
	case op_insert: 
	case op_update: 
	case op_delete:
	case op_truncate:
		rel_print_refs(sql, fout, rel->l, depth, refs, decorate);
		if (rel->l && rel_is_ref(rel->l) && !find_ref(refs, rel->l)) {
			rel_print_(sql, fout, rel->l, depth, refs, decorate);
			list_append(refs, rel->l);
		}
		rel_print_refs(sql, fout, rel->r, depth, refs, decorate);
		if (rel->r && rel_is_ref(rel->r) && !find_ref(refs, rel->r)) {
			rel_print_(sql, fout, rel->r, depth, refs, decorate);
			list_append(refs, rel->r);
		}
		break;
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
		while(r[*pos] && r[*pos] != '"')
			(*pos)++;
		(*pos)++;
	} else {
		while(r[*pos] && (isalnum((unsigned char) r[*pos]) || r[*pos] == '_' || r[*pos] == '%'))
			(*pos)++;
	}
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

static char *
readString( char *r, int *pos) 
{
	char *st = NULL;

	if (r[*pos] == '"'){
		(*pos)++;
		st = r+*pos;
		while (r[*pos] != '"')
			(*pos)++;
		r[*pos] = 0;
		(*pos)++;
	}
	return st;
}

static sql_exp* exp_read(mvc *sql, sql_rel *lrel, sql_rel *rrel, list *pexps, char *r, int *pos, int grp) ;


static void *
read_prop( mvc *sql, sql_exp *exp, char *r, int *pos) 
{
	/* PROPs */
	if (strncmp(r+*pos, "JOINIDX",  strlen("JOINIDX")) == 0) {
		int old;
		char *sname,*iname;
		sql_schema *s = NULL;
		prop *p;

		(*pos)+= (int) strlen("JOINIDX");
		skipWS(r, pos);
		/* schema.table.index */
		sname = r+*pos;
		skipIdent(r,pos);
		if (r[*pos] != '.') 
			return sql_error(sql, -1, SQLSTATE(42000) "JOINIDX: missing '.'\n");
		r[*pos] = 0;
		(*pos)++;
		skipIdent(r,pos);
		if (r[*pos] != '.') 
			return sql_error(sql, -1, SQLSTATE(42000) "JOINIDX: missing '.'\n");
		r[*pos] = 0;
		(*pos)++;
		iname = r+*pos;
		skipIdent(r,pos);
		old = r[*pos];
		r[*pos] = 0;
		
		s = mvc_bind_schema(sql, sname);
		if (!find_prop(exp->p, PROP_JOINIDX)) {
			p = exp->p = prop_create(sql->sa, PROP_JOINIDX, exp->p);
			p->value = mvc_bind_idx(sql, s, iname);
		}
		r[*pos] = old;
		skipWS(r,pos);
	}
	return exp->p;
}

static list*
read_exps(mvc *sql, sql_rel *lrel, sql_rel *rrel, list *pexps, char *r, int *pos, char bracket, int grp) 
{
	list *exps = new_exp_list(sql->sa);
	sql_exp *e;
	char ebracket = (bracket == '[')?']':')';

	if (r[*pos] == bracket) {
		skipWS( r, pos);

		(*pos)++;
		skipWS( r, pos);
		e = exp_read(sql, lrel, rrel, pexps, r, pos, grp);
		if (!e && r[*pos] != ebracket) {
			return sql_error(sql, -1, SQLSTATE(42000) "Missing closing %c\n", ebracket);
		} else if (!e) {
			(*pos)++;
			skipWS( r, pos);
			return exps;
		}
		append(exps, e);
		skipWS( r, pos);
		read_prop( sql, e, r, pos);
		while (r[*pos] == ',') {
			int op = 0;

			(*pos)++;
			skipWS( r, pos);
			op = *pos;
			e = exp_read(sql, lrel, rrel, exps, r, pos, grp);
			if (!e && pexps) {
				*pos = op;
				e = exp_read(sql, lrel, rrel, pexps, r, pos, grp);
			}
			if (!e)
				return NULL;
			append(exps, e);
			skipWS( r, pos);
			read_prop( sql, e, r, pos);
		}
		if (r[*pos] != ebracket) 
			return sql_error(sql, -1, SQLSTATE(42000) "Missing closing %c\n", ebracket);
		(*pos)++;
		skipWS( r, pos);
	}
	return exps;
}

static sql_exp*
exp_read(mvc *sql, sql_rel *lrel, sql_rel *rrel, list *pexps, char *r, int *pos, int grp) 
{
	int f = -1;
	int not = 1, old, d=0, s=0, unique = 0, no_nils = 0, quote = 0;
	char *tname, *cname = NULL, *e, *b = r + *pos, *st;
	sql_exp *exp = NULL;
	list *exps = NULL;
	sql_subtype *tpe;

	quote = (r[*pos] == '"');
	b += quote;
	skipIdent(r, pos);
	e = r+*pos-quote;
	skipWS(r, pos);
	switch(r[*pos]) {
	case '.': 
		*e = 0;
		(*pos)++;
		tname = b;
		cname = r + *pos + quote;
		skipIdentOrSymbol(r, pos);
		e = r+*pos - quote;
		old = *e;
		*e = 0;
		
		tname = sa_strdup(sql->sa, tname);
		cname = sa_strdup(sql->sa, cname);
		*e = old;
		skipWS(r, pos);
		if (pexps) {
			exp = exps_bind_column2(pexps, tname, cname);
			if (exp)
				exp = exp_alias_or_copy(sql, tname, cname, lrel, exp);
		}
		if (!exp && lrel) { 
			exp = rel_bind_column2(sql, lrel, tname, cname, 0);
			if (!exp && rrel)
				exp = rel_bind_column2(sql, rrel, tname, cname, 0);
		} else if (!exp) {
			exp = exp_column(sql->sa, tname, cname, NULL, CARD_ATOM, 1, (strchr(cname,'%') != NULL));
		}
		break;
	/* atom */
	case '(': 
		if (b == (r+*pos)) { /* or */
			int filter = 0, anti = 0;
			list *lexps,*rexps;
			char *fname = NULL;
		       
			lexps = read_exps(sql, lrel, rrel, pexps, r, pos, '(', 0);
			skipWS(r, pos);
			if (r[*pos] == '!') {
				anti = 1;
				(*pos)++;
				skipWS(r, pos);
			}
			if (strncmp(r+*pos, "or",  strlen("or")) == 0) {
				(*pos)+= (int) strlen("or");
			} else if (strncmp(r+*pos, "FILTER",  strlen("FILTER")) == 0) {
				(*pos)+= (int) strlen("FILTER");
				filter = 1;
			} else {
				return sql_error(sql, -1, SQLSTATE(42000) "Type: missing 'or'\n");
			}
			skipWS(r, pos);
			if (filter) {
				fname = r+*pos;

				skipIdent(r,pos);
				e = r+*pos;
				*e = 0;
				(*pos)++;
				skipWS(r,pos);
			}

			rexps = read_exps(sql, lrel, rrel, pexps, r, pos, '(', 0);
			if (filter) {
				sql_subfunc *func = sql_find_func(sql->sa, mvc_bind_schema(sql, "sys"), fname, 1+list_length(exps), F_FILT, NULL);
				if (!func)
					return sql_error(sql, -1, SQLSTATE(42000) "Filter: missing function '%s'\n", fname);
					
				return exp_filter(sql->sa, lexps, rexps, func, anti);
			}
			return exp_or(sql->sa, lexps, rexps, anti);
		}
		/* fall through */
	case '[': 
		old = *e;
		*e = 0;
		tname = b;
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
		tpe = sql_bind_subtype(sql->sa, tname, d, s);
		skipWS(r, pos);
		*e = old;
		if (r[*pos] == '[') { /* convert */
			(*pos)++;
			skipWS(r, pos);
			exp = exp_read(sql, lrel, rrel, pexps, r, pos, 0);
			if (!exp)
				return NULL;
			if (r[*pos] != ']') 
				return sql_error(sql, -1, SQLSTATE(42000) "Convert: missing ']'\n");
			(*pos)++;
			skipWS(r, pos);
			exp = exp_convert(sql->sa, exp, exp_subtype(exp), tpe);
		} else {
			st = readString(r,pos);
			if (st && strcmp(st, "NULL") == 0)
				exp = exp_atom(sql->sa, atom_general(sql->sa, tpe, NULL));
			else
				exp = exp_atom(sql->sa, atom_general(sql->sa, tpe, st));
			skipWS(r, pos);
		}
		break;
	case '\"': 
		*e = 0;
		tname = b;
		tpe = sql_bind_subtype(sql->sa, tname, 0, 0);
		st = readString(r,pos);
		if (st && strcmp(st, "NULL") == 0)
			exp = exp_atom(sql->sa, atom_general(sql->sa, tpe, NULL));
		else
			exp = exp_atom(sql->sa, atom_general(sql->sa, tpe, st));
		skipWS(r, pos);
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
	}
	if (r[*pos] == '(') {
		sql_schema *s;
		sql_subfunc *f = NULL;
		sql_subaggr *a = NULL;
		node *n;

		exps = read_exps(sql, lrel, rrel, pexps, r, pos, '(', 0);
		if (!exps)
			return NULL;
		tname = b;
		*e = 0;
		s = mvc_bind_schema(sql, tname);
		if (grp) {
			if (exps && exps->h)
				a = sql_bind_aggr(sql->sa, s, cname, exp_subtype(exps->h->data));
			else
				a = sql_bind_aggr(sql->sa, s, cname, NULL);
			exp = exp_aggr( sql->sa, exps, a, unique, no_nils, CARD_ATOM, 1);
		} else {
			list *ops = sa_list(sql->sa);
			for( n = exps->h; n; n = n->next)
				append(ops, exp_subtype(n->data));
			f = sql_bind_func_(sql->sa, s, cname, ops, F_FUNC);

			/* fix scale of mul function, other type casts are explicit */
			if (f && f->func->fix_scale == SCALE_MUL && list_length(exps) == 2) {
				sql_arg *ares = f->func->res->h->data;

        			if (strcmp(f->func->imp, "*") == 0 && ares->type.type->scale == SCALE_FIX) {
                			sql_subtype *res = f->res->h->data;
					sql_subtype *lt = ops->h->data;
					sql_subtype *rt = ops->h->next->data;

					res->digits = lt->digits;
                			res->scale = lt->scale + rt->scale;
				}
			}

			if (f)
				exp = exp_op( sql->sa, exps, f);
			else
				return sql_error(sql, -1, SQLSTATE(42000) "Function: missing '%s.%s %d'\n", tname, cname, list_length(ops));
		}
	}

	if (!exp && b != e) { /* simple ident */
		if (b[0] == 'A' && isdigit((unsigned char) b[1])) {
			char *e2;
			int nr = strtol(b+1,&e2,10);

			if (e == e2 && nr < sql->argc) {
				atom *a = sql->args[nr];

				exp = exp_atom_ref(sql->sa, nr, &a->tpe);
			}
			assert(exp);
		}
		if (!exp) {
			old = *e;
			*e = 0;
			if (stack_find_var(sql, b)) {
				sql_subtype *tpe = stack_find_type(sql, b);
				int frame = stack_find_frame(sql, b);
				exp = exp_param(sql->sa, sa_strdup(sql->sa, b), tpe, frame);
			}
			*e = old;
		}
		if (!exp && lrel) { 
			int amb = 0;
			char *cname;

			old = *e;
			*e = 0;
			cname = sa_strdup(sql->sa, b);
			if (pexps) {
				exp = exps_bind_column(pexps, cname, &amb);
				if (exp)
					exp = exp_alias_or_copy(sql, exp->rname, cname, lrel, exp);
			}
			(void)amb;
			assert(amb == 0);
			if (!exp && lrel)
				exp = rel_bind_column(sql, lrel, cname, 0);
			if (!exp && rrel)
				exp = rel_bind_column(sql, rrel, cname, 0);
			*e = old;
			skipWS(r,pos);
		}
	}
	if (!exp)
		return NULL;

	if (r[*pos] == '!') {
		(*pos)++;
		skipWS(r, pos);
		set_anti(exp);
	}
	/* [ COUNT ] */
	if (strncmp(r+*pos, "COUNT",  strlen("COUNT")) == 0) {
		(*pos)+= (int) strlen("COUNT");
		skipWS( r, pos);
	}
	/* [ ASC ] */
	if (strncmp(r+*pos, "ASC",  strlen("ASC")) == 0) {
		(*pos)+= (int) strlen("NOT");
		skipWS(r, pos);
		set_direction(exp, ASCENDING);
	}
	/* [ NOT ] NULL */
	if (strncmp(r+*pos, "NOT",  strlen("NOT")) == 0) {
		(*pos)+= (int) strlen("NOT");
		skipWS(r, pos);
		not = 1;
	}
	if (strncmp(r+*pos, "NULL",  strlen("NULL")) == 0) {
		(*pos)+= (int) strlen("NULL");
		skipWS(r, pos);
		if (not)
			set_has_no_nil(exp);
	}
	if (strncmp(r+*pos, "HASHIDX",  strlen("HASHIDX")) == 0) {
		(*pos)+= (int) strlen("HASHIDX");
		if (!find_prop(exp->p, PROP_HASHIDX))
			exp->p = prop_create(sql->sa, PROP_HASHIDX, exp->p);
		skipWS(r,pos);
	}
	if (strncmp(r+*pos, "HASHCOL",  strlen("HASHCOL")) == 0) {
		(*pos)+= (int) strlen("HASHCOL");
		if (!find_prop(exp->p, PROP_HASHCOL))
			exp->p = prop_create(sql->sa, PROP_HASHCOL, exp->p);
		skipWS(r,pos);
	}
	if (strncmp(r+*pos, "FETCH",  strlen("FETCH")) == 0) {
		(*pos)+= (int) strlen("FETCH");
		if (!find_prop(exp->p, PROP_FETCH))
			exp->p = prop_create(sql->sa, PROP_FETCH, exp->p);
		skipWS(r,pos);
	}
	read_prop( sql, exp, r, pos);
	skipWS(r,pos);

	/* as alias */
	if (strncmp(r+*pos, "as", 2) == 0) {
		(*pos)+=2;
		skipWS(r, pos);

		tname = r+*pos+1;
		skipIdent(r, pos);
		if (r[*pos] != '.') {
			r[*pos-1] = 0;
			cname = tname;
			exp_setname(sql->sa, exp, NULL, cname);
			skipWS(r, pos);
		} else {
			r[*pos-1] = 0;
			(*pos)++;
			cname = r+*pos+1;
			skipIdent(r, pos);
			e = r+*pos-1;
			skipWS(r, pos);
			*e = 0;
			exp_setname(sql->sa, exp, tname, cname);
		}
	}
	skipWS(r, pos);
	switch(r[*pos]) {
	case 'n': 
		if (strncmp(r+*pos, "notin",  strlen("notin")) == 0) {
			(*pos)+= (int) strlen("notin");
			f = cmp_notin;
		}
		break;
	case 'F':
		if (strncmp(r+*pos, "FILTER",  strlen("FILTER")) == 0) {
			(*pos)+= (int) strlen("FILTER");
			f = cmp_filter;
		}
		break;
	case 'i': 
		if (strncmp(r+*pos, "in",  strlen("in")) == 0) {
			(*pos)+= (int) strlen("in");
			f = cmp_in;
		}
		break;
	case 'o': 
		if (strncmp(r+*pos, "or",  strlen("or")) == 0) {
			(*pos)+= (int) strlen("or");
			f = cmp_or;
		}
		break;
	case '=': 
		f = cmp_equal;
		(*pos)++;
		break;
	case '<': 
		f = cmp_lt;
		(*pos)++;
		if (r[(*pos)] == '=') {
			f = cmp_lte;
			(*pos)++;
		}
		break;
	case '>': 
		f = cmp_gt;
		(*pos)++;
		if (r[(*pos)] == '=') {
			f = cmp_gte;
			(*pos)++;
		}
		break;
	case ',': 
	case ']': 
	case ')': 
	default:
		return exp;
	}
	if (f >= 0) {
		skipWS(r,pos);
		if (f == cmp_in || f == cmp_notin) {
	        	list *exps;
		       
			exps = read_exps(sql, lrel, rrel, pexps, r, pos, '(', 0);
			if (f == cmp_in || f == cmp_notin)
				return exp_in(sql->sa, exp, exps, f);
		} else {
			sql_exp *e;

	        	e = exp_read(sql, lrel, rrel, pexps, r, pos, 0);
			if (e && e->type == e_cmp) {
				sql_exp *ne = exp_compare2(sql->sa, e->l, exp, e->r, compare2range(swap_compare((comp_type)f), e->flag));
				if (is_anti(exp))
					set_anti(ne);
				return ne;
			} else if (e)
				return exp_compare(sql->sa, exp, e, f);
		}
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

		e->tpe = *exp_subtype( n->data );
	}
	return 0;
}

sql_rel*
rel_read(mvc *sql, char *r, int *pos, list *refs)
{
	sql_rel *rel = NULL, *nrel, *lrel, *rrel;
	list *exps, *gexps;
	int distinct = 0;
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
		rel = rel_read(sql, r, pos, refs);
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
	if (r[*pos] == 'd') {
		*pos += (int) strlen("distinct");
		skipWS(r, pos);
		distinct = 1;
	}
	switch(r[*pos]) {
	case 't': 
		if (r[*pos+1] == 'a') {
			sql_schema *s = NULL;
			sql_table *t = NULL;
			char *sname, *tname, *e;
			*pos += (int) strlen("table");
			skipWS(r, pos);
			if (r[*pos] != '(') 
				return sql_error(sql, -1, SQLSTATE(42000) "Table: missing '('\n");
			(*pos)++;
			skipWS(r, pos);
			sname = r+*pos;
			skipIdent(r, pos);
			e = r+*pos;
			if (r[*pos] != '.') 
				return sql_error(sql, -1, SQLSTATE(42000) "Table: missing '.' in table name\n");
			*e = 0;
			(*pos)++;
			tname = r+*pos;
			skipIdent(r, pos);
			e = r+*pos;
			skipWS(r, pos);
			if (r[*pos] != ')') 
				sql_error(sql, -1, SQLSTATE(42000) "Table: missing ')'\n");
			*e = 0;
			(*pos)++;
			skipWS(r, pos);
			s = mvc_bind_schema(sql, sname);
			if (s)
				t = mvc_bind_table(sql, s, tname);
			if (!s || !t)
				return sql_error(sql, -1, SQLSTATE(42000) "Table: missing '%s.%s'\n", sname, tname);
			rel = rel_basetable(sql, t, tname);
		
			if (!r[*pos])
				return rel;
	
			/* scan aliases */
			exps = read_exps(sql, rel, NULL, NULL, r, pos, '[', 0);
			if (exps && list_length(exps))
				rel->exps = exps;
			if (strncmp(r+*pos, "COUNT",  strlen("COUNT")) == 0) {
				(*pos)+= (int) strlen("COUNT");
				skipWS( r, pos);
			}
		} else { /* top N */
			*pos += (int) strlen("top N");
			skipWS(r, pos);
			if (r[*pos] != '(') 
				return sql_error(sql, -1, SQLSTATE(42000) "Top N: missing '('\n");
			(*pos)++;
			skipWS(r, pos);
			nrel = rel_read(sql, r, pos, refs);
			if (r[*pos] != ')') 
				return sql_error(sql, -1, SQLSTATE(42000) "Top N: missing ')'\n");
			(*pos)++;
			skipWS(r, pos);
			exps = read_exps(sql, nrel, NULL, NULL, r, pos, '[', 0);
			rel = rel_topn(sql->sa, nrel, exps);
		}
		break;
	case 'p':
		*pos += (int) strlen("project");
		skipWS(r, pos);

		if (r[*pos] != '(') 
			return sql_error(sql, -1, SQLSTATE(42000) "Project: missing '('\n");
		(*pos)++;
		skipWS(r, pos);
		nrel = rel_read(sql, r, pos, refs);
		skipWS(r, pos);
		if (r[*pos] != ')') 
			return sql_error(sql, -1, SQLSTATE(42000) "Project: missing ')'\n");
		(*pos)++;
		skipWS(r, pos);

		exps = read_exps(sql, nrel, NULL, NULL, r, pos, '[', 0);
		rel = rel_project(sql->sa, nrel, exps);
		/* order by ? */
		if (r[*pos] == '[') 
			rel->r = read_exps(sql, nrel, rel, NULL, r, pos, '[', 0);
		if (distinct)
			set_distinct(rel);
		distinct = 0;
		break;
	case 'g':
		*pos += (int) strlen("group by");
		skipWS(r, pos);

		if (r[*pos] != '(') 
			return sql_error(sql, -1, SQLSTATE(42000) "Group by: missing '('\n");
		(*pos)++;
		skipWS(r, pos);
		nrel = rel_read(sql, r, pos, refs);
		skipWS(r, pos);
		if (r[*pos] != ')') 
			return sql_error(sql, -1, SQLSTATE(42000) "Group by: missing ')'\n");
		(*pos)++;
		skipWS(r, pos);

		gexps = read_exps(sql, nrel, NULL, NULL, r, pos, '[', 0);
		skipWS(r, pos);
		exps = read_exps(sql, nrel, NULL, gexps, r, pos, '[', 1);
		if (!exps)
			return NULL;

		rel = rel_groupby(sql, nrel, gexps);
		rel->exps = exps;
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
			nrel = rel_read(sql, r, pos, refs);
			if (r[*pos] != ')') 
				return sql_error(sql, -1, SQLSTATE(42000) "Sample: missing ')'\n");
			(*pos)++;
			skipWS(r, pos);
			exps = read_exps(sql, nrel, NULL, NULL, r, pos, '[', 0);
			rel = rel_sample(sql->sa, nrel, exps);
		} else if (r[*pos+2] == 'l') {
			*pos += (int) strlen("select");
			skipWS(r, pos);
			if (r[*pos] != '(') 
				return sql_error(sql, -1, SQLSTATE(42000) "Select: missing '('\n");
			(*pos)++;
			skipWS(r, pos);
			nrel = rel_read(sql, r, pos, refs);
			skipWS(r, pos);
			if (r[*pos] != ')') 
				return sql_error(sql, -1, SQLSTATE(42000) "Select: missing ')'\n");
			(*pos)++;
			skipWS(r, pos);

			exps = read_exps(sql, nrel, NULL, NULL, r, pos, '[', 0);
			rel = rel_select_copy(sql->sa, nrel, exps);
			/* semijoin or antijoin */
		} else if (r[*pos+1] == 'e' || r[*pos+1] == 'n') {
			j = op_semi;

			if (r[*pos+1] == 'n') 
				j = op_anti;

			*pos += (int) strlen("semijoin");
			skipWS(r, pos);
			if (r[*pos] != '(') 
				return sql_error(sql, -1, SQLSTATE(42000) "Semijoin: missing '('\n");
			(*pos)++;
			skipWS(r, pos);
			lrel = rel_read(sql, r, pos, refs);
			skipWS(r, pos);

			if (r[*pos] != ',') 
				return sql_error(sql, -1, SQLSTATE(42000) "Semijoin: missing ','\n");
			(*pos)++;
			skipWS(r, pos);
			rrel = rel_read(sql, r, pos, refs);

			skipWS(r, pos);
			if (r[*pos] != ')') 
				return sql_error(sql, -1, SQLSTATE(42000) "Semijoin: missing ')'\n");
			(*pos)++;
			skipWS(r, pos);

			exps = read_exps(sql, lrel, rrel, NULL, r, pos, '[', 0);
			rel = rel_crossproduct(sql->sa, lrel, rrel, j);
			rel->exps = exps;
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
		lrel = rel_read(sql, r, pos, refs);
		skipWS(r, pos);

		if (r[*pos] != ',') 
			return sql_error(sql, -1, SQLSTATE(42000) "Join: missing ','\n");
		(*pos)++;
		skipWS(r, pos);
		rrel = rel_read(sql, r, pos, refs);

		skipWS(r, pos);
		if (r[*pos] != ')') 
			return sql_error(sql, -1, SQLSTATE(42000) "Join: missing ')'\n");
		(*pos)++;
		skipWS(r, pos);

		exps = read_exps(sql, lrel, rrel, NULL, r, pos, '[', 0);
		rel = rel_crossproduct(sql->sa, lrel, rrel, j);
		rel->exps = exps;
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
		lrel = rel_read(sql, r, pos, refs);
		skipWS(r, pos);

		if (r[*pos] != ',') 
			return sql_error(sql, -1, SQLSTATE(42000) "Setop: missing ','\n");
		(*pos)++;
		skipWS(r, pos);
		rrel = rel_read(sql, r, pos, refs);

		skipWS(r, pos);
		if (r[*pos] != ')') 
			return sql_error(sql, -1, SQLSTATE(42000) "Setop: missing ')'\n");
		(*pos)++;
		skipWS(r, pos);

		exps = read_exps(sql, NULL, NULL, NULL, r, pos, '[', 0);
		rel = rel_setop(sql->sa, lrel, rrel, j);
		if (!exps)
			return NULL;
		rel->exps = exps;
		if (rel_set_types(sql, rel) < 0)
			return NULL;
		set_processed(rel);
		break;
	case 'd':
		/* 'ddl' not supported */
	default:
		return NULL;
	}
	/* sometimes the properties are send */
	while (strncmp(r+*pos, "REMOTE",  strlen("REMOTE")) == 0) {
		(*pos)+= (int) strlen("REMOTE");
		skipWS(r, pos);
		skipUntilWS(r, pos);
		skipWS(r, pos);
	}
	return rel;
}

