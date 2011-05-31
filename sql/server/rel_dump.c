/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#define LINESIZE 160
#define TABSTOP 2

#include "rel_dump.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_select.h"

static void
print_indent(mvc *sql, stream *fout, int depth)
{
	char buf[LINESIZE+1];
	int i;

	(void)sql;
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
	case cmp_notlike: 	r = "notlike"; break;
	case cmp_like: 		r = "like"; break;
	case cmp_notilike: 	r = "notilike"; break;
	case cmp_ilike: 	r = "ilike"; break;
	case cmp_all: 		r = "all"; break;
	case cmp_or: 		r = "or"; break;
	case cmp_in: 		r = "in"; break;
	case cmp_notin: 	r = "notin"; break;
	default:
		r = "";
	}
	mnstr_printf(fout, " %s ", r);
}

static void exps_print(mvc *sql, stream *fout, list *exps, int depth, int alias, int brackets);

static void
exp_print(mvc *sql, stream *fout, sql_exp *e, int depth, int comma, int alias) 
{
	(void)sql;
	if (!e)
		return;
	switch(e->type) {
	case e_convert: {
		//list *l = e->r;
		//sql_subtype *f = l->h->data;
		char *to_type = sql_subtype_string(&e->tpe);
		//char *from_type = sql_subtype_string(f);
		//mnstr_printf(fout, "convert(");
		mnstr_printf(fout, "%s[", to_type);
		exp_print(sql, fout, e->l, depth, 0, 0);
		//mnstr_printf(fout, " from %s to %s)", from_type, to_type);
		mnstr_printf(fout, "]");
		_DELETE(to_type);
		//_DELETE(from_type);
	 	break;
	}
	case e_atom: {
		if (e->l) {
			atom *a = e->l;
			if (atom_type(a)->type->localtype == TYPE_ptr) {
				sql_table *t = a->data.val.pval;
				mnstr_printf(fout, "%s(%s)", 
					isStream(t)?"stream":isMergeTable(t)?"merge table":"table",
					t->base.name);
			} else {
				char *t = sql_subtype_string(atom_type(a));
				char *s = atom2string(sql->sa, a);
				mnstr_printf(fout, "%s \"%s\"", t, s);
			}
		} else { /* variables */
			if (e->r) { /* named parameters */
				char *name = e->r;
				mnstr_printf(fout, "%s", name);
			} else if (e->f) {	/* values list */
				list *l = e->f;
				exp_print(sql, fout, l->h->data, depth, 0, 0);
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
		exps_print(sql, fout, e->l, depth, alias, 1);
		if (e->r)
			exps_print(sql, fout, e->r, depth, alias, 1);
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
		if (e->l)
			exps_print(sql, fout, e->l, depth, alias, 1);
		else
			mnstr_printf(fout, "()");
	} 	break;
	case e_column: 
		if (e->l)
			mnstr_printf(fout, "%s.", (char*)e->l);
		mnstr_printf(fout, "%s", (char*)e->r);
		if (e->rname && e->name && e->l && e->r &&
			strcmp(e->rname, e->l) == 0 &&
			strcmp(e->name, e->r) == 0) 
			alias = 0;
		if (!e->rname && e->name && strcmp(e->name, e->r)==0)
			alias = 0;
	 	break;
	case e_cmp: 
		if (e->flag == cmp_in || e->flag == cmp_notin) {
			exp_print(sql, fout, e->l, depth, alias, 1);
			cmp_print(sql, fout, e->flag );
			exps_print(sql, fout, e->r, depth, alias, 1);
		} else if (e->flag == cmp_or) {
			exps_print(sql, fout, e->l, depth, alias, 1);
			cmp_print(sql, fout, e->flag );
			exps_print(sql, fout, e->r, depth, alias, 1);
		} else if (e->f) {
			exp_print(sql, fout, e->r, depth+1, 0, 0);
			if (is_anti(e))
				mnstr_printf(fout, " ! ");
			cmp_print(sql, fout, swap_compare(range2lcompare(e->flag)) );
			exp_print(sql, fout, e->l, depth+1, 0, 0);
			if (is_anti(e))
				mnstr_printf(fout, " ! ");
			cmp_print(sql, fout, range2rcompare(e->flag) );
			exp_print(sql, fout, e->f, depth+1, 0, 0);
		} else {
			exp_print(sql, fout, e->l, depth+1, 0, 0);
			if (is_anti(e))
				mnstr_printf(fout, " ! ");
			cmp_print(sql, fout, e->flag );

			exp_print(sql, fout, e->r, depth+1, 0, 0);
		}
	 	break;
	default:
		;
	}
	if (e->type != e_atom && is_ascending(e))
		mnstr_printf(fout, " ASC");
	if (e->type != e_atom && e->type != e_cmp && !has_nil(e))
		mnstr_printf(fout, " NOT NULL");
	if (e->name && alias) {
		mnstr_printf(fout, " as ");
		if (e->rname)
			mnstr_printf(fout, "%s.", e->rname);
		mnstr_printf(fout, "%s", e->name);
	}
	if (e->p) {
		prop *p = e->p;

		for (; p; p = p->p)
			mnstr_printf(fout, " %s", propkind2string(p));
	}
	if (comma)
		mnstr_printf(fout, ", ");
}

static void
exps_print(mvc *sql, stream *fout, list *exps, int depth, int alias, int brackets) 
{
	node *en;

	if (!exps)
		return;

	if (brackets)
		mnstr_printf(fout, "(");
	else
		mnstr_printf(fout, " [ ");
	for (en = exps->h; en; en = en->next) 
		exp_print(sql, fout, en->data, depth+1, (en->next!=NULL), alias);
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
	case op_insert: 
	case op_update: 
	case op_delete: 
		return "modify op";
	}
	return "unknown";
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
rel_print_(mvc *sql, stream  *fout, sql_rel *rel, int depth, list *refs) 
{ 
	char *r = NULL;

	if (!rel)
		return;

	if (rel_is_ref(rel)) {
		int nr = list_length(refs) + 1;
		int cnt = rel->ref.refcnt;
		mnstr_printf(fout, "\n= REF %d (%d)", nr, cnt);
	}

	switch (rel->op) {
	case op_basetable: {
		sql_table *t = rel->l;
		print_indent(sql, fout, depth);
		if (t->s)
			mnstr_printf(fout, "%s(%s.%s)", 
				isStream(t)?"stream":"table",
				t->s->base.name, t->base.name);
		else
			mnstr_printf(fout, "%s(%s)", 
				isStream(t)?"stream":"table",
				t->base.name);
		if (rel->exps) 
			exps_print(sql, fout, rel->exps, depth, 1, 0);
	} 	break;
	case op_table:
		print_indent(sql, fout, depth);
		mnstr_printf(fout, "table ");
		if (rel->l)
			rel_print_(sql, fout, rel->l, depth+1, refs);
		if (rel->r)
			exp_print(sql, fout, rel->r, depth, 1, 0);
		if (rel->exps) 
			exps_print(sql, fout, rel->exps, depth, 1, 0);
		break;
	case op_ddl:
		print_indent(sql, fout, depth);
		mnstr_printf(fout, "ddl");
		if (rel->l)
			rel_print_(sql, fout, rel->l, depth+1, refs);
		if (rel->r)
			rel_print_(sql, fout, rel->r, depth+1, refs);
		if (rel->exps) 
			exps_print(sql, fout, rel->exps, depth, 1, 0);
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
		print_indent(sql, fout, depth);
		if (need_distinct(rel))
			mnstr_printf(fout, "distinct ");
		mnstr_printf(fout, "%s (", r);
		if (rel_is_ref(rel->l)) {
			int nr = find_ref(refs, rel->l);
			mnstr_printf(fout, " & REF %d ", nr);
		} else
			rel_print_(sql, fout, rel->l, depth+1, refs);
		mnstr_printf(fout, ",");
		if (rel_is_ref(rel->r)) {
			int nr = find_ref(refs, rel->r);
			mnstr_printf(fout, " & REF %d  ", nr);
		} else
			rel_print_(sql, fout, rel->r, depth+1, refs);
		print_indent(sql, fout, depth);
		mnstr_printf(fout, ")");
		exps_print(sql, fout, rel->exps, depth, 1, 0);
		break;
	case op_project:
	case op_select: 
	case op_groupby: 
	case op_topn: 
		r = "project";
		if (rel->op == op_select)
			r = "select";
		if (rel->op == op_groupby)
			r = "group by";
		if (rel->op == op_topn)
			r = "top N";
		print_indent(sql, fout, depth);
		if (rel->l) {
			if (need_distinct(rel))
				mnstr_printf(fout, "distinct ");
			if (need_including(rel))
				mnstr_printf(fout, "including ");
			mnstr_printf(fout, "%s (", r);
			if (rel_is_ref(rel->l)) {
				int nr = find_ref(refs, rel->l);
				mnstr_printf(fout, " & REF %d ", nr);
			} else
				rel_print_(sql, fout, rel->l, depth+1, refs);
			print_indent(sql, fout, depth);
			mnstr_printf(fout, ")");
		}
		if (rel->r && 
	           (rel->op == op_groupby || rel->op == op_project)) { 
		   /* group by and order by columns */
			if (rel->op == op_project)
				r = " order by ";
			exps_print(sql, fout, rel->r, depth, 1, 0);
		}
		exps_print(sql, fout, rel->exps, depth, 1, 0);
		break;
	case op_insert:
	case op_update:
	case op_delete: {

		print_indent(sql, fout, depth);
		if (rel->op == op_insert)
			mnstr_printf(fout, "insert(");
		else if (rel->op == op_update)
			mnstr_printf(fout, "update(");
		else if (rel->op == op_delete)
			mnstr_printf(fout, "delete(");

		if (rel_is_ref(rel->l)) {
			int nr = find_ref(refs, rel->l);
			mnstr_printf(fout, " & REF %d ", nr);
		} else
			rel_print_(sql, fout, rel->l, depth+1, refs);

		if (rel->r) {
			if (rel_is_ref(rel->r)) {
				int nr = find_ref(refs, rel->r);
				mnstr_printf(fout, " & REF %d ", nr);
			} else
				rel_print_(sql, fout, rel->r, depth+1, refs);
		}
		print_indent(sql, fout, depth);
		mnstr_printf(fout, ")");
		if (rel->exps)
			exps_print(sql, fout, rel->exps, depth, 1, 0);
	} 	break;
	default:
		assert(0);
	}
}

static void
rel_print_refs(mvc *sql, stream* fout, sql_rel *rel, int depth, list *refs) 
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
	case op_semi: 
	case op_anti: 
	case op_union: 
	case op_inter: 
	case op_except: 
		rel_print_refs(sql, fout, rel->l, depth, refs);
		rel_print_refs(sql, fout, rel->r, depth, refs);
		if (rel_is_ref(rel->l) && !find_ref(refs, rel->l)) {
			rel_print_(sql, fout, rel->l, depth, refs);
			list_append(refs, rel->l);
		}
		if (rel_is_ref(rel->r) && !find_ref(refs, rel->r)) {
			rel_print_(sql, fout, rel->r, depth, refs);
			list_append(refs, rel->r);
		}
		break;
	case op_project:
	case op_select: 
	case op_groupby: 
	case op_topn: 
		rel_print_refs(sql, fout, rel->l, depth, refs);
		if (rel->l && rel_is_ref(rel->l) && !find_ref(refs, rel->l)) {
			rel_print_(sql, fout, rel->l, depth, refs);
			list_append(refs, rel->l);
		}
		break;
	case op_insert: 
	case op_update: 
	case op_delete: 
		rel_print_refs(sql, fout, rel->l, depth, refs);
		if (rel->l && rel_is_ref(rel->l) && !find_ref(refs, rel->l)) {
			rel_print_(sql, fout, rel->l, depth, refs);
			list_append(refs, rel->l);
		}
		rel_print_refs(sql, fout, rel->r, depth, refs);
		if (rel->r && rel_is_ref(rel->r) && !find_ref(refs, rel->r)) {
			rel_print_(sql, fout, rel->r, depth, refs);
			list_append(refs, rel->r);
		}
		break;
	}
}

void
_rel_print(mvc *sql, sql_rel *rel) 
{
	list *refs = list_create(NULL);
	rel_print_refs(sql, THRdata[0], rel, 0, refs);
	rel_print_(sql, THRdata[0], rel, 0, refs);
	mnstr_printf(THRdata[0], "\n");
	list_destroy(refs);
}

void
rel_print(mvc *sql, sql_rel *rel, int depth) 
{
	list *refs = list_create(NULL);
	size_t pos;
	size_t nl = 0;
	size_t len = 0, lastpos = 0;
	stream *fd = sql->scanner.ws;
	stream *s;
	buffer *b = buffer_create(16364); /* hopefully enough */
	if (!b)
		return; /* signal somehow? */
	s = buffer_wastream(b, "SQL Plan");
	if (!s) {
		buffer_destroy(b);
		return; /* signal somehow? */
	}

	rel_print_refs(sql, s, rel, depth, refs);
	rel_print_(sql, s, rel, depth, refs);
	mnstr_printf(s, "\n");

	/* count the number of lines in the output, skip the leading \n */
	for (pos = 1; pos < b->pos; pos++) {
		if (b->buf[pos] == '\n') {
			nl++;
			if (len < pos - lastpos)
				len = pos - lastpos;
			lastpos = pos + 1;
		}
	}
	b->buf[b->pos - 1] = '\0';  /* should always end with a \n, can overwrite */

	/* craft a semi-professional header */
	mnstr_printf(fd, "&1 0 %d 1 %d\n", /* type id rows columns tuples */
			nl, nl);
	mnstr_printf(fd, "%% .plan # table_name\n");
	mnstr_printf(fd, "%% rel # name\n");
	mnstr_printf(fd, "%% clob # type\n");
	mnstr_printf(fd, "%% " SZFMT " # length\n", len - 1 /* remove = */);

	/* output the data */
	mnstr_printf(fd, "%s\n", b->buf + 1 /* omit starting \n */);

	mnstr_close(s);
	mnstr_destroy(s);
	buffer_destroy(b);

	list_destroy(refs);
}

static void
skipWS( char *r, int *pos)
{
	while(r[*pos] && (isspace(r[*pos]) || r[*pos] == '|')) 
		(*pos)++;
}

static void
skipIdent( char *r, int *pos)
{
	while(r[*pos] && (isalnum(r[*pos]) || r[*pos] == '_' || r[*pos] == '%'))
		(*pos)++;
}

static int
readInt( char *r, int *pos)
{
	int res = 0;

	while (isdigit(r[*pos])) {
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

static sql_exp* exp_read(mvc *sql, sql_rel *lrel, sql_rel *rrel, char *r, int *pos, int grp) ;

static list*
read_exps(mvc *sql, sql_rel *lrel, sql_rel *rrel, char *r, int *pos, char bracket, int grp) 
{
	list *exps = new_exp_list(sql->sa);
	sql_exp *e;
	char ebracket = (bracket == '[')?']':')';

	if (r[*pos] == bracket) {
		skipWS( r, pos);

		(*pos)++;
		skipWS( r, pos);
		e = exp_read(sql, lrel, rrel, r, pos, grp);
		if (!e && r[*pos] != ebracket) {
			return sql_error(sql, -1, "missing closing %c\n", ebracket);
		} else if (!e) {
			(*pos)++;
			skipWS( r, pos);
			return exps;
		}
		append(exps, e);
		skipWS( r, pos);
		while (r[*pos] == ',') {
			(*pos)++;
			skipWS( r, pos);
			e = exp_read(sql, lrel, rrel, r, pos, grp);
			if (!e)
				return NULL;
			append(exps, e);
			skipWS( r, pos);
		}
		if (r[*pos] != ebracket) 
			return sql_error(sql, -1, "missing closing %c\n", ebracket);
		(*pos)++;
		skipWS( r, pos);
	}
	return exps;
}

static sql_exp*
exp_read(mvc *sql, sql_rel *lrel, sql_rel *rrel, char *r, int *pos, int grp) 
{
	int not = 1, f = 0, old, d=0, s=0, unique = 0, no_nils = 0;
	char *tname, *cname, *e, *b = r + *pos, *st;
	sql_exp *exp = NULL;
	list *exps = NULL;
	sql_subtype *tpe;

	skipIdent(r, pos);
	e = r+*pos;
	skipWS(r, pos);
	switch(r[*pos]) {
	/* ident */
	case '.': 
		*e = 0;
		(*pos)++;
		tname = b;
		cname = r + *pos;
		skipIdent(r, pos);
		e = r+*pos;
		skipWS(r, pos);
		old = *e;
		*e = 0;
		
		if (lrel) { 
			exp = rel_bind_column2(sql, lrel, tname, cname, 0);
			if (!exp && rrel)
				exp = rel_bind_column2(sql, rrel, tname, cname, 0);
		} else {
			exp = exp_column(sql->sa, tname, cname, NULL, CARD_ATOM, 1, (strchr(cname,'%') != NULL));
		}
		*e = old;
		break;
	/* atom */
	case '(': 
	case '[': 
		old = *e;
		*e = 0;
		tname = b;
		if (old != '[') {
			(*pos)++;
			d = readInt(r,pos);
			if (r[*pos] != ')' && r[*pos] != ',')
				return sql_error(sql, -1, "type: missing ')' or ','\n");
			if (r[*pos] == ',') {
				(*pos)++;
				s = readInt(r,pos);
			}
			if (r[*pos] != ')') 
				return sql_error(sql, -1, "type: missing ')'\n");
			(*pos)++;
		}
		tpe = sql_bind_subtype(tname, d, s);
		skipWS(r, pos);
		*e = old;
		if (r[*pos] == '[') { /* convert */
			(*pos)++;
			skipWS(r, pos);
			exp = exp_read(sql, lrel, rrel, r, pos, 0);
			if (r[*pos] != ']') 
				return sql_error(sql, -1, "convert: missing ']'\n");
			(*pos)++;
			skipWS(r, pos);
			exp = exp_convert(sql->sa, exp, exp_subtype(exp), tpe);
		} else {
			st = readString(r,pos);
			exp = exp_atom(sql->sa, atom_general(sql->sa, tpe, st));
		}
		break;
	case '\"': 
		*e = 0;
		tname = b;
		tpe = sql_bind_subtype(tname, 0, 0);
		st = readString(r,pos);
		return exp_atom(sql->sa, atom_general(sql->sa, tpe, st));
	default:
		(void)sql;
	}

	/* func or aggr */
	if (grp) {
		skipWS(r, pos);
		if (r[*pos] == 'u') {
			unique = 1;
			(*pos)+= strlen("unique");
			skipWS(r, pos);
		}
		if (r[*pos] == 'n') {
			no_nils = 1;
			(*pos)+= strlen("no nil");
			skipWS(r, pos);
		}
	}
	if (r[*pos] == '(') {
		sql_schema *s;
		sql_subfunc *f = NULL;
		sql_subaggr *a = NULL;
		node *n;

		exps = read_exps(sql, lrel, rrel, r, pos, '(', 0);
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
			list *ops = list_new(sql->sa);
			for( n = exps->h; n; n = n->next)
				append(ops, exp_subtype(n->data));
			f = sql_bind_func_(sql->sa, s, cname, ops);
			exp = exp_op( sql->sa, exps, f);
		}
	}

	if (!exp && b != e) { /* simple ident */
		if (lrel) { 
			old = *e;
			*e = 0;
			exp = rel_bind_column(sql, lrel, b, 0);
			if (!exp && rrel)
				exp = rel_bind_column(sql, rrel, b, 0);
			*e = old;
		}
	}
	/* [ ASC ] */
	if (strncmp(r+*pos, "ASC",  strlen("ASC")) == 0) {
		(*pos)+= strlen("NOT");
		skipWS(r, pos);
		set_direction(exp, ASCENDING);
	}

	/* [ NOT ] NULL */
	if (strncmp(r+*pos, "NOT",  strlen("NOT")) == 0) {
		(*pos)+= strlen("NOT");
		skipWS(r, pos);
		not = 1;
	}
	if (strncmp(r+*pos, "NULL",  strlen("NULL")) == 0) {
		(*pos)+= strlen("NULL");
		skipWS(r, pos);
		if (not)
			set_has_no_nil(exp);
	}
	/* as alias */
	if (strncmp(r+*pos, "as", 2) == 0) {
		int old;
		(*pos)+=2;
		skipWS(r, pos);

		tname = r+*pos;
		skipIdent(r, pos);
		if (r[*pos] != '.') {
			old = r[*pos];
			r[*pos] = 0;
			cname = tname;
			exp_setname(sql->sa, exp, NULL, cname);
			r[*pos] = old;
			skipWS(r, pos);
		} else {
			r[*pos] = 0;
			(*pos)++;
			cname = r+*pos;
			skipIdent(r, pos);
			e = r+*pos;
			skipWS(r, pos);
			old = *e;
			*e = 0;
			exp_setname(sql->sa, exp, tname, cname);
			*e = old;
		}
	}
	switch(r[*pos]) {
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
		return exp;
	default:
		return NULL;
	}
	if (f) {
		sql_exp *e;

		skipWS(r,pos);
	        e = exp_read(sql, lrel, rrel, r, pos, 0);
		/* TODO case for compare 2 */
		if (e)
			return exp_compare(sql->sa, exp, e, f);
	}
	return exp;
}

sql_rel*
rel_read(mvc *sql, char *r, int *pos)
{
	sql_rel *rel = NULL, *nrel, *lrel, *rrel;
	list *exps, *gexps;
	int distinct = 0, j=0;

	skipWS(r,pos);
	switch(r[*pos]) {
	case 't': { 
		sql_schema *s = NULL;
		sql_table *t = NULL;
		char *sname, *tname, *e;
		*pos += strlen("table");
		skipWS(r, pos);
		if (r[*pos] != '(') 
			return sql_error(sql, -1, "table: missing '('\n");
		(*pos)++;
		skipWS(r, pos);
		sname = r+*pos;
		skipIdent(r, pos);
		e = r+*pos;
		if (r[*pos] != '.') 
			return sql_error(sql, -1, "table: missing '.' in table name\n");
		*e = 0;
		(*pos)++;
		tname = r+*pos;
		skipIdent(r, pos);
		e = r+*pos;
		skipWS(r, pos);
		if (r[*pos] != ')') 
			sql_error(sql, -1, "table: missing ')'\n");
		*e = 0;
		(*pos)++;
		skipWS(r, pos);
		s = mvc_bind_schema(sql, sname);
		if (s)
			t = mvc_bind_table(sql, s, tname);
		if (!s || !t)
			return sql_error(sql, -1, "table: missing '%s.%s'\n", sname, tname);
		rel = rel_basetable(sql, t, tname);
	
		if (!r[*pos])
			return rel;

		/* scan aliases */
		exps = read_exps(sql, rel, NULL, r, pos, '[', 0);
		if (exps && list_length(exps))
			rel = rel_project(sql->sa, rel, exps);
		return rel;
	} break;
	case 'p':
		*pos += strlen("project");
		skipWS(r, pos);

		if (r[*pos] != '(') 
			return sql_error(sql, -1, "project: missing '('\n");
		(*pos)++;
		skipWS(r, pos);
		nrel = rel_read(sql, r, pos);
		skipWS(r, pos);
		if (r[*pos] != ')') 
			return sql_error(sql, -1, "project: missing ')'\n");
		(*pos)++;
		skipWS(r, pos);

		exps = read_exps(sql, nrel, NULL, r, pos, '[', 0);
		rel = rel_project(sql->sa, nrel, exps);
		/* order by ? */
		if (r[*pos] == '[') {
			rel->r = rel->exps;
			rel->exps = read_exps(sql, nrel, NULL, r, pos, '[', 0);
		}
		if (distinct)
			set_distinct(rel);
		distinct = 0;
		return rel;
	case 'g':
		*pos += strlen("group by");
		skipWS(r, pos);

		if (r[*pos] != '(') 
			return sql_error(sql, -1, "group by: missing '('\n");
		(*pos)++;
		skipWS(r, pos);
		nrel = rel_read(sql, r, pos);
		skipWS(r, pos);
		if (r[*pos] != ')') 
			return sql_error(sql, -1, "group by: missing ')'\n");
		(*pos)++;
		skipWS(r, pos);

		gexps = read_exps(sql, nrel, NULL, r, pos, '[', 0);
		skipWS(r, pos);
		exps = read_exps(sql, nrel, NULL, r, pos, '[', 1);

		rel = rel_groupby(sql->sa, nrel, gexps);
		rel->exps = exps;
		return rel;
	case 's':
	case 'a':
		if (r[*pos+2] == 'l') {
			*pos += strlen("select");
			skipWS(r, pos);
			if (r[*pos] != '(') 
				return sql_error(sql, -1, "select: missing '('\n");
			(*pos)++;
			skipWS(r, pos);
			nrel = rel_read(sql, r, pos);
			skipWS(r, pos);
			if (r[*pos] != ')') 
				return sql_error(sql, -1, "select: missing ')'\n");
			(*pos)++;
			skipWS(r, pos);

			exps = read_exps(sql, nrel, NULL, r, pos, '[', 0);
			rel = rel_select_copy(sql->sa, nrel, exps);
			return rel;
			// semijoin or antijoin
		} else if (r[*pos+2] == 'l' || r[*pos+1] == 'n') {
			j = op_semi;

			if (r[*pos+1] == 'n') 
				j = op_semi;

			*pos += strlen("semijoin");
			skipWS(r, pos);
			if (r[*pos] != '(') 
				return sql_error(sql, -1, "semijoin: missing '('\n");
			(*pos)++;
			skipWS(r, pos);
			lrel = rel_read(sql, r, pos);
			skipWS(r, pos);

			if (r[*pos] != ',') 
				return sql_error(sql, -1, "semijoin: missing ','\n");
			(*pos)++;
			skipWS(r, pos);
			rrel = rel_read(sql, r, pos);

			skipWS(r, pos);
			if (r[*pos] != ')') 
				return sql_error(sql, -1, "semijoin: missing ')'\n");
			(*pos)++;
			skipWS(r, pos);

			exps = read_exps(sql, lrel, rrel, r, pos, '[', 0);
			rel = rel_crossproduct(sql->sa, lrel, rrel, j);
			rel->exps = exps;
			return rel;
		}
		break;
	case 'l':
		*pos += strlen("left outer join");
		j = op_left;
		/* fall through */
	case 'r': 
		if (!j) {
			*pos += strlen("right outer join");
			j = op_right;
		}
		/* fall through */
	case 'f':
		if (!j) {
			*pos += strlen("full outer join");
			j = op_full;
		}
		/* fall through */
	case 'c':
		if (!j) {
			*pos += strlen("crossproduct");
			j = op_join;
		}
		/* fall through */
	case 'j':
		if (!j) {
			*pos += strlen("join");
			j = op_join;
		}
		skipWS(r, pos);

		if (r[*pos] != '(') 
			return sql_error(sql, -1, "join: missing '('\n");
		(*pos)++;
		skipWS(r, pos);
		lrel = rel_read(sql, r, pos);
		skipWS(r, pos);

		if (r[*pos] != ',') 
			return sql_error(sql, -1, "join: missing ','\n");
		(*pos)++;
		skipWS(r, pos);
		rrel = rel_read(sql, r, pos);

		skipWS(r, pos);
		if (r[*pos] != ')') 
			return sql_error(sql, -1, "join: missing ')'\n");
		(*pos)++;
		skipWS(r, pos);

		exps = read_exps(sql, lrel, rrel, r, pos, '[', 0);
		rel = rel_crossproduct(sql->sa, lrel, rrel, j);
		rel->exps = exps;
		return rel;
	case 'u':
		if (!j) {
			*pos += strlen("union");
			j = op_union;
		}
	case 'i':
		if (!j) {
			*pos += strlen("intersect");
			j = op_inter;
		}
	case 'e':
		if (!j) {
			*pos += strlen("except");
			j = op_except;
		}
		skipWS(r, pos);

		if (r[*pos] != '(') 
			return sql_error(sql, -1, "setop: missing '('\n");
		(*pos)++;
		skipWS(r, pos);
		lrel = rel_read(sql, r, pos);
		skipWS(r, pos);

		if (r[*pos] != ',') 
			return sql_error(sql, -1, "setop: missing ','\n");
		(*pos)++;
		skipWS(r, pos);
		rrel = rel_read(sql, r, pos);

		skipWS(r, pos);
		if (r[*pos] != ')') 
			return sql_error(sql, -1, "setop: missing ')'\n");
		(*pos)++;
		skipWS(r, pos);

		exps = read_exps(sql, lrel, rrel, r, pos, '[', 0);
		rel = rel_setop(sql->sa, lrel, rrel, j);
		rel->exps = exps;
		return rel;
	case 'd':
		if (r[*pos+1] == 'i') {
			*pos += strlen("distinct");
			skipWS(r, pos);
			distinct = 1;
			break;
		}
		/* 'ddl' not supported */
	default:
		return NULL;
	}
	return NULL;
}

