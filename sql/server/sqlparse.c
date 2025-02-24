/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#ifdef HAVE_GETOPT_H
#include "getopt.h"
#endif
#include "stream.h"
#include "sql_mvc.h"
#include "sql_parser.tab.h"
#include "rel_sequence.h"
#include "sql_semantic.h"
#include <stdio.h>

extern void bat_storage_init(void *b);
extern void bat_table_init(void *b);
extern void bat_logger_init(void *b);
extern void bat_utils_init(void *b);

void bat_storage_init(void *b) { (void)b; }
void bat_table_init(void *b) { (void)b; }
void bat_logger_init(void *b) { (void)b; }
void bat_utils_init(void *b) { (void)b; }

void
sql_add_param(mvc *sql, const char *name, sql_subtype *st)
{
	(void)sql;
	(void)name;
	(void)st;
}

int sql_bind_param(mvc *sql, const char *name)
{	/* -1 error, 0 nr-1, param */
	(void)sql;
	(void)name;
	return 0;
}

char*
sql_next_seq_name(mvc *m)
{
	(void)m;
	return NULL;
}

sql_type *
mvc_bind_type(mvc *m, const char *name)
{
	(void)m;
	(void)name;
	return NULL;
}


/* only need the session->status */
static sql_session *
session_new(allocator *sa)
{
	sql_session *s = SA_ZNEW(sa, sql_session);
	if (!s)
		return NULL;
	s->sa = sa;
	assert(sa);
	s->tr = NULL;
	return s;
}

static mvc *
mvc_new( bstream *rs, stream *ws) {
	mvc *m;

	allocator *pa = sa_create(NULL);
 	m = SA_ZNEW(pa, mvc);
	if (!m)
		return NULL;

	TRC_DEBUG(SQL_TRANS, "MVC create\n");

	m->errstr[0] = '\0';
	/* if an error exceeds the buffer we don't want garbage at the end */
	m->errstr[ERRSIZE-1] = '\0';

	m->qc = NULL;
	m->pa = pa;
	m->sa = sa_create(m->pa);
	m->ta = sa_create(m->pa);
#ifdef __has_builtin
#if __has_builtin(__builtin_frame_address)
	m->sp = (uintptr_t) __builtin_frame_address(0);
#define BUILTIN_USED
#endif
#endif
#ifndef BUILTIN_USED
	m->sp = (uintptr_t)(&m);
#endif
#undef BUILTIN_USED

	m->use_views = false;
	m->sym = NULL;

	m->role_id = m->user_id = -1;
	m->timezone = 0;
	m->sql_optimizer = INT_MAX;
	m->clientid = 0;
	m->div_min_scale = 3;

	m->emode = m_normal;
	m->emod = mod_none;
	m->reply_size = 100;
	m->debug = 0;

	m->label = 0;
	m->nid = 1;
	m->cascade_action = NULL;
	m->runs = NULL;

	m->schema_path_has_sys = true;
	m->schema_path_has_tmp = false;
	m->no_int128 = false;
	m->store = NULL;

	m->session = session_new(pa);
	if (!m->session)
		return NULL;

	m->type = Q_PARSE;
	scanner_init(&m->scanner, rs, ws);
	//m->scanner.started = 1;
	m->scanner.mode = LINE_1;
	return m;
}

char *
qname_schema(dlist *qname)
{
	assert(qname && qname->h);

	if (dlist_length(qname) == 2) {
		return qname->h->data.sval;
	} else if (dlist_length(qname) == 3) {
		return qname->h->next->data.sval;
	}
	return NULL;
}

char *
qname_schema_object(dlist *qname)
{
	assert(qname && qname->h);

	if (dlist_length(qname) == 1) {
		return qname->h->data.sval;
	} else if (dlist_length(qname) == 2) {
		return qname->h->next->data.sval;
	} else if (dlist_length(qname) == 3) {
		return qname->h->next->next->data.sval;
	}
	return "unknown";
}


static char * sp_symbol2string(mvc *sql, symbol *se, int expression, char **err, int depth, bool indent);
static char * dlist2string(mvc *sql, dlist *l, char *sep, char *lb, char *rb, int expression, char **err, int depth, bool indent);

static char *
dnode2string(mvc *sql, dnode *n, int expression, char **err, int depth, bool indent)
{
	char *s = NULL;
	if (n->type == type_string) {
		if (n->data.sval)
			s = sa_strdup(sql->ta, n->data.sval);
		else
			s = "";
	} else if (n->type == type_int) {
		char buf[10];
		snprintf(buf, sizeof(buf), "%d", n->data.i_val);
		s = sa_strdup(sql->ta, buf);
	} else if (n->type == type_lng) {
		char buf[16];
		snprintf(buf, sizeof(buf), LLFMT, n->data.l_val);
		s = sa_strdup(sql->ta, buf);
	} else if (n->type == type_symbol) {
		s = sp_symbol2string(sql, n->data.sym, expression, err, depth, indent);
	} else if (n->type == type_list) {
		s = dlist2string(sql, n->data.lval, ", ", "( ", " )", expression, err, depth, indent);
	} else if (n->type == type_type) {
		s = sql_subtype_string(sql->ta, &n->data.typeval);
	}
	return s;
}

static char *
dlist2string(mvc *sql, dlist *l, char *sep, char *lb, char *rb, int expression, char **err, int depth, bool indent)
{
	if (!l)
		return " EMPTY_LIST";
	char *b = lb;
	dnode *n;

	size_t seplen = strlen(sep);

	bool first_el = lb;
	for (n=l->h; n; n = n->next) {
		char *s = dnode2string(sql, n, expression, err, depth, indent);

		if (!s)
			return NULL;
		if (b) {
			size_t depth_chars = indent ? depth + 1 : 0;
			char *o = SA_NEW_ARRAY(sql->ta, char, depth_chars + strlen(b) + strlen(s) + seplen + 1);
			if (o) {
				char *tmp;
				tmp = stpcpy(o, b);
				if (!first_el)
					tmp = stpcpy(tmp, sep);
				if (indent) {
					tmp = stpcpy(tmp, "\n");
					for (int i=0; i < depth; i++)
						tmp = stpcpy(tmp, "\t");
				}
				tmp = stpcpy(tmp, s);

				first_el = false;
			}
			b = o;
			if (b == NULL)
				return NULL;
		} else {
			b = s;
		}
	}
	if (rb) {
		/* here we do (depth - 1) * tab + 1 * newline to align with the parent op */
		size_t depth_chars = indent ? (depth > 0 ? depth : 1) : 0;

		char *o = SA_NEW_ARRAY(sql->ta, char, depth_chars + strlen(b) + strlen(rb) + 1);
		if (o) {
			char *tmp;
			tmp = stpcpy(o, b);
			if (indent) {
				tmp = stpcpy(tmp, "\n");
				for (int i=0; i < (depth - 1); i++)
					tmp = stpcpy(tmp, "\t");
			}
			stpcpy(tmp, rb);
		}
		b = o;
	}
	return b;
}

static const char *
symbol_escape_ident(allocator *sa, const char *s)
{
	char *res = NULL;
	if (s) {
		size_t l = strlen(s);
		char *r = SA_NEW_ARRAY(sa, char, (l * 2) + 1);

		res = r;
		while (*s) {
			if (*s == '"')
				*r++ = '"';
			*r++ = *s++;
		}
		*r = '\0';
	}
	return res;
}

static char *
sa_concat(allocator *sa, char *prefix, char *a, char *infix, char *b, char *postfix)
{
	size_t len = (prefix?strlen(prefix):0) + (a?strlen(a):0) + (infix?strlen(infix):0) + (b?strlen(b):0) + (postfix?strlen(postfix):0);
	char *res = NULL;

	if (!prefix)
		prefix = "";
	if (!a)
		a = "";
	if (!postfix)
		postfix = "";
	if (!infix)
		infix = "";
	if ((res = SA_NEW_ARRAY(sa, char, len + 1)))
		stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, prefix), a), infix), b), postfix);
	return res;
}

static char *
sp_symbol2string(mvc *sql, symbol *se, int expression, char **err, int depth, bool indent)
{
	if (!se)
		return "EMPTY SYMBOL";
	/* inner sp_symbol2string uses the temporary allocator */
	switch (se->token) {
	case SQL_SELECT: {
		SelectNode *s = (SelectNode*)se;
		char *res = s->distinct?"SELECT DISTINCT (\n":"SELECT (\n";
		if (s->from)
			res = sa_concat(sql->ta, "", res, NULL, sp_symbol2string(sql, s->from, expression, err, depth, indent), "\n");
		if (s->where)
			res = sa_concat(sql->ta, "", res, "WHERE ", sp_symbol2string(sql, s->where, expression, err, depth + 1, indent), "\n");
		if (s->groupby)
			res = sa_concat(sql->ta, "", res, NULL, sp_symbol2string(sql, s->groupby, expression, err, depth, indent), "\n");
		if (s->having)
			res = sa_concat(sql->ta, "", res, "HAVING ", sp_symbol2string(sql, s->having, expression, err, depth, indent), "\n");
		if (s->orderby)
			res = sa_concat(sql->ta, "", res, NULL, sp_symbol2string(sql, s->orderby, expression, err, depth, indent), "\n");
		if (s->selection)
			res = sa_concat(sql->ta, "", res, "SELECTION ", dlist2string(sql, s->selection, ", ", "(", ")", expression, err, depth, indent), "\n");
		return res;
	}
	case SQL_NOP: {
		dnode *lst = se->data.lval->h, *ops = NULL, *aux;
		const char *op = symbol_escape_ident(sql->ta, qname_schema_object(lst->data.lval)),
				   *sname = symbol_escape_ident(sql->ta, qname_schema(lst->data.lval));
		int i = 0, nargs = 0;
		char** inputs = NULL, *res;
		size_t inputs_length = 0, extra = sname ? strlen(sname) + 3 : 0;

		if (lst->next->next->data.lval)
			ops = lst->next->next->data.lval->h;

		for (aux = ops; aux; aux = aux->next)
			nargs++;
		if (!(inputs = SA_ZNEW_ARRAY(sql->ta, char*, nargs)))
			return NULL;

		for (aux = ops; aux; aux = aux->next) {
			if (!(inputs[i] = sp_symbol2string(sql, aux->data.sym, expression, err, depth, false))) {
				return NULL;
			}
			inputs_length += strlen(inputs[i]);
			i++;
		}

		if ((res = SA_NEW_ARRAY(sql->ta, char, extra + strlen(op) + inputs_length + 3 + (nargs - 1 /* commas */) + 2))) {
			char *concat = res;
			if (sname)
				concat = stpcpy(stpcpy(stpcpy(res, "\""), sname), "\".");
			concat = stpcpy(stpcpy(stpcpy(concat, "\""), op), "\"(");
			i = 0;
			for (aux = ops; aux; aux = aux->next) {
				concat = stpcpy(concat, inputs[i]);
				if (aux->next)
					concat = stpcpy(concat, ",");
				i++;
			}
			concat = stpcpy(concat, ")");
		}
		return res;
	}
	case SQL_PARAMETER:
		return sa_strdup(sql->ta, "?");
	case SQL_NULL:
		return sa_strdup(sql->ta, "NULL");
	case SQL_ATOM:{
		AtomNode *an = (AtomNode *) se;
		if (an && an->a)
			return atom2sql(sql->ta, an->a, sql->timezone);
		else
			return sa_strdup(sql->ta, "NULL");
	}
	case SQL_NEXT: {
		const char *seq = symbol_escape_ident(sql->ta, qname_schema_object(se->data.lval)),
				   *sname = qname_schema(se->data.lval);
		char *res;

		if (!sname)
			sname = "";
		else
			sname = symbol_escape_ident(sql->ta, sname);
		if (!seq)
			seq = "";

		if ((res = SA_NEW_ARRAY(sql->ta, char, strlen("next value for \"") + strlen(sname) + strlen(seq) + 5)))
			stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, "next value for \""), sname), "\".\""), seq), "\"");
		return res;
	}	break;
	case SQL_IDENT:
	case SQL_COLUMN: {
		/* can only be variables */
		dlist *l = se->data.lval;
		assert(l->h->type != type_lng);
		if (expression && dlist_length(l) == 1 && l->h->type == type_string) {
			/* when compiling an expression, a column of a table might be present in the symbol, so we need this case */
			const char *op = symbol_escape_ident(sql->ta, l->h->data.sval);
			char *res;

			if ((res = SA_NEW_ARRAY(sql->ta, char, strlen(op) + 3)))
				stpcpy(stpcpy(stpcpy(res, "\""), op), "\"");
			return res;
		} else if (expression && dlist_length(l) == 2 && l->h->type == type_string && l->h->next->type == type_string) {
			const char *first = symbol_escape_ident(sql->ta, l->h->data.sval),
					   *second = symbol_escape_ident(sql->ta, l->h->next->data.sval);
			char *res;

			if (!first || !second)
				return NULL;
			if ((res = SA_NEW_ARRAY(sql->ta, char, strlen(first) + strlen(second) + 6)))
				stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, "\""), first), "\".\""), second), "\"");
			return res;
		} else {
			return dlist2string(sql, l, ".", NULL, NULL, expression, err, depth, false);
		}
	}
	case SQL_CAST: {
		dlist *dl = se->data.lval;
		char *val = NULL, *tpe = NULL, *res;

		if (!(val = sp_symbol2string(sql, dl->h->data.sym, expression, err, depth, false), 0) ||
			!(tpe = subtype2string2(sql->ta, &dl->h->next->data.typeval)))
			return NULL;
		if ((res = SA_NEW_ARRAY(sql->ta, char, strlen(val) + strlen(tpe) + 11)))
			stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, "cast("), val), " as "), tpe), ")");
		return res;
	}
	case SQL_SET: { /* dlist(ident-list, expression) */
					  dlist *set = se->data.lval;
					  char *ident = dlist2string(sql, set->h->data.lval, ".", NULL, NULL, expression, err, depth, false);
					  char *exp = dnode2string(sql, set->h->next, expression, err, depth, indent);
					  size_t len = strlen(ident) + strlen(exp);
					  char *res;
					  if ((res = SA_NEW_ARRAY(sql->ta, char, len + 6)))
							stpcpy(stpcpy(stpcpy(stpcpy(res, "SET "), ident), " "), exp);
					  return res;
				}
	case SQL_AND:
	case SQL_OR: {
		char *tok_str = token2string(se->token);
		char *args = dlist2string(sql, se->data.lval, ", ", " (", " )", expression, err, depth + 1, indent);
		char *res;

		if (!args)
			args = "";
		 if ((res = SA_NEW_ARRAY(sql->ta, char, strlen(tok_str) + strlen(args) + 1)))
			 stpcpy(stpcpy(res, tok_str), args);
		return res;
	}
	default:
		/* generic */
		if (se->type == type_list) {
			char *tok_str = token2string(se->token);
			char *args = dlist2string(sql, se->data.lval, ", ", " (", " )", expression, err, depth, false);
			char *res;

			if (!args)
				args = "";
			if ((res = SA_NEW_ARRAY(sql->ta, char, strlen(tok_str) + strlen(args) + 1)))
				stpcpy(stpcpy(res, tok_str), args);
			return res;
		} else if (se->type == type_symbol) {
			char *tok_str = token2string(se->token);
			char *sym = sp_symbol2string(sql, se->data.sym, expression, err, depth, false);
			char *res;

			if ((res = SA_NEW_ARRAY(sql->ta, char, strlen(tok_str) + strlen(sym) + 5)))
				stpcpy(stpcpy(stpcpy(stpcpy(res, tok_str), " "), sym), " ");
			return res;
		} else if (se->type == type_string) {
			char *tok_str = token2string(se->token);
			char *res = NULL;

			if (se->data.sval && ((res = SA_NEW_ARRAY(sql->ta, char, strlen(tok_str) + strlen(se->data.sval) + 2)))) {
				stpcpy(stpcpy(stpcpy(res, tok_str), " "), se->data.sval);
				return res;
			} else
				return tok_str;
			return res;
		} else if (se->type == type_int) {
			char *tok_str = token2string(se->token), *res = NULL;
			char buf[10];
			snprintf(buf, sizeof(buf), "%d", se->data.i_val);

			if ((res = SA_NEW_ARRAY(sql->ta, char, strlen(tok_str) + strlen(buf) + 2))) {
				stpcpy(stpcpy(stpcpy(res, tok_str), " "), buf);
			}
			return res;
		} else if (se->type == type_lng) {
			char *tok_str = token2string(se->token), *res = NULL;
			char buf[10];
			snprintf(buf, sizeof(buf), LLFMT, se->data.l_val);

			if ((res = SA_NEW_ARRAY(sql->ta, char, strlen(tok_str) + strlen(buf) + 2))) {
				stpcpy(stpcpy(stpcpy(res, tok_str), " "), buf);
			}
			return res;
		} else {
			const char msg[] = "SQL feature not yet available for expressions and default values: ";
			char *tok_str = token2string(se->token);
			if ((*err = SA_NEW_ARRAY(sql->ta, char, strlen(msg) + strlen(tok_str) + 1)))
				stpcpy(stpcpy(*err, msg), tok_str);
		}
	}
	return NULL;
}

static void
usage_error(char *selfname)
{
	printf("Usage: %s [-b] file.sql\n", selfname);
}

int
main(int argc, char *argv[])
{
	if (argc != 2 && argc != 3) {
		usage_error(argv[0]);
		return -1;
	}

	int opt;
	bool beautify = false;
	while ((opt = getopt(argc, argv, "b")) != -1) {
		switch (opt) {
		case 'b':
			beautify = true;
			break;
		default: /* '?' */
			usage_error(argv[0]);
			return -1;
		}
	}

	mnstr_init();

	stream *f = open_rstream(argv[optind]);
	if (!f) {
		printf("ERROR: Failed to open file '%s'\n", argv[optind]);
		return -2;
	}
	bstream *rs = bstream_create(f, 8192);
	mvc *m = mvc_new( rs, stdout_wastream());
	keyword_init();
    if (scanner_init_keywords() != 0) {
		return -3;
	}
	types_init(m->pa);
	/* parse file */
	int err = 0;
	/* read some data */
	if (bstream_next(rs) < 0)
		return -4;
	while ((err = sqlparse(m)) == 0 && m->scanner.rs->pos < m->scanner.rs->len) {
		if (m->sym) {
			char *err = NULL;
			char *res = sp_symbol2string(m, m->sym, 1, &err, 0, beautify);

			if (err)
				printf("ERROR:\n%s\n", err);
			else
				printf("SYM:\n%s\n", res);
	 	} else {
			if (m->session->status)
				printf("ERROR:\n%s\n", m->errstr);
		}
		scanner_query_processed(&m->scanner);
		m->sym = NULL;
	}
	return 0;
}
