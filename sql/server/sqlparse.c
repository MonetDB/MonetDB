
#include "monetdb_config.h"
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


static char * sp_symbol2string(mvc *sql, symbol *se, int expression, char **err);

static char *
dlist2string(mvc *sql, dlist *l, int expression, char **err)
{
	char *b = NULL;
	dnode *n;

	for (n=l->h; n; n = n->next) {
		char *s = NULL;

		if (n->type == type_string && n->data.sval)
			s = sa_strdup(sql->ta, n->data.sval);
		else if (n->type == type_symbol)
			s = sp_symbol2string(sql, n->data.sym, expression, err);

		if (!s)
			return NULL;
		if (b) {
			char *o = SA_NEW_ARRAY(sql->ta, char, strlen(b) + strlen(s) + 2);
			if (o)
				stpcpy(stpcpy(stpcpy(o, b), "."), s);
			b = o;
			if (b == NULL)
				return NULL;
		} else {
			b = s;
		}
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

char *
sp_symbol2string(mvc *sql, symbol *se, int expression, char **err)
{
	/* inner sp_symbol2string uses the temporary allocator */
	switch (se->token) {
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
			if (!(inputs[i] = sp_symbol2string(sql, aux->data.sym, expression, err))) {
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
			sname = sql->session->schema->base.name;
		sname = symbol_escape_ident(sql->ta, sname);

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
			char *e = dlist2string(sql, l, expression, err);
			if (e)
				*err = e;
			return NULL;
		}
	}
	case SQL_CAST: {
		dlist *dl = se->data.lval;
		char *val = NULL, *tpe = NULL, *res;

		if (!(val = sp_symbol2string(sql, dl->h->data.sym, expression, err)) || !(tpe = subtype2string2(sql->ta, &dl->h->next->data.typeval)))
			return NULL;
		if ((res = SA_NEW_ARRAY(sql->ta, char, strlen(val) + strlen(tpe) + 11)))
			stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, "cast("), val), " as "), tpe), ")");
		return res;
	}
	default: {
		const char msg[] = "SQL feature not yet available for expressions and default values: ";
		char *tok_str = token2string(se->token);
		if ((*err = SA_NEW_ARRAY(sql->ta, char, strlen(msg) + strlen(tok_str) + 1)))
			stpcpy(stpcpy(*err, msg), tok_str);
	}
	}
	return NULL;
}

int
main(int argc, char *argv[])
{
	if (argc > 2) {
		printf("usage: %s file.sql\n", argv[0]);
		return -1;
	}
	mnstr_init();

	stream *f = open_rstream(argv[1]);
	if (!f) {
		printf("ERROR: Failed to open file '%s'\n", argv[1]);
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
			char *res = sp_symbol2string(m, m->sym, 1, &err);

			if (err)
				printf("ERROR: %s\n", err);
			else
				printf("SYM: %s\n", res);
	 	}
		scanner_query_processed(&m->scanner);
		m->sym = NULL;
	}
	return 0;
}
