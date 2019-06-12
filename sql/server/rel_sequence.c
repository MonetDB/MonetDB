/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_select.h"
#include "rel_rel.h"
#include "rel_sequence.h"
#include "rel_exp.h"
#include "sql_privileges.h"

char*
sql_next_seq_name(mvc *m)
{
	sqlid id = store_next_oid();
	size_t len = 5 + 10;	/* max nr of digits of (4 bytes) int is 10 */
	char *msg = sa_alloc(m->sa, len);

	snprintf(msg, len, "seq_%d", id);
	return msg;
}

static sql_rel *
rel_drop_seq(sql_allocator *sa, char *sname, char *seqname)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);
	if(!rel || !exps)
		return NULL;

	append(exps, exp_atom_clob(sa, sname));
	append(exps, exp_atom_clob(sa, seqname));
	append(exps, exp_atom_int(sa, 0));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = ddl_drop_seq;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
}

static sql_rel *
rel_seq(sql_allocator *sa, int cat_type, char *sname, sql_sequence *s, sql_rel *r, sql_exp *val)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);
	if(!rel || !exps)
		return NULL;

	if (val)
		append(exps, val);
	else
		append(exps, exp_atom_int(sa, 0));
	append(exps, exp_atom_str(sa, sname, sql_bind_localtype("str") ));
	append(exps, exp_atom_str(sa, s->base.name, sql_bind_localtype("str") ));
	append(exps, exp_atom_ptr(sa, s));
	rel->l = r;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = cat_type;
	rel->exps = exps;
	rel->card = CARD_MULTI;
	rel->nrcols = 0;
	return rel;
}

static sql_rel *
rel_create_seq(
	mvc *sql,
	sql_schema *ss,
	dlist *qname,
	sql_subtype *tpe,
	lng start,
	lng inc,
	lng min,
	lng max,
	lng cache,
	bit cycle,
	bit bedropped)
{
	sql_rel *res = NULL;
	sql_sequence *seq = NULL;
	char *name = qname_table(qname);
	char *sname = qname_schema(qname);
	sql_schema *s = NULL;

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, SQLSTATE(3F000) "CREATE SEQUENCE: no such schema '%s'", sname);
	if (s == NULL)
		s = ss;
	(void) tpe;
	if (find_sql_sequence(s, name)) {
		return sql_error(sql, 02, SQLSTATE(42000) "CREATE SEQUENCE: name '%s' already in use", name);
	} else if (!mvc_schema_privs(sql, s)) {
		return sql_error(sql, 02, SQLSTATE(42000) "CREATE SEQUENCE: insufficient privileges "
				"for '%s' in schema '%s'", stack_get_string(sql, "current_user"), s->base.name);
	}

	/* generate defaults */
	if (is_lng_nil(start)) start = 1;
	if (is_lng_nil(inc)) inc = 1;
	if (is_lng_nil(min)) min = 0;
	if (cycle && is_lng_nil(max)) cycle = 0;
	if (is_lng_nil(max)) max = 0;
	if (is_lng_nil(cache)) cache = 1;

	seq = create_sql_sequence(sql->sa, s, name, start, min, max, inc, cache, cycle);
	seq->bedropped = bedropped;
	res = rel_seq(sql->sa, ddl_create_seq, s->base.name, seq, NULL, NULL);
	/* for multi statements we keep the sequence around */
	if (res && stack_has_frame(sql, "MUL") != 0) {
		if(!stack_push_rel_view(sql, name, rel_dup(res)))
			return sql_error(sql, 02, SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	return res;
}

#define SEQ_TYPE	0
#define SEQ_START	1
#define SEQ_INC		2
#define SEQ_MIN		3
#define SEQ_MAX		4
#define SEQ_CYCLE	5
#define SEQ_CACHE	6

static sql_rel *
list_create_seq(
	mvc *sql,
	sql_schema *ss,
	dlist *qname,
	dlist *options,
	bit bedropped)
{
	dnode *n;
	sql_subtype* t = NULL;
	lng start = lng_nil, inc = lng_nil, min = lng_nil, max = lng_nil, cache = lng_nil;
	unsigned int used = 0;
	bit cycle = 0;

	/* check if no option is given twice */
	for (n = options->h; n; n = n->next) {
		symbol *s = n->data.sym;

		switch(s->token) {
		case SQL_TYPE:
			if ((used&(1<<SEQ_TYPE)))
				return sql_error(sql, 02, SQLSTATE(3F000) "CREATE SEQUENCE: AS type found should be used as most once");
			used |= (1<<SEQ_TYPE);
			t = &s->data.lval->h->data.typeval;
			break;
		case SQL_START:
			if ((used&(1<<SEQ_START)))
				return sql_error(sql, 02, SQLSTATE(3F000) "CREATE SEQUENCE: START value should be passed as most once");
			used |= (1<<SEQ_START);
			if (is_lng_nil(s->data.l_val))
				return sql_error(sql, 02, SQLSTATE(42000) "CREATE SEQUENCE: START must not be null");
			start = s->data.l_val;
			break;
		case SQL_INC:
			if ((used&(1<<SEQ_INC)))
				return sql_error(sql, 02, SQLSTATE(3F000) "CREATE SEQUENCE: INCREMENT value should be passed as most once");
			used |= (1<<SEQ_INC);
			if (is_lng_nil(s->data.l_val))
				return sql_error(sql, 02, SQLSTATE(42000) "CREATE SEQUENCE: INCREMENT must not be null");
			inc = s->data.l_val;
			break;
		case SQL_MINVALUE:
			if ((used&(1<<SEQ_MIN)))
				return sql_error(sql, 02, SQLSTATE(3F000) "CREATE SEQUENCE: MINVALUE or NO MINVALUE should be passed as most once");
			used |= (1<<SEQ_MIN);
			if (is_lng_nil(s->data.l_val))
				return sql_error(sql, 02, SQLSTATE(42000) "CREATE SEQUENCE: MINVALUE must not be null");
			min = s->data.l_val;
			break;
		case SQL_MAXVALUE:
			if ((used&(1<<SEQ_MAX)))
				return sql_error(sql, 02, SQLSTATE(3F000) "CREATE SEQUENCE: MAXVALUE or NO MAXVALUE should be passed as most once");
			used |= (1<<SEQ_MAX);
			if (is_lng_nil(s->data.l_val))
				return sql_error(sql, 02, SQLSTATE(42000) "CREATE SEQUENCE: MAXVALUE must be non-NULL");
			max = s->data.l_val;
			break;
		case SQL_CYCLE:
			if ((used&(1<<SEQ_CYCLE)))
				return sql_error(sql, 02, SQLSTATE(3F000) "CREATE SEQUENCE: CYCLE or NO CYCLE should be passed as most once");
			used |= (1<<SEQ_CYCLE);
			cycle = s->data.i_val != 0;
			break;
		case SQL_CACHE:
			if ((used&(1<<SEQ_CACHE)))
				return sql_error(sql, 02, SQLSTATE(3F000) "CREATE SEQUENCE: CACHE value should be passed as most once");
			used |= (1<<SEQ_CACHE);
			if (is_lng_nil(s->data.l_val))
				return sql_error(sql, 02, SQLSTATE(42000) "CREATE SEQUENCE: CACHE must be non-NULL");
			cache = s->data.l_val;
			break;
		default:
			assert(0);
		}
	}
	return rel_create_seq(sql, ss, qname, t, start, inc, min, max, cache, cycle, bedropped);
}

static sql_rel *
rel_alter_seq(
		sql_query *query,
		sql_schema *ss,
		dlist *qname,
		sql_subtype *tpe,
		dlist* start_list,
		lng inc,
		lng min,
		lng max,
		lng cache,
		bit cycle)
{
	mvc *sql = query->sql;
	char* name = qname_table(qname);
	char *sname = qname_schema(qname);
	sql_sequence *seq;
	sql_schema *s = NULL;

	int start_type = start_list->h->data.i_val;
	sql_rel *r = NULL;
	sql_exp *val = NULL;

	assert(start_list->h->type == type_int);
	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, SQLSTATE(3F000) "ALTER SEQUENCE: no such schema '%s'", sname);
	if (!s)
		s = ss;
	(void) tpe;
	if (!(seq = find_sql_sequence(s, name))) {
		return sql_error(sql, 02, SQLSTATE(42000) "ALTER SEQUENCE: no such sequence '%s'", name);
	}
	if (!mvc_schema_privs(sql, s)) {
		return sql_error(sql, 02, SQLSTATE(42000) "ALTER SEQUENCE: insufficient privileges "
				"for '%s' in schema '%s'", stack_get_string(sql, "current_user"), s->base.name);
	}

	/* first alter the known values */
	seq = create_sql_sequence(sql->sa, s, name, seq->start, min, max, inc, cache, (bit) cycle);

	/* restart may be a query, i.e. we create a statement
	   restart(ssname,seqname,value) */

	if (start_type == 0) {
		val = exp_atom_lng(sql->sa, seq->start);
	} else if (start_type == 1) { /* value (exp) */
		exp_kind ek = {type_value, card_value, FALSE};
		int is_last = 0;
		sql_subtype *lng_t = sql_bind_localtype("lng");

		val = rel_value_exp2(query, &r, start_list->h->next->data.sym, sql_sel, ek, &is_last);
		if (!val || !(val = rel_check_type(sql, lng_t, val, type_equal)))
			return NULL;
		if (val && r && r->op == op_project) {
			exp_label(sql->sa, val, ++sql->label);
			val = rel_project_add_exp(sql, r, val);
			val = exp_ref(sql->sa, val);
		}
	} else if (start_type == 2) {
		assert (start_list->h->next->type == type_lng);
		val = exp_atom_lng(sql->sa, start_list->h->next->data.l_val);
	}
	if (val && val->card > CARD_ATOM) {
		sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(val));
		val = exp_aggr1(sql->sa, val, zero_or_one, 0, 0, CARD_ATOM, 0);
	}
	return rel_seq(sql->sa, ddl_alter_seq, s->base.name, seq, r, val);
}

static sql_rel *
list_alter_seq(
	sql_query *query,
	sql_schema *ss,
	dlist *qname,
	dlist *options)
{
	mvc *sql = query->sql;
	dnode *n;
	sql_subtype* t = NULL;
	lng inc = lng_nil, min = lng_nil, max = lng_nil, cache = lng_nil;
	dlist *start = NULL;
	unsigned int used = 0;
	bit cycle = 0;

	/* check if no option is given twice */
	for (n = options->h; n; n = n->next) {
		symbol *s = n->data.sym;

		switch(s->token) {
		case SQL_TYPE:
			if ((used&(1<<SEQ_TYPE)))
				return sql_error(sql, 02, SQLSTATE(3F000) "ALTER SEQUENCE: AS type found should be used as most once");
			used |= (1<<SEQ_TYPE);
			t = &s->data.lval->h->data.typeval;
			break;
		case SQL_START:
			if ((used&(1<<SEQ_START)))
				return sql_error(sql, 02, SQLSTATE(3F000) "ALTER SEQUENCE: START value should be passed as most once");
			used |= (1<<SEQ_START);
			if (is_lng_nil(s->data.l_val))
				return sql_error(sql, 02, SQLSTATE(42000) "ALTER SEQUENCE: START must be non-NULL");
			start = s->data.lval;
			break;
		case SQL_INC:
			if ((used&(1<<SEQ_INC)))
				return sql_error(sql, 02, SQLSTATE(3F000) "ALTER SEQUENCE: INCREMENT value should be passed as most once");
			used |= (1<<SEQ_INC);
			if (is_lng_nil(s->data.l_val))
				return sql_error(sql, 02, SQLSTATE(42000) "ALTER SEQUENCE: INCREMENT must be non-NULL");
			inc = s->data.l_val;
			break;
		case SQL_MINVALUE:
			if ((used&(1<<SEQ_MIN)))
				return sql_error(sql, 02, SQLSTATE(3F000) "ALTER SEQUENCE: MINVALUE or NO MINVALUE should be passed as most once");
			used |= (1<<SEQ_MIN);
			if (is_lng_nil(s->data.l_val))
				return sql_error(sql, 02, SQLSTATE(42000) "ALTER SEQUENCE: MINVALUE must be non-NULL");
			min = s->data.l_val;
			break;
		case SQL_MAXVALUE:
			if ((used&(1<<SEQ_MAX)))
				return sql_error(sql, 02, SQLSTATE(3F000) "ALTER SEQUENCE: MAXVALUE or NO MAXVALUE should be passed as most once");
			used |= (1<<SEQ_MAX);
			if (is_lng_nil(s->data.l_val))
				return sql_error(sql, 02, SQLSTATE(42000) "ALTER SEQUENCE: MAXVALUE must be non-NULL");
			max = s->data.l_val;
			break;
		case SQL_CYCLE:
			if ((used&(1<<SEQ_CYCLE)))
				return sql_error(sql, 02, SQLSTATE(3F000) "ALTER SEQUENCE: CYCLE or NO CYCLE should be passed as most once");
			used |= (1<<SEQ_CYCLE);
			cycle = s->data.i_val != 0;
			break;
		case SQL_CACHE:
			if ((used&(1<<SEQ_CACHE)))
				return sql_error(sql, 02, SQLSTATE(3F000) "ALTER SEQUENCE: CACHE value should be passed as most once");
			used |= (1<<SEQ_CACHE);
			if (is_lng_nil(s->data.l_val))
				return sql_error(sql, 02, SQLSTATE(42000) "ALTER SEQUENCE: CACHE must be non-NULL");
			cache = s->data.l_val;
			break;
		default:
			assert(0);
		}
	}
	return rel_alter_seq(query, ss, qname, t, start, inc, min, max, cache, cycle);
}

sql_rel *
rel_sequences(sql_query *query, symbol *s)
{
	mvc *sql = query->sql;
	sql_rel *res = NULL;

	switch (s->token) {
		case SQL_CREATE_SEQ:
		{
			dlist *l = s->data.lval;

			res = list_create_seq(
/* mvc* sql */		sql,
/* sql_schema* s */	cur_schema(sql),
/* dlist* qname */	l->h->data.lval,
/* dlist* options */	l->h->next->data.lval,
/* bit bedropped */	(bit) (l->h->next->next->data.i_val != 0));
		}
		break;
		case SQL_ALTER_SEQ:
		{
			dlist* l = s->data.lval;

			res = list_alter_seq(
/* mvc* sql */		query,
/* sql_schema* s */	cur_schema(sql),
/* dlist* qname */	l->h->data.lval,
/* dlist* options */	l->h->next->data.lval);
		}
		break;
		case SQL_DROP_SEQ:
		{
			dlist *l = s->data.lval;
			char *sname = qname_schema(l->h->data.lval);
			char *seqname = qname_table(l->h->data.lval);

			if (!sname) {
				sql_schema *ss = cur_schema(sql);

				sname = ss->base.name;
			}
			res = rel_drop_seq(sql->sa, sname, seqname);
		}
		break;
		default:
			return sql_error(sql, 01, SQLSTATE(42000) "sql_stmt Symbol(%p)->token = %s", s, token2string(s->token));
	}
	sql->type = Q_SCHEMA;
	return res;
}
