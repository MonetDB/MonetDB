/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
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
#include "rel_select.h"
#include "rel_sequence.h"
#include "rel_exp.h"
#include "sql_privileges.h"

char*
sql_next_seq_name(mvc *m)
{
	oid id = store_next_oid();
	oid len = 5 + ((id+7)>>3);
	char *msg = sa_alloc(m->sa, len);

	snprintf(msg, len, "seq_%d", store_next_oid());
	return msg;
}

static sql_rel *
rel_drop_seq(sql_allocator *sa, char *sname, char *seqname)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);

	append(exps, exp_atom_clob(sa, sname));
	append(exps, exp_atom_clob(sa, seqname));
	append(exps, exp_atom_int(sa, 0));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = DDL_DROP_SEQ;
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

	if (val)
		append(exps, val);
	else
		append(exps, exp_atom_int(sa, 0));
	append(exps, exp_atom_str(sa, sname, sql_bind_localtype("str") ));
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
	int cycle,
	int bedropped)
{
	sql_rel *res = NULL;
	sql_sequence *seq = NULL;
	char* name = qname_table(qname);
	char *sname = qname_schema(qname);
	sql_schema *s = NULL;

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, "CREATE SEQUENCE: no such schema '%s'", sname);
	if (s == NULL)
		s = ss;
	(void) tpe;
	if (find_sql_sequence(s, name)) {
		return sql_error(sql, 02,
				"CREATE SEQUENCE: "
				"name '%s' already in use", name);
	} else if (!schema_privs(sql->role_id, s)) {
		return sql_error(sql, 02,
				"CREATE SEQUENCE: insufficient privileges "
				"for '%s' in schema '%s'", stack_get_string(sql, "current_user"), s->base.name);
	}

	/* generate defaults */
	if (inc <= 0) inc = 1;
	if (min < 0) min = 0;
	if (cycle && max < 0) cycle = 0;
	if (max < 0) max = 0;
	if (cache <= 0) cache = 1;

	seq = create_sql_sequence(sql->sa, s, name, start, min, max, inc, cache, cycle);  
	seq->bedropped = bedropped;
	res = rel_seq(sql->sa, DDL_CREATE_SEQ, s->base.name, seq, NULL, NULL);
	/* for multi statements we keep the sequence around */
	if (res && stack_has_frame(sql, "MUL") != 0)
		stack_push_rel_view(sql, name, rel_dup(res));
	return res;
}

static sql_rel *
rel_alter_seq(
		mvc *sql,
		sql_schema *ss,
		dlist *qname,
		sql_subtype *tpe,
		dlist* start_list,
		lng inc,
		lng min,
		lng max,
		lng cache,
		int cycle)
{
	char* name = qname_table(qname);
	char *sname = qname_schema(qname);
	sql_sequence *seq;
	sql_schema *s = NULL;

	int start_type = start_list->h->data.i_val;
	sql_rel *r = NULL;
	sql_exp *val = NULL;

	assert(start_list->h->type == type_int);
	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, "CREATE SEQUENCE: no such schema '%s'", sname);
	if (!s)
		s = ss;
	(void) tpe;
	if (!(seq = find_sql_sequence(s, name))) {
		return sql_error(sql, 02,
				"ALTER SEQUENCE: "
				"no such sequence '%s'", name);
	}
	if (!schema_privs(sql->role_id, s)) {
		return sql_error(sql, 02,
				"ALTER SEQUENCE: insufficient privileges "
				"for '%s' in schema '%s'", stack_get_string(sql, "current_user"), s->base.name);
	}

	/* first alter the known values */
	seq = create_sql_sequence(sql->sa, s, name, seq->start, min, max, inc, cache, cycle);  

	/* restart may be a query, i.e. we create a statement 
	   restart(ssname,seqname,value) */ 

	if (start_type == 0) {
		val = exp_atom_lng(sql->sa, seq->start);
	} else if (start_type == 1) { /* value (exp) */
		exp_kind ek = {type_value, card_value, FALSE};
		int is_last = 0;
		sql_subtype *lng_t = sql_bind_localtype("lng");

		val = rel_value_exp2(sql, &r, start_list->h->next->data.sym, sql_sel, ek, &is_last);
		(void)is_last;
		if (!val || !(val = rel_check_type(sql, lng_t, val, type_equal)))
			return NULL;
	} else if (start_type == 2) {
		switch (start_list->h->next->type) {
			case type_int:
				val = exp_atom_lng(sql->sa, (lng)start_list->h->next->data.i_val);
				break;
			case type_lng:
				val = exp_atom_lng(sql->sa, start_list->h->next->data.l_val);
				break;
			default:
				assert(0);
		}
	}
	return rel_seq(sql->sa, DDL_ALTER_SEQ, s->base.name, seq, r, val);
}

sql_rel *
rel_sequences(mvc *sql, symbol *s)
{
	sql_rel *res = NULL;

	switch (s->token) {
		case SQL_CREATE_SEQ:
		{
			dlist *l = s->data.lval;
			dlist *h = l->h->next->next->data.lval;

			assert(h->h->type == type_lng);
			assert(h->h->next->type == type_lng);
			assert(h->h->next->next->type == type_lng);
			assert(h->h->next->next->next->type == type_lng);
			assert(h->h->next->next->next->next->type == type_lng);
			assert(h->h->next->next->next->next->next->type == type_int);
			assert(h->h->next->next->next->next->next->next->type == type_int);
			res = rel_create_seq(
/* mvc* sql */		sql,
/* sql_schema* s */	cur_schema(sql), 
/* dlist* qname */	l->h->data.lval, 
/* sql_subtype* t*/	&l->h->next->data.typeval, 
/* lng start */		h->h->data.l_val, 
/* lng inc */		h->h->next->data.l_val, 
/* lng min */		h->h->next->next->data.l_val, 
/* lng max */		h->h->next->next->next->data.l_val,
/* lng cache */		h->h->next->next->next->next->data.l_val,
/* int cycle */		h->h->next->next->next->next->next->data.i_val,
/* int bedropped */	h->h->next->next->next->next->next->next->data.i_val);
		}
		break;
		case SQL_ALTER_SEQ:
		{
			dlist* l = s->data.lval;
			dlist *h = l->h->next->next->data.lval;
			
			assert(h->h->next->type == type_lng);
			assert(h->h->next->next->type == type_lng);
			assert(h->h->next->next->next->type == type_lng);
			assert(h->h->next->next->next->next->type == type_lng);
			assert(h->h->next->next->next->next->next->type == type_int);
			res = rel_alter_seq(
/* mvc* sql */		sql,
/* sql_schema* s */	cur_schema(sql), 
/* dlist* qname */	l->h->data.lval, 
/* sql_subtype* t*/	&l->h->next->data.typeval, 
/* dlist start */	h->h->data.lval, 
/* lng inc */		h->h->next->data.l_val, 
/* lng min */		h->h->next->next->data.l_val, 
/* lng max */		h->h->next->next->next->data.l_val,
/* lng cache */		h->h->next->next->next->next->data.l_val,
/* int cycle */		h->h->next->next->next->next->next->data.i_val);
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
			return sql_error(sql, 01, "sql_stmt Symbol(" PTRFMT ")->token = %s", PTRFMTCAST s, token2string(s->token));
	}
	sql->type = Q_SCHEMA; 
	return res;
}
