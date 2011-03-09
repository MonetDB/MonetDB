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
#include "sql_parser.h"
#include "sql_symbol.h"
#include "rel_semantic.h"
#include "rel_select.h"
#include "rel_updates.h"
#include "rel_trans.h"
#include "rel_schema.h"
#include "rel_sequence.h"
#include "rel_exp.h"

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

comp_type 
swap_compare( comp_type t )
{
	switch(t) {
	case cmp_equal:
		return cmp_equal;
	case cmp_lt:
		return cmp_gt;
	case cmp_lte:
		return cmp_gte;
	case cmp_gte:
		return cmp_lte;
	case cmp_gt:
		return cmp_lt;
	case cmp_notequal:
		return cmp_notequal;
	default:
		return cmp_equal;
	}
}

comp_type 
range2lcompare( int r )
{
	if (r&1) {
		return cmp_gte;
	} else {
		return cmp_gt;
	}
}

comp_type 
range2rcompare( int r )
{
	if (r&2) {
		return cmp_lte;
	} else {
		return cmp_lt;
	}
}

int 
compare2range( int l, int r )
{
	if (l == cmp_gt) {
		if (r == cmp_lt)
			return 0;
		else if (r == cmp_lte)
			return 2;
	} else if (l == cmp_gte) {
		if (r == cmp_lt)
			return 1;
		else if (r == cmp_lte)
			return 3;
	} 
	return -1;
}


sql_rel *
rel_parse(mvc *m, char *query, char emode)
{
	mvc o = *m;
	sql_rel *rel;
	buffer *b;
	char *n;
	int len = _strlen(query);

	m->qc = NULL;

	m->caching = 0;
	m->emode = emode;

	b = (buffer*)GDKmalloc(sizeof(buffer));
	n = GDKmalloc(len + 1 + 1);
	strncpy(n, query, len);
	query = n;
	query[len] = '\n';
	query[len+1] = 0;
	len++;
	buffer_init(b, query, len);
	scanner_init( &m->scanner, 
		bstream_create(buffer_rastream(b, "sqlstatement"), b->len),
		NULL);
	m->scanner.mode = LINE_1; 
	bstream_next(m->scanner.rs);

	m->params = NULL;
	/*m->args = NULL;*/
	m->argc = 0;
	m->sym = NULL;
	m->errstr[0] = '\0';
	/* via views we give access to protected objects */
	m->user_id = USER_MONETDB;

	(void) sqlparse(m);	/* blindly ignore errors */
	rel = rel_semantic(m, m->sym);

	GDKfree(query);
	GDKfree(b);
	bstream_destroy(m->scanner.rs);

	m->sym = NULL;
	if (m->session->status || m->errstr[0]) {
		int status = m->session->status;
		char errstr[ERRSIZE];

		strcpy(errstr, m->errstr);
		*m = o;
		m->session->status = status;
		strcpy(m->errstr, errstr);
	} else {
		*m = o;
	}
	return rel;
}

sql_rel * 
rel_semantic(mvc *sql, symbol *s)
{
	if (!s)
		return NULL;

	switch (s->token) {

	case TR_COMMIT:
	case TR_SAVEPOINT:
	case TR_RELEASE:
	case TR_ROLLBACK:
	case TR_START:
	case TR_MODE:
		return rel_transactions(sql, s);

	case SQL_CREATE_SCHEMA:
	case SQL_DROP_SCHEMA:

	case SQL_CREATE_TABLE:
	case SQL_CREATE_VIEW:
	case SQL_DROP_TABLE:
	case SQL_DROP_VIEW:
	case SQL_ALTER_TABLE:

	case SQL_GRANT:
	case SQL_REVOKE:
	case SQL_GRANT_ROLES:
	case SQL_REVOKE_ROLES:
		return rel_schemas(sql, s);

	case SQL_CREATE_SEQ:
	case SQL_ALTER_SEQ:
	case SQL_DROP_SEQ:
		return rel_sequences(sql, s);

	case SQL_CREATE_INDEX:
	case SQL_DROP_INDEX:
	case SQL_CREATE_USER:
	case SQL_DROP_USER:
	case SQL_ALTER_USER:
	case SQL_RENAME_USER:
	case SQL_CREATE_ROLE:
	case SQL_DROP_ROLE:
	case SQL_CREATE_TYPE:
	case SQL_CREATE_TRIGGER:
	case SQL_DROP_TRIGGER:
	case SQL_CONNECT:
	case SQL_DISCONNECT:

	case SQL_CREATE_FUNC:
	case SQL_CREATE_PROC:
	case SQL_CREATE_AGGR:
	case SQL_DROP_FUNC:
	case SQL_DROP_PROC:
	case SQL_DECLARE:
	case SQL_CALL:
	case SQL_SET:
		return NULL;

	case SQL_INSERT:
	case SQL_UPDATE:
	case SQL_DELETE:
	case SQL_COPYFROM:
	case SQL_BINCOPYFROM:
	case SQL_COPYTO:
		return rel_updates(sql, s);

	case SQL_WITH:
	{
		dnode *d = s->data.lval->h;
		symbol *select = d->next->data.sym;
		sql_rel *rel = NULL;

		stack_push_frame(sql, "WITH");
		/* first handle all with's (ie inlined views) */
		for (d = d->data.lval->h; d; d = d->next) {
			symbol *sym = d->data.sym;
			dnode *dn = sym->data.lval->h;
			char *name = qname_table(dn->data.lval);
			sql_rel *nrel;

			if (frame_find_var(sql, name)) {
				return sql_error(sql, 01, "Variable '%s' allready declared", name);
			}
			nrel = rel_semantic(sql, sym);
			if (!nrel) {  
				stack_pop_frame(sql);
				return NULL;
			}
			stack_push_rel_view(sql, name, nrel);
			assert(is_project(nrel->op));
			if (is_project(nrel->op) && nrel->exps) {
				node *ne = nrel->exps->h;
	
				for (; ne; ne = ne->next) 
					exp_setname(sql->sa, ne->data, name, NULL );
			}
		}
		rel = rel_semantic(sql, select);
		stack_pop_frame(sql);
		return rel;
	}

	case SQL_MULSTMT: {
		dnode *d;
		sql_rel *r = NULL;

		stack_push_frame(sql, "MUL");
		for (d = s->data.lval->h; d; d = d->next) {
			symbol *sym = d->data.sym;
			sql_rel *nr = rel_semantic(sql, sym);
			
			if (!nr)
				return NULL;
			if (r)
				r = rel_list(sql->sa, r, nr);
			else
				r = nr;
		}
		stack_pop_frame(sql);
		return r;
	}
	case SQL_PREP:
	{
		dnode *d = s->data.lval->h;
		symbol *sym = d->data.sym;
		sql_rel *r = rel_semantic(sql, sym);

		if (!r) 
			return NULL;
		return r;
	}

	case SQL_SELECT:
	case SQL_JOIN:
	case SQL_CROSS:
	case SQL_UNION:
	case SQL_EXCEPT:
	case SQL_INTERSECT:
		return rel_selects(sql, s);

	default:
		return sql_error(sql, 02, "symbol type not found");
	}
}
