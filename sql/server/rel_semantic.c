/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_parser.h"
#include "sql_symbol.h"
#include "rel_semantic.h"
#include "rel_select.h"
#include "rel_updates.h"
#include "rel_trans.h"
#include "rel_schema.h"
#include "rel_psm.h"
#include "rel_sequence.h"
#include "rel_exp.h"

#include <unistd.h>
#include <string.h>
#include <ctype.h>


sql_rel *
rel_parse(mvc *m, sql_schema *s, char *query, char emode)
{
	mvc o = *m;
	sql_rel *rel = NULL;
	buffer *b;
	bstream *bs;
	stream *buf;
	char *n;
	int len = _strlen(query);
	sql_schema *c = cur_schema(m);

	m->qc = NULL;

	m->caching = 0;
	m->emode = emode;
	if (s)
		m->session->schema = s;

	b = (buffer*)GDKmalloc(sizeof(buffer));
	if (!b) {
		return NULL;
	}
	n = GDKmalloc(len + 1 + 1);
	if (!n) {
		GDKfree(b);
		return NULL;
	}
	strncpy(n, query, len);
	query = n;
	query[len] = '\n';
	query[len+1] = 0;
	len++;
	buffer_init(b, query, len);
	buf = buffer_rastream(b, "sqlstatement");
	if(buf == NULL) {
		buffer_destroy(b);
		return NULL;
	}
	bs = bstream_create(buf, b->len);
	if(bs == NULL) {
		buffer_destroy(b);
		return NULL;
	}
	scanner_init( &m->scanner, bs, NULL);
	m->scanner.mode = LINE_1; 
	bstream_next(m->scanner.rs);

	m->params = NULL;
	/*m->args = NULL;*/
	m->argc = 0;
	m->sym = NULL;
	m->errstr[0] = '\0';
	/* via views we give access to protected objects */
	if (emode != m_instantiate)
		m->user_id = USER_MONETDB;

	(void) sqlparse(m);     /* blindly ignore errors */
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
		int label = m->label;
		list *sqs = m->sqs;

		while (m->topvars > o.topvars) {
			if (m->vars[--m->topvars].name)
				c_delete(m->vars[m->topvars].name);
		}
		*m = o;
		m->sqs = sqs;
		m->label = label;
	}
	m->session->schema = c;
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

	case SQL_COMMENT:

	case SQL_GRANT:
	case SQL_REVOKE:
	case SQL_GRANT_ROLES:
	case SQL_REVOKE_ROLES:

	case SQL_CREATE_ROLE:
	case SQL_DROP_ROLE:

	case SQL_CREATE_INDEX:
	case SQL_DROP_INDEX:

	case SQL_CREATE_USER:
	case SQL_DROP_USER:
	case SQL_ALTER_USER:
	case SQL_RENAME_USER:

	case SQL_CREATE_TYPE:
	case SQL_DROP_TYPE:
		return rel_schemas(sql, s);

	case SQL_CREATE_SEQ:
	case SQL_ALTER_SEQ:
	case SQL_DROP_SEQ:
		return rel_sequences(sql, s);

	case SQL_CREATE_FUNC:
	case SQL_DROP_FUNC:
	case SQL_DECLARE:
	case SQL_CALL:
	case SQL_SET:
	
	case SQL_CREATE_TABLE_LOADER:

	case SQL_CREATE_TRIGGER:
	case SQL_DROP_TRIGGER:

	case SQL_ANALYZE:
		return rel_psm(sql, s);

	case SQL_INSERT:
	case SQL_UPDATE:
	case SQL_DELETE:
	case SQL_TRUNCATE:
	case SQL_COPYFROM:
	case SQL_BINCOPYFROM:
	case SQL_COPYLOADER:
	case SQL_COPYTO:
		return rel_updates(sql, s);

	case SQL_WITH:
		return rel_with_query(sql, s);

	case SQL_MULSTMT: {
		dnode *d;
		sql_rel *r = NULL;

		if(!stack_push_frame(sql, "MUL"))
			return sql_error(sql, 02, SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		return sql_error(sql, 02, SQLSTATE(42000) "Symbol type not found");
	}
}
