/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "sql_privileges.h"

#include <unistd.h>
#include <string.h>
#include <ctype.h>

sql_rel *
rel_parse(mvc *m, sql_schema *s, const char *query, char emode)
{
	mvc o = *m;
	sql_rel *rel = NULL;
	buffer *b;
	bstream *bs;
	stream *buf;
	char *n;
	size_t len = _strlen(query);
	sql_schema *c = cur_schema(m);
	sql_query *qc = NULL;

	m->qc = NULL;

	m->emode = emode;
	if (s)
		m->session->schema = s;

	if ((b = malloc(sizeof(buffer))) == NULL)
		return NULL;
	if ((n = malloc(len + 1 + 1)) == NULL) {
		free(b);
		return NULL;
	}
	snprintf(n, len + 2, "%s\n", query);
	len++;
	buffer_init(b, n, len);
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
	m->sym = NULL;
	m->errstr[0] = '\0';
	/* via views we give access to protected objects */
	assert(emode == m_instantiate || emode == m_deps);
	m->user_id = USER_MONETDB;

	(void) sqlparse(m);     /* blindly ignore errors */
	qc = query_create(m);
	rel = rel_semantic(qc, m->sym);

	buffer_destroy(b);
	bstream_destroy(m->scanner.rs);

	m->sym = NULL;
	o.frames = m->frames;	/* may have been realloc'ed */
	o.sizeframes = m->sizeframes;
	if (m->session->status || m->errstr[0]) {
		int status = m->session->status;

		strcpy(o.errstr, m->errstr);
		*m = o;
		m->session->status = status;
	} else {
		unsigned int label = m->label;

		while (m->topframes > o.topframes)
			clear_frame(m, m->frames[--m->topframes]);
		*m = o;
		m->label = label;
	}
	m->session->schema = c;
	return rel;
}

sql_rel *
rel_semantic(sql_query *query, symbol *s)
{
	mvc *sql = query->sql;
	if (!s)
		return NULL;

	switch (s->token) {

	case TR_COMMIT:
	case TR_SAVEPOINT:
	case TR_RELEASE:
	case TR_ROLLBACK:
	case TR_START:
	case TR_MODE:
		return rel_transactions(query, s);

	case SQL_CREATE_SCHEMA:
	case SQL_DROP_SCHEMA:

	case SQL_DECLARE_TABLE:
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

	case SQL_RENAME_COLUMN:
	case SQL_RENAME_SCHEMA:
	case SQL_RENAME_TABLE:
	case SQL_RENAME_USER:
	case SQL_SET_TABLE_SCHEMA:

	case SQL_CREATE_TYPE:
	case SQL_DROP_TYPE:
		return rel_schemas(query, s);

	case SQL_CREATE_SEQ:
	case SQL_ALTER_SEQ:
	case SQL_DROP_SEQ:
		return rel_sequences(query, s);

	case SQL_CREATE_FUNC:
	case SQL_DROP_FUNC:
	case SQL_DECLARE:
	case SQL_CALL:
	case SQL_SET:

	case SQL_CREATE_TABLE_LOADER:

	case SQL_CREATE_TRIGGER:
	case SQL_DROP_TRIGGER:

	case SQL_ANALYZE:
		return rel_psm(query, s);

	case SQL_INSERT:
	case SQL_UPDATE:
	case SQL_DELETE:
	case SQL_TRUNCATE:
	case SQL_MERGE:
	case SQL_COPYFROM:
	case SQL_BINCOPYFROM:
	case SQL_COPYLOADER:
	case SQL_COPYTO:
		return rel_updates(query, s);

	case SQL_WITH:
		return rel_with_query(query, s);

	case SQL_MULSTMT: {
		dnode *d;
		sql_rel *r = NULL;

		if (!stack_push_frame(sql, "%MUL"))
			return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		for (d = s->data.lval->h; d; d = d->next) {
			symbol *sym = d->data.sym;
			sql_rel *nr = rel_semantic(query, sym);

			if (!nr) {
				stack_pop_frame(sql);
				return NULL;
			}
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
		sql_rel *r = rel_semantic(query, sym);

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
	case SQL_VALUES:
		return rel_selects(query, s);

	default:
		return sql_error(sql, 02, SQLSTATE(42000) "Symbol type not found");
	}
}
