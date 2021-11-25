/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_trans.h"
#include "rel_select.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "sql_parser.h"

static sql_rel *
rel_trans(mvc *sql, int trans_type, int nr, char *name)
{
	sql_rel *rel = rel_create(sql->sa);
	list *exps = new_exp_list(sql->sa);
	if(!rel || !exps)
		return NULL;

	append(exps, exp_atom_int(sql->sa, nr));
	if (name)
		append(exps, exp_atom_clob(sql->sa, name));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = trans_type;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
}

sql_rel *
rel_transactions(sql_query *query, symbol *s)
{
	mvc *sql = query->sql;
	sql_rel *ret = NULL;

	switch (s->token) {
	case TR_RELEASE:
		ret = rel_trans(sql, ddl_release, 0, s->data.sval);
		break;
	case TR_COMMIT:
		assert(s->type == type_int);
		ret = rel_trans(sql, ddl_commit, s->data.i_val, NULL);
		break;
	case TR_SAVEPOINT:
		ret = rel_trans(sql, ddl_commit, 0, s->data.sval);
		break;
	case TR_ROLLBACK: {
		dnode *n = s->data.lval->h;
		assert(n->type == type_int);
		ret= rel_trans(sql, ddl_rollback, n->data.i_val, n->next->data.sval);
	} 	break;
	case TR_START:
	case TR_MODE: {
		int tr_mode = s->data.i_val;

		assert(s->type == type_int && (tr_mode & tr_append) == 0);
		if ((tr_mode & tr_none) == tr_none)
			return sql_error(sql, 01, SQLSTATE(42000) "Transaction diagnostic not supported");
		if ((tr_mode & tr_readonly) == tr_readonly)
			return sql_error(sql, 01, SQLSTATE(42000) "Readonly transactions not supported");
		if ((tr_mode & tr_snapshot) == tr_snapshot && (tr_mode & tr_serializable) == tr_serializable)
			return sql_error(sql, 01, SQLSTATE(42000) "Cannot set multiple ISO levels on the same transaction");
		if ((tr_mode & tr_snapshot) == 0 && (tr_mode & tr_serializable) == 0)
			tr_mode |= tr_serializable; /* set serializable level by default */
		tr_mode &= ~tr_writable; /* all transactions are writable by default */
		ret = rel_trans(sql, ddl_trans, tr_mode, NULL);
	} 	break;
	default:
		return sql_error(sql, 01, SQLSTATE(42000) "Transaction unknown Symbol(%p)->token = %s", s, token2string(s->token));
	}
	return ret;
}
