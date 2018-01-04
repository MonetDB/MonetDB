/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
rel_transactions(mvc *sql, symbol *s)
{
	sql_rel *ret = NULL;

	switch (s->token) {
	case TR_RELEASE:
		ret = rel_trans(sql, DDL_RELEASE, 0, s->data.sval);
		break;
	case TR_COMMIT:
		assert(s->type == type_int);
		ret = rel_trans(sql, DDL_COMMIT, s->data.i_val, NULL);
		break;
	case TR_SAVEPOINT:
		ret = rel_trans(sql, DDL_COMMIT, 0, s->data.sval);
		break;
	case TR_ROLLBACK: {
		dnode *n = s->data.lval->h;
		assert(n->type == type_int);
		ret= rel_trans(sql, DDL_ROLLBACK, n->data.i_val, n->next->data.sval);
	} 	break;
	case TR_START:
	case TR_MODE:
		assert(s->type == type_int);
		ret = rel_trans(sql, DDL_TRANS, s->data.i_val, NULL);
		break;
	default:
		return sql_error(sql, 01, "transaction unknown Symbol(" PTRFMT ")->token = %s", PTRFMTCAST s, token2string(s->token));
	}
	return ret;
}
