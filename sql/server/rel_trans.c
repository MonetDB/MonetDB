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
#include "rel_trans.h"
#include "rel_select.h"
#include "rel_exp.h"
#include "sql_parser.h"

static sql_rel *
rel_trans(mvc *sql, int trans_type, int nr, char *name)
{
	sql_rel *rel = rel_create(sql->sa);
	list *exps = new_exp_list(sql->sa);

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
