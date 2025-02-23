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

/*
 * authors M Kersten, N Nes
 * SQL catalog support implementation
 * This module contains the wrappers around the SQL catalog operations
 */
#include "monetdb_config.h"
#include "sql_transaction.h"
#include "sql_gencode.h"
#include "sql_optimizer.h"
#include "sql_scenario.h"
#include "sql_mvc.h"
#include "sql_qc.h"
#include "sql_optimizer.h"
#include "mal_namespace.h"
#include "querylog.h"
#include "mal_builder.h"

#include "rel_select.h"
#include "rel_prop.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_bin.h"
#include "rel_dump.h"
#include "orderidx.h"

#define initcontext() \
	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)\
		return msg;\
	if ((msg = checkSQLContext(cntxt)) != NULL)\
		return msg; \
	if (strNil(name))\
		name = NULL;

str
SQLtransaction_release(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg;
	int chain = *getArgReference_int(stk, pci, 1);
	str name = *getArgReference_str(stk, pci, 2);

	initcontext();

	(void) chain;
	if (sql->session->auto_commit)
		throw(SQL, "sql.trans", SQLSTATE(3BM30) "RELEASE SAVEPOINT: not allowed in auto commit mode");
	return mvc_release(sql, name);
}

str
SQLtransaction_commit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg;
	int chain = *getArgReference_int(stk, pci, 1);
	str name = *getArgReference_str(stk, pci, 2);

	initcontext();

	if (sql->session->auto_commit) {
		if (name)
			throw(SQL, "sql.trans", SQLSTATE(3BM30) "SAVEPOINT: not allowed in auto commit mode");
		throw(SQL, "sql.trans", SQLSTATE(2DM30) "COMMIT: not allowed in auto commit mode");
	}
	return mvc_commit(sql, chain, name, false);
}

str
SQLtransaction_rollback(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg;
	int chain = *getArgReference_int(stk, pci, 1);
	str name = *getArgReference_str(stk, pci, 2);

	initcontext();

	if (sql->session->auto_commit)
		throw(SQL, "sql.trans", SQLSTATE(2DM30) "ROLLBACK: not allowed in auto commit mode");
	return mvc_rollback(sql, chain, name, false);
}

str
SQLtransaction_begin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg;
	int chain = *getArgReference_int(stk, pci, 1);
	str name = *getArgReference_str(stk, pci, 2);

	initcontext();

	(void) chain;
	if (!sql->session->auto_commit)
		throw(SQL, "sql.trans", SQLSTATE(25001) "START TRANSACTION: cannot start a transaction within a transaction");
	if (sql->session->tr->active)
		msg = mvc_rollback(sql, 0, NULL, false);
	if (msg)
		return msg;
	switch (mvc_trans(sql)) {
		case -1:
			throw(SQL, "sql.trans", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		case -3:
			throw(SQL, "sql.trans", SQLSTATE(42000) "The session's schema was not found, this transaction won't start");
		default:
			break;
	}
	/* set transaction properties after successfully starting */
	sql->session->auto_commit = 0;
	sql->session->ac_on_commit = 1;
	return MAL_SUCCEED;
}
