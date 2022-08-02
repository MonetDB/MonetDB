/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
#include "opt_prelude.h"
#include "querylog.h"
#include "mal_builder.h"
#include "mal_debugger.h"

#include "rel_select.h"
#include "rel_optimizer.h"
#include "rel_prop.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_bin.h"
#include "rel_dump.h"
#include "rel_remote.h"
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
	if (sql->session->auto_commit == 1)
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

	if (sql->session->auto_commit == 1) {
		if (name)
			throw(SQL, "sql.trans", SQLSTATE(3BM30) "SAVEPOINT: not allowed in auto commit mode");
		else
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

	if (sql->session->auto_commit == 1)
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
	int ret = 0;

	initcontext();

	if (sql->session->auto_commit == 0)
		throw(SQL, "sql.trans", SQLSTATE(25001) "START TRANSACTION: cannot start a transaction within a transaction");
	if (sql->session->tr->active)
		msg = mvc_rollback(sql, 0, NULL, false);
	sql->session->auto_commit = 0;
	sql->session->ac_on_commit = 1;
	sql->session->level = chain;
	ret = mvc_trans(sql);
	if(msg)
		return msg;
	else if(ret < 0)
		throw(SQL, "sql.trans", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

str
SQLtransaction2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *sql = NULL;
	str msg;
	int ret = 0;

	(void) stk;
	(void) pci;

	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (sql->session->auto_commit == 0)
		throw(SQL, "sql.trans", SQLSTATE(25001) "START TRANSACTION: cannot start a transaction within a transaction");
	if (sql->session->tr->active)
		msg = mvc_rollback(sql, 0, NULL, false);
	sql->session->auto_commit = 0;
	sql->session->ac_on_commit = 1;
	sql->session->level = 0;
	ret = mvc_trans(sql);
	if(msg)
		return msg;
	else if(ret < 0)
		throw(SQL, "sql.trans", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}
