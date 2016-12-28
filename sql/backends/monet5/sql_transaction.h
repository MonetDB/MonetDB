/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * (author) M Kersten, N Nes
 */
#ifndef _SQL_TRANSACTION_H
#define _SQL_TRANSACTION_H

#ifdef WIN32
#ifndef LIBSQL
#define sql5_export extern __declspec(dllimport)
#else
#define sql5_export extern __declspec(dllexport)
#endif
#else
#define sql5_export extern
#endif

#include <sql.h>
#include <mal_backend.h>
#include <sql_atom.h>
#include <sql_statement.h>
#include <sql_env.h>
#include <sql_mvc.h>
#include <mal_function.h>

sql5_export str SQLtransaction_begin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLtransaction_release(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLtransaction_commit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLtransaction_abort(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLtransaction_rollback(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLtransaction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;

#endif /* _SQL_TRANSACTION_H */

