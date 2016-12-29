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
#ifndef _SQL_CATALOG_H
#define _SQL_CATALOG_H

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

sql5_export str SQLcreate_seq(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLalter_seq(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLdrop_seq(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLcreate_schema(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLdrop_schema(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLcreate_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLcreate_view(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLdrop_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLdrop_view(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLdrop_constraint(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLalter_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLcreate_type(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLdrop_type(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLgrant_roles(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLrevoke_roles(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLgrant(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLrevoke(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLgrant_function(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLrevoke_function(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLcreate_user(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLdrop_user(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLalter_user(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLrename_user(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLcreate_role(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLdrop_role(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLdrop_index(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLdrop_function(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLcreate_function(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLcreate_trigger(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLdrop_trigger(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
sql5_export str SQLalter_add_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLalter_del_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str SQLalter_set_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

sql5_export str UPGdrop_func(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str UPGcreate_func(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
sql5_export str UPGcreate_view(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);


#endif /* _SQL_CATALOG_H */

