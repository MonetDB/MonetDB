/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

/*
 * (author) M Kersten, N Nes
 */
#ifndef _SQL_CATALOG_H
#define _SQL_CATALOG_H

#include "sql.h"
#include "mal_backend.h"
#include "sql_atom.h"
#include "sql_statement.h"
#include "sql_env.h"
#include "sql_mvc.h"
#include "mal_function.h"

extern str SQLcreate_seq(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLalter_seq(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLdrop_seq(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLcreate_schema(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLdrop_schema(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLcreate_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLcreate_view(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLdrop_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLdrop_view(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLdrop_constraint(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLalter_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLcreate_type(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLdrop_type(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLgrant_roles(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLrevoke_roles(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLgrant(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLrevoke(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLgrant_function(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLrevoke_function(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLcreate_user(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLdrop_user(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLalter_user(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLrename_user(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLcreate_role(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLdrop_role(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLdrop_index(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLdrop_function(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLcreate_function(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLcreate_trigger(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLdrop_trigger(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) ;
extern str SQLalter_add_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLalter_add_range_partition(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLalter_add_value_partition(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLalter_del_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLalter_set_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLcomment_on(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLrename_schema(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLrename_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str SQLrename_column(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* _SQL_CATALOG_H */

