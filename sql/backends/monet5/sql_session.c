/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/* (c) M.L. Kersten
 * The SQL session parameters can be pre-tested
*/
#include "monetdb_config.h"
#include "mal_client.h"
#include "mal_instruction.h"
#include "clients.h"
#include "opt_pipes.h"
#include "sql_scenario.h"
#include "sql_session.h"

str
SQLsetoptimizer(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str opt;

	if( pci->argc == 3){
		opt = *getArgReference_str(stk,pci,2);
	} else {
		opt = *getArgReference_str(stk,pci,1);
	}
	if( ! isOptimizerPipe(opt))
		throw(SQL, "sqloptimizer", "Valid optimizer pipe expected");

	return CLTsetoptimizer(cntxt, mb, stk, pci);
}

str
SQLsetworkerlimit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	// We might restrict this based on DBA limitations set
	return CLTsetworkerlimit(cntxt, mb, stk, pci);
}

str
SQLsetmemorylimit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	// We might restrict this based on DBA limitations set
	return CLTsetmemorylimit(cntxt, mb, stk, pci);
}

str
SQLqueryTimeout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	// We might restrict this based on DBA limitations set
	return CLTqueryTimeout(cntxt, mb, stk, pci);
}

str
SQLsessionTimeout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	// We might restrict this based on DBA limitations set
	return CLTsessionTimeout(cntxt, mb, stk, pci);
}
