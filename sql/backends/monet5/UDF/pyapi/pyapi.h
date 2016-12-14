
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * M.Raasveldt & H. Muehleisen
 * The Python interface
 */

#ifndef _PYPI_LIB_
#define _PYPI_LIB_

#include "pyheader.h"

pyapi_export str PyAPIevalStd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
pyapi_export str PyAPIevalAggr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
pyapi_export str PyAPIevalStdMap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
pyapi_export str PyAPIevalAggrMap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
pyapi_export str PyAPIevalLoader(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

pyapi_export str PyAPIprelude(void *ret);

int PyAPIEnabled(void);
int PyAPIInitialized(void);

str _loader_init(void);

pyapi_export char *PyError_CreateException(char *error_text, char *pycall);

#define pyapi_enableflag "embedded_py"

#endif /* _PYPI_LIB_ */
