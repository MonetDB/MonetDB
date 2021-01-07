/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * M.Raasveldt & H. Muehleisen
 * The Python interface
 */

#ifndef _PYPI_LIB_
#define _PYPI_LIB_

#include "pyheader.h"

pyapi_export str PYAPI3PyAPIevalStd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
pyapi_export str PYAPI3PyAPIevalAggr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
pyapi_export str PYAPI3PyAPIevalStdMap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
pyapi_export str PYAPI3PyAPIevalAggrMap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
pyapi_export str PYAPI3PyAPIevalLoader(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

pyapi_export str PYAPI3PyAPIprelude(void *ret);

bool PYAPI3PyAPIInitialized(void);

str _loader_init(void);

pyapi_export char *PyError_CreateException(char *error_text, char *pycall);

pyapi_export bool option_disable_fork;

#endif /* _PYPI_LIB_ */
