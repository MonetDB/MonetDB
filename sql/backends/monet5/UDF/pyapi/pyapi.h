/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * M.Raasveldt & H. Muehleisen
 * The Python interface
 */

#ifndef _PYPI_LIB_
#define _PYPI_LIB_

#include "pyheader.h"

pyapi_export str PYFUNCNAME(PyAPIevalStd)(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
pyapi_export str PYFUNCNAME(PyAPIevalAggr)(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
pyapi_export str PYFUNCNAME(PyAPIevalStdMap)(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
pyapi_export str PYFUNCNAME(PyAPIevalAggrMap)(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
pyapi_export str PYFUNCNAME(PyAPIevalLoader)(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

pyapi_export str PYFUNCNAME(PyAPIprelude)(void *ret);

int PYFUNCNAME(PyAPIInitialized)(void);

str _loader_init(void);

pyapi_export char *PyError_CreateException(char *error_text, char *pycall);

#endif /* _PYPI_LIB_ */
