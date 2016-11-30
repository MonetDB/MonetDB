
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

#ifndef NDEBUG
// Enable verbose output, note that this #define must be set and mserver must be started with --set <verbose_enableflag>=true
#define _PYAPI_VERBOSE_
// Enable performance warnings, note that this #define must be set and mserver must be started with --set <warning_enableflag>=true
#define _PYAPI_WARNINGS_
// Enable debug mode, does literally nothing right now, but hey we have this nice #define here anyway
#define _PYAPI_DEBUG_
#endif

#ifdef _PYAPI_VERBOSE_
#define VERBOSE_MESSAGE(...) {              \
    if (option_verbose) {                   \
    printf(__VA_ARGS__);                    \
    fflush(stdout); }                       \
}
#else
#define VERBOSE_MESSAGE(...) ((void) 0)
#endif

#ifdef _PYAPI_WARNINGS_
extern bool option_warning;
#define WARNING_MESSAGE(...) {           \
    if (option_warning) {                \
    fprintf(stderr, __VA_ARGS__);        \
    fflush(stdout);     }                \
}
#else
#define WARNING_MESSAGE(...) ((void) 0)
#endif

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
