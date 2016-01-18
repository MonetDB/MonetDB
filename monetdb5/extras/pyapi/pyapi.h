
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

#include "monetdb_config.h"
#include "mal.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

#define PYAPI_TESTING
 
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
#define WARNING_MESSAGE(...) {           \
    if (option_warning) {                \
    fprintf(stderr, __VA_ARGS__);        \
    fflush(stdout);     }                \
}
#else
#define WARNING_MESSAGE(...) ((void) 0)
#endif

#ifdef WIN32
#ifndef LIBPYAPI
#define pyapi_export extern __declspec(dllimport)
#else
#define pyapi_export extern __declspec(dllexport)
#endif
#else
#define pyapi_export extern
#endif

pyapi_export str PyAPIevalStd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
pyapi_export str PyAPIevalAggr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
pyapi_export str PyAPIevalStdMap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
pyapi_export str PyAPIevalAggrMap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

pyapi_export str PyAPIprelude(void *ret);

int PyAPIEnabled(void);

#endif /* _PYPI_LIB_ */
