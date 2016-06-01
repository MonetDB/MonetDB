
#ifndef _FWF_H_
#define _FWF_H_

/* #define _DEBUG_FWF_ */

#include "mal.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define fwf_export extern __declspec(dllimport)
#else
#define fwf_export extern __declspec(dllexport)
#endif
#else
#define fwf_export extern
#endif


fwf_export str fwf_load(mvc *m, char* schema, char* table, char* filename, BAT *widths, char padding);
fwf_export str fwf_load_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif
