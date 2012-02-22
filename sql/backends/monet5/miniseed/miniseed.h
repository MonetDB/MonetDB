#ifndef _MiniSEED_H
#define _MiniSEED_H

#include "clients.h"
#include "mal.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_function.h"
#include "mtime.h"
#include "libmseed.h"

#ifdef WIN32
#ifndef LIBMSEED
#define vault_export extern __declspec(dllimport)
#else
#define vault_export extern __declspec(dllexport)
#endif
#else
#define vault_export extern
#endif

vault_export str MiniseedMount(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#define _MSEED_DEBUG_

#endif /* _MSEED_H */