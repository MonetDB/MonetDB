#ifndef _REGISTRAR_H
#define _REGISTRAR_H

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

vault_export str register_repo(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
vault_export str register_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
//vault_export str mseed_register_fil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
//vault_export str mseed_register_cat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#define _MSEED_DEBUG_

#endif /* _MSEED_H */