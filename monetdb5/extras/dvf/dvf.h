#ifndef _DVF_H
#define _DVF_H

#ifdef WIN32
#ifndef LIBDVF
#define dvf_export extern __declspec(dllimport)
#else
#define dvf_export extern __declspec(dllexport)
#endif
#else
#define dvf_export extern
#endif

#include "mal.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_function.h"
#include "opt_prelude.h"

dvf_export str plan_modifier(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

//TODO: What is the following line?
#define _DVF_DEBUG_

#endif /* _DVF_H */