/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is the MonetDB Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
 */


#ifndef _OPT_DVF_
#define _OPT_DVF_

#ifdef WIN32
#ifndef LIBOPT_DVF
#define opt_dvf_export extern __declspec(dllimport)
#else
#define opt_dvf_export extern __declspec(dllexport)
#endif
#else
#define opt_dvf_export extern
#endif

#include "opt_prelude.h"
#include "opt_support.h"

/*@:exportOptimizer(dvf)@*/
opt_dvf_export str OPTdvf(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

//TODO: What are the following two lines?
/* #define DEBUG_OPT_DVF 61 
 # define OPTDEBUGdvf if (optDebug & ((lng)1 << DEBUG_OPT_DVF)) */
# define OPTDEBUGdvf if (optDebug)

#endif /* _OPT_DVF_ */

