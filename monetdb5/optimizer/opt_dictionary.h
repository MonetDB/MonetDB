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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _OPT_DICTIONARY_
#define _OPT_DICTIONARY_
#include "opt_prelude.h"
#include "opt_support.h"

opt_export str DICTinitialize(int *ret);
opt_export str DICTbind(int *idx, int *val, str * nme);
opt_export str DICTexpand(int *rval, int *val, int *bid);
opt_export str DICTencode(int *ridx, int *val, int *bid);
opt_export str DICTcompress(int *idx, str *nme, int *bid);
opt_export str DICTdecompress(int *ret, str *nme);
opt_export str DICTgroupid(int *ret, int *idx, int *val);
opt_export int OPTdictionaryImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#define OPTDEBUGdictionary  if ( optDebug & (1 <<DEBUG_OPT_DICTIONARY) )

#endif

