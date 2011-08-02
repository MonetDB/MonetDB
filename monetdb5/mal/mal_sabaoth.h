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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */


#ifndef _MAL_SABAOTH_DEF
#define _MAL_SABAOTH_DEF

#include <mal.h>
#include <mal_exception.h>
#include <msabaoth.h>
mal_export void SABAOTHinit(str dbfarm, str dbname);
mal_export str SABAOTHgetDBfarm(str *ret);
mal_export str SABAOTHgetDBname(str *ret);
mal_export str SABAOTHmarchScenario(int *ret, str *lang);
mal_export str SABAOTHretreatScenario(int *ret, str *lang);
mal_export str SABAOTHmarchConnection(int *ret, str *host, int *port);
mal_export str SABAOTHgetLocalConnection(str *ret);
mal_export str SABAOTHwildRetreat(int *ret);
mal_export str SABAOTHregisterStart(int *ret);
mal_export str SABAOTHregisterStop(int *ret);
mal_export str SABAOTHgetMyStatus(sabdb** ret);
mal_export str SABAOTHgetStatus(sabdb** ret, str dbname);
mal_export str SABAOTHfreeStatus(sabdb** ret);
mal_export str SABAOTHgetUplogInfo(sabuplog *ret, sabdb *db);
mal_export str SABAOTHserialise(str *ret, sabdb *db);
mal_export str SABAOTHdeserialise(sabdb **ret, str *sabdb);
#endif
