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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _REST_JSONSTORE_HANDLE_GET_H_
#define _REST_JSONSTORE_HANDLE_GET_H_
#include "mal.h"
#include "mal_client.h"
#include <gdk.h>

#ifdef WIN32
#ifndef LIBRESTJSONSTORE
#define rest_export extern __declspec(dllimport)
#else
#define rest_export extern __declspec(dllexport)
#endif
#else
#define rest_export extern
#endif

rest_export str RESTwelcome(char **result);
rest_export str RESTuuid(char **result);
rest_export str RESTallDBs(char **result);
rest_export str RESTcreateDB(char **result, char * dbname);
rest_export str RESTdeleteDB(char **result, char * dbname);
rest_export str RESTcreateDoc(char **result, char * dbname, const char * doc);
rest_export str RESTdbInfo(char **result, char * dbname);

#endif
