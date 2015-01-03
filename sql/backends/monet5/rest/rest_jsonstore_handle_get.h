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
 * Copyright August 2008-2015 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _REST_JSONSTORE_HANDLE_GET_H_
#define _REST_JSONSTORE_HANDLE_GET_H_
#include "mal.h"
#include "mal_client.h"
#include <gdk.h>

str RESTunknown(char **result);
str RESTwelcome(char **result);
str RESTuuid(char **result);
str RESTallDBs(char **result);
str RESTcreateDB(char **result, char * dbname);
str RESTdeleteDB(char **result, char * dbname);
str RESTcreateDoc(char **result, char * dbname, const char * doc);
str RESTdbInfo(char **result, char * dbname);
str RESTgetDoc(char ** result, char * dbname, const char * doc_id);
str RESTupdateDoc(char **result, char * dbname, const char * doc, const char * doc_id);
str RESTdeleteDoc(char ** result, char * dbname, const char * doc_id);
str RESTerror(char **result, int rest_command);
str RESTinsertAttach(char ** result, char * dbname, const char * attachment, const char * doc_id);
str RESTgetAttach(char ** result, char * dbname, const char * doc_id);
str RESTdeleteAttach(char ** result, char * dbname, const char * doc_id);
str RESTinsertDesign(char ** result, char * dbname, const char * doc_id, const char * doc);
str RESTgetDesign(char ** result, char * dbname, const char * doc_id);

#endif
