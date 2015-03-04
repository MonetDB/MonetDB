/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
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
