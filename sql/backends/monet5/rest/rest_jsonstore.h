/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#ifndef _REST_JSONSTORE_H_
#define _REST_JSONSTORE_H_
#include "mal.h"
#include "clients.h"

#ifdef WIN32
#ifndef LIBRESTJSONSTORE
#define rest_export extern __declspec(dllimport)
#else
#define rest_export extern __declspec(dllexport)
#endif
#else
#define rest_export extern
#endif

#define API_SPECIAL_CHAR "_"

#define MONETDB_REST_WELCOME 1
#define MONETDB_REST_GET_ALLDBS 2
#define MONETDB_REST_CREATE_DB 3
#define MONETDB_REST_DELETE_DB 4
#define MONETDB_REST_DB_INFO 5
#define MONETDB_REST_UNKWOWN_SPECIAL 6
#define MONETDB_REST_GET_ALLUUIDS 7
#define MONETDB_REST_NO_PARAMETER_ALLOWED 8
#define MONETDB_REST_MISSING_DATABASENAME 9
#define MONETDB_REST_POST_NEW_DOC 10
#define MONETDB_REST_DB_GETDOCID 11
#define MONETDB_REST_DB_UPDATE_DOC 12
#define MONETDB_REST_DB_DELETE_DOC 13
#define MONETDB_REST_INSERT_ATTACHMENT 14
#define MONETDB_REST_NO_ATTACHMENT_PATH 15
#define MONETDB_REST_INSERT_DESIGN 16
#define MONETDB_REST_GET_DESIGN 17

#define MONETDB_REST_ATTACHMENT "attachment"
#define MONETDB_REST_PATH_ALLDBS "_all_dbs"
#define MONETDB_REST_PATH_UUIDS "_uuids"
#define MONETDB_REST_PATH_DESIGN "_design"
#define MONETDB_REST_PATH_INFO "_info"
#define MONETDB_REST_PATH_VIEW "_view"
#define MONETDB_REST_PATH_SHOW "_show"
#define MONETDB_REST_PATH_LIST "_list"
#define MONETDB_REST_PATH_UPDATE "_update"
#define MONETDB_REST_PATH_REWRITE "_rewrite"

rest_export str RESTprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
rest_export int
handle_http_request (const char *url, const char *method, char **page, 
		     char * postdata);

#endif
