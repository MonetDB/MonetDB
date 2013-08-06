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

/*
 *  A. de Rijke
 * The rest_jsonstore module
 * The rest_jsonstore module contains all functions of the rest interface
 * of the jsonstore.
 */
#include "monetdb_config.h"

#include <sys/types.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <string.h>

#include <stdint.h>
#include <stdarg.h>

#include <uriparser/Uri.h>
#include <stdio.h>
#include "mal_http_daemon.h"
#include "rest_jsonstore.h"
#include "rest_jsonstore_handle_get.h"

str
RESTprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    register_http_handler((http_request_handler)&handle_http_request);
    (void) cntxt;
    (void) mb;
    (void) stk;
    (void) pci;		/* fool compiler */
    return MAL_SUCCEED;
}

static int
mserver_browser_get(const UriUriA uri) {
	int mserver_rest_command = 0;
	if (uri.absolutePath) {
		if (uri.pathHead != NULL) {
			if (uri.pathHead->next == NULL) {
			  if (strncmp(uri.pathHead->text.first, API_SPECIAL_CHAR, 1) == 0) {
					// This path element is on of the special cases
					mserver_rest_command = MONETDB_REST_UNKWOWN_SPECIAL;
					if (strcmp(uri.pathHead->text.first, MONETDB_REST_PATH_ALLDBS) == 0) {
						mserver_rest_command = MONETDB_REST_GET_ALLDBS;
						fprintf(stderr, "special url: %s\n", uri.pathHead->text.first);
					}
					if (strcmp(uri.pathHead->text.first, MONETDB_REST_PATH_UUIDS) == 0) {
						mserver_rest_command = MONETDB_REST_GET_ALLUUIDS;
						fprintf(stderr, "special url: %s\n", uri.pathHead->text.first);
					}
				} else {
					// This path element is a table name
					mserver_rest_command = MONETDB_REST_DB_INFO;
					fprintf(stderr, "url: %s\n", uri.pathHead->text.first);
				}
			} else {
				// We have multiple paths
			  if (strncmp(uri.pathHead->text.first, API_SPECIAL_CHAR, 1) == 0) {
					// This path element is on of the special cases
					mserver_rest_command = MONETDB_REST_UNKWOWN_SPECIAL;
					if (strcmp(uri.pathHead->text.first, MONETDB_REST_PATH_ALLDBS) == 0) {
						mserver_rest_command = MONETDB_REST_NO_PARAMETER_ALLOWED;
						fprintf(stderr, "special url: %s\n", uri.pathHead->text.first);
					}
					if (strcmp(uri.pathHead->text.first, MONETDB_REST_PATH_UUIDS) == 0) {
						mserver_rest_command = MONETDB_REST_NO_PARAMETER_ALLOWED;
						fprintf(stderr, "special url: %s\n", uri.pathHead->text.first);
					}
				} else {
					// The first path element is a table name
					// we cannot check this here, so we assume the table exists
					fprintf(stderr, "url: %s\n", uri.pathHead->text.first);
					if (strncmp(uri.pathTail->text.first, API_SPECIAL_CHAR, 1) == 0) {
						// This path element is on of the special cases
						mserver_rest_command = MONETDB_REST_UNKWOWN_SPECIAL;
						if (strcmp(uri.pathTail->text.first, MONETDB_REST_PATH_ALLDBS) == 0) {
							mserver_rest_command = MONETDB_REST_NO_PARAMETER_ALLOWED;
							fprintf(stderr, "special url: %s\n", uri.pathTail->text.first);
						}
						if (strcmp(uri.pathTail->text.first, MONETDB_REST_PATH_UUIDS) == 0) {
							mserver_rest_command = MONETDB_REST_NO_PARAMETER_ALLOWED;
							fprintf(stderr, "special url: %s\n", uri.pathTail->text.first);
						}
					} else {
						// The first path element is a table name
						// we cannot check this here, so we assume the table exists
						mserver_rest_command = MONETDB_REST_DB_INFO;
						fprintf(stderr, "url: %s\n", uri.pathHead->text.first);
					}
				}
			}
		} else {
			// A absolutePath with an empty pathHead means the root url
			mserver_rest_command = MONETDB_REST_WELCOME;
			fprintf(stderr, "url: %s\n", "/");
		}
	} else {
		// handle relative paths
	}
	return mserver_rest_command;
}

static int
mserver_browser_put(const UriUriA uri) {
	int mserver_rest_command = 0;
	if (uri.absolutePath) {
		if (uri.pathHead != NULL) {
			if (uri.pathHead->next == NULL) {
				if (strcmp(uri.pathHead->text.first, API_SPECIAL_CHAR) < 0) {
					// This path element is on of the special cases
					mserver_rest_command = MONETDB_REST_UNKWOWN_SPECIAL;
				} else {
					mserver_rest_command = MONETDB_REST_CREATE_DB;
					fprintf(stderr, "url: %s\n", uri.pathHead->text.first);
				}
			}
		} else {
			// A absolutePath with an empty pathHead means the root url
			// This is not allowed in a put message
			mserver_rest_command = MONETDB_REST_MISSING_DATABASENAME;
		}
	} else {
		// handle relative paths
	}
	return mserver_rest_command;
}

static int
mserver_browser_delete(const UriUriA uri) {
	int mserver_rest_command = 0;
	if (uri.absolutePath) {
		if (uri.pathHead != NULL) {
			if (uri.pathHead->next == NULL) {
				if (strcmp(uri.pathHead->text.first, API_SPECIAL_CHAR) < 0) {
					// This path element is on of the special cases
					mserver_rest_command = MONETDB_REST_UNKWOWN_SPECIAL;
				} else {
					mserver_rest_command = MONETDB_REST_DELETE_DB;
					fprintf(stderr, "url: %s\n", uri.pathHead->text.first);
				}
			}
		} else {
			// A absolutePath with an empty pathHead means the root url
			// This is not allowed in a put message
			mserver_rest_command = MONETDB_REST_MISSING_DATABASENAME;
		}
	} else {
		// handle relative paths
	}
	return mserver_rest_command;
}

static int
mserver_browser_post(const UriUriA uri) {
	int mserver_rest_command = 0;
	if (uri.absolutePath) {
		if (uri.pathHead != NULL) {
			if (uri.pathHead->next == NULL) {
				if (strcmp(uri.pathHead->text.first, API_SPECIAL_CHAR) < 0) {
					// This path element is on of the special cases
					mserver_rest_command = MONETDB_REST_UNKWOWN_SPECIAL;
				} else {
					mserver_rest_command = MONETDB_REST_POST_NEW_DOC;
					fprintf(stderr, "url: %s\n", uri.pathHead->text.first);
				}
			}
		} else {
			// A absolutePath with an empty pathHead means the root url
			// This is not allowed in a put message
			mserver_rest_command = MONETDB_REST_MISSING_DATABASENAME;
		}
	} else {
		// handle relative paths
	}
	return mserver_rest_command;
}

static
char * get_dbname(UriUriA uri) {
	int len;
	char * dbname;
	len = strlen(uri.pathHead->text.first);
	dbname = malloc(len + 1);
	strcpy(dbname, uri.pathHead->text.first);
	return dbname;
}

int
handle_http_request (const char *url, const char *method, char **page, 
					 char * postdata)
{
	int ret;
	int mserver_rest_command = 0;
	char * dbname = NULL;

	UriParserStateA state;
	UriUriA uri;

	state.uri = &uri;
	if (uriParseUriA(&state, url) != URI_SUCCESS) {
		/* Failure */
		printf("failed parse");
		uriFreeUriMembersA(&uri);
	}

	if ((strcmp(method, "GET")) == 0) {
		mserver_rest_command = mserver_browser_get(uri);
	} else if ((strcmp(method, "PUT")) == 0) {
		mserver_rest_command = mserver_browser_put(uri);
	} else if ((strcmp(method, "POST")) == 0) {
		mserver_rest_command = mserver_browser_post(uri);
	} else if ((strcmp(method, "DELETE")) == 0) {
		mserver_rest_command = mserver_browser_delete(uri);
	} else {
		// error
	}

	switch (mserver_rest_command) {
	case MONETDB_REST_WELCOME:
		RESTwelcome(page);
		break;
	case MONETDB_REST_GET_ALLDBS:
		RESTallDBs(page);
		break;
	case MONETDB_REST_CREATE_DB:
		dbname = get_dbname(uri);
		RESTcreateDB(page, dbname);
		break;
	case MONETDB_REST_DELETE_DB:
		dbname = get_dbname(uri);
		RESTdeleteDB(page, dbname);
		break;
	case  MONETDB_REST_GET_ALLUUIDS:
		RESTuuid(page);
		break;
	case  MONETDB_REST_POST_NEW_DOC:
		dbname = get_dbname(uri);
		RESTcreateDoc(page, dbname, postdata);
		break;
	case MONETDB_REST_DB_INFO:
		dbname = get_dbname(uri);
		RESTdbInfo(page, dbname);
		break;
	default:
		// error, unknown command
		ret = 1;
	}

	uriFreeUriMembersA(&uri);
	if (dbname != NULL) {
		free(dbname);
	}

	return ret;
}
