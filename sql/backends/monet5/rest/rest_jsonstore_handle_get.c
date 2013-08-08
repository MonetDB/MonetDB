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

#include "monetdb_config.h"
#include <stdio.h>
#include "mal_mapi.h"
#include "mal_client.h"
#include "mal_linker.h"
#include "stream.h"
#include "sql_scenario.h"
#include <mapi.h>
#include <rest_jsonstore_handle_get.h>
#include "mal_backend.h"

static str RESTsqlQuery(char **result, char * query);
char * result_ok = "select true as ok;";

static str
RESTsqlQuery(char **result, char * query)
{
	str msg = MAL_SUCCEED;
	str qmsg = MAL_SUCCEED;
	char * resultstring = NULL;
	struct buffer * resultbuffer;
	stream * resultstream;
	Client c;
	bstream *fin = NULL;
	int len = 0;
	backend *be;

	resultbuffer = buffer_create(BLOCK);
	resultstream = buffer_wastream(resultbuffer, "resultstring");

	c = MCinitClient(CONSOLE, fin, resultstream);
	c->nspace = newModule(NULL, putName("user", 4));

	// TODO: lookup user_id in bat
	c->user = 1;
	initLibraries();
	msg = setScenario(c, "sql");
	msg = SQLinitClient(c);
	MSinitClientPrg(c, "user", "main");
	(void) MCinitClientThread(c);
	be = (backend*)c->sqlcontext;
	be->output_format = OFMT_JSON;

	qmsg = SQLstatementIntern(c, &query, "rest", TRUE, TRUE);
	if (qmsg == MAL_SUCCEED) {
		resultstring = buffer_get_buf(resultbuffer);
		*result = GDKstrdup(resultstring);
		free(resultstring);
	} else {
		len = strlen(qmsg) + 19;
		resultstring = malloc(len);
		snprintf(resultstring, len, "{ \"error\": \"%s\" }\n", qmsg);
		*result = GDKstrdup(resultstring);
		free(resultstring);
	}
	buffer_destroy(resultbuffer);
	msg = SQLexitClient(c);
	return msg;
}

str RESTwelcome(char **result)
{
	str msg = MAL_SUCCEED;
	// TODO: get version from variable
	char * querytext = "select 'Welcome' as jsonstore, '(unreleased)' as version;";
	msg = RESTsqlQuery(result, querytext);
	return msg;
}

str RESTallDBs(char **result)
{
	str msg = MAL_SUCCEED;
	char * querytext = "select substring(name, 6, length(name) -5) from tables where name like 'json_%';";
	msg = RESTsqlQuery(result, querytext);
	return msg;
}

str RESTuuid(char **result)
{
	str msg = MAL_SUCCEED;
	char * querytext = "select uuid() as uuid;";
	msg = RESTsqlQuery(result, querytext);
	return msg;
}

str RESTcreateDB(char ** result, char * dbname)
{
	str msg = MAL_SUCCEED;
	int len = strlen(dbname) + 58;
	char * querytext = NULL;

	querytext = malloc(len);
	snprintf(querytext, len, "CREATE TABLE json_%s (_id uuid, _rev VARCHAR(34), js json);", dbname);

	msg = RESTsqlQuery(result, querytext);
	if (querytext != NULL) {
		free(querytext);
	}
	if (strcmp(*result,"") == 0) {
	  msg = RESTsqlQuery(result, result_ok);
	}
	return msg;
}

str RESTdeleteDB(char ** result, char * dbname)
{
	str msg = MAL_SUCCEED;
	int len = strlen(dbname) + 23;
	char * querytext = NULL;

	querytext = malloc(len);
	snprintf(querytext, len, "DROP TABLE json_%s;", dbname);

	msg = RESTsqlQuery(result, querytext);
	if (querytext != NULL) {
		free(querytext);
	}
	if (strcmp(*result,"") == 0) {
	  msg = RESTsqlQuery(result, result_ok);
	}
	return msg;
}

str RESTcreateDoc(char ** result, char * dbname, const char * doc)
{
	str msg = MAL_SUCCEED;
	int len = strlen(dbname) + 32 + strlen(doc)+ 72;
	char * querytext = NULL;

	querytext = malloc(len);
	snprintf(querytext, len, "INSERT INTO json_%s (_id, _rev, js) VALUES (uuid(), concat('1-', md5('%s')), '%s');", dbname, doc, doc);

	msg = RESTsqlQuery(result, querytext);
	if (querytext != NULL) {
		free(querytext);
	}
	if (strcmp(*result,"&2 1 -1\n") == 0) {
	  msg = RESTsqlQuery(result, result_ok);
	}
	return msg;
}

str RESTdbInfo(char **result, char * dbname)
{
	str msg = MAL_SUCCEED;
	int len = strlen(dbname) + 21;
	char * querytext = NULL;

	querytext = malloc(len);
	snprintf(querytext, len, "SELECT * FROM json_%s;", dbname);

	msg = RESTsqlQuery(result, querytext);
	if (querytext != NULL) {
		free(querytext);
	}
	return msg;
}
