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
#include "stream.h"
#include "sql_scenario.h"
#include <mapi.h>
#include <rest_jsonstore_handle_get.h>

static str RESTsqlQuery(char **result, char * query);

static str
RESTsqlQuery(char **result, char * query)
{
	str msg = MAL_SUCCEED;
	str qmsg = MAL_SUCCEED;
	char * resultstring = NULL;
	struct buffer * resultbuffer;
	stream * oldstream;
	stream * resultstream;
	Client c;

	resultbuffer = buffer_create(BLOCK);
	resultstream = buffer_wastream(resultbuffer, "resultstring");

	c = mal_clients;
	oldstream = c->fdout;
	c->fdout = resultstream;
	msg = setScenario(c, "sql");
	qmsg = SQLstatementIntern(c, &query, "rest", TRUE, TRUE);

	resultstring = buffer_get_buf(resultbuffer);
	*result = GDKstrdup(resultstring);
	msg = setScenario(c, "mal");
	c->fdout = oldstream;
	free(resultstring);
	buffer_destroy(resultbuffer);

	if (qmsg != MAL_SUCCEED) {
		return qmsg;
	} else {
		return msg;
	}
}

str RESTwelcome(char **result)
{
	str msg = MAL_SUCCEED;
	char * querytext = "select '{ \"monetdb jsonstore\": \"Welcome\", \"version\":\"(unreleased)\" }';";

	msg = RESTsqlQuery(result, querytext);

	return msg;
}

str RESTallDBs(char **result)
{
	str msg = MAL_SUCCEED;
	char * querytext = "select name from tables where name like 'json_%';";

	msg = RESTsqlQuery(result, querytext);

	return msg;
}

str RESTuuid(char **result)
{
	str msg = MAL_SUCCEED;
	char * querytext = "select uuid();";

	msg = RESTsqlQuery(result, querytext);

	return msg;
}

str RESTcreateDB(char ** result, char * dbname)
{
	str msg = MAL_SUCCEED;
	str qmsg = MAL_SUCCEED;
	int len = strlen(dbname) + 45;
	char * committext = "commit;";
	char * rollbacktext = "rollback;";
	char * querytext = NULL;

	querytext = malloc(len);
	sprintf(querytext, "CREATE TABLE sys.json_%s (u uuid, r int, js json);", dbname);

	qmsg = RESTsqlQuery(result, querytext);
	if (qmsg == MAL_SUCCEED) {
		msg = RESTsqlQuery(result, committext);
	} else {
		msg = RESTsqlQuery(result, rollbacktext);
	}
	if (msg) {};
	if (querytext != NULL) {
		free(querytext);
	}
	return qmsg;
}

str RESTdeleteDB(char ** result, char * dbname)
{
	str msg = MAL_SUCCEED;
	str qmsg = MAL_SUCCEED;
	int len = strlen(dbname) + 23;
	char * committext = "commit;";
	char * rollbacktext = "rollback;";
	char * querytext = NULL;

	querytext = malloc(len);
	sprintf(querytext, "DROP TABLE json_%s;", dbname);

	qmsg = RESTsqlQuery(result, querytext);
	if (qmsg == MAL_SUCCEED) {
		msg = RESTsqlQuery(result, committext);
	} else {
		msg = RESTsqlQuery(result, rollbacktext);
	}
	if (msg) {};
	if (querytext != NULL) {
		free(querytext);
	}
	return qmsg;
}

str RESTcreateDoc(char ** result, char * dbname, const char * doc)
{
	str msg = MAL_SUCCEED;
	str qmsg = MAL_SUCCEED;
	int len = strlen(dbname) + strlen(doc)+ 52;
	char * committext = "commit;";
	char * rollbacktext = "rollback;";
	char * querytext = NULL;

	querytext = malloc(len);
	sprintf(querytext, "INSERT INTO json_%s (u, r, js) VALUES (uuid(), 1, '%s');", dbname, doc);

	qmsg = RESTsqlQuery(result, querytext);
	if (qmsg == MAL_SUCCEED) {
		msg = RESTsqlQuery(result, committext);
	} else {
		msg = RESTsqlQuery(result, rollbacktext);
	}
	if (msg) {};
	if (querytext != NULL) {
		free(querytext);
	}
	return qmsg;
}
