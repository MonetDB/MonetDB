/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at 
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 * 
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 * 
 * The Original Code is the Monet Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2002 CWI.  
 * All Rights Reserved.
 * 
 * Contributor(s):
 * 		Martin Kersten <Martin.Kersten@cwi.nl>
 * 		Peter Boncz <Peter.Boncz@cwi.nl>
 * 		Niels Nes <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
 */

/**********************************************************************
 * SQLConnect()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"
#include "ODBCUtil.h"

SQLRETURN SQLConnect(
	SQLHDBC		hDbc,
	SQLCHAR *	szDataSource,
	SQLSMALLINT	nDataSourceLength,
	SQLCHAR *	szUID,
	SQLSMALLINT	nUIDLength,
	SQLCHAR *	szPWD,
	SQLSMALLINT	nPWDLength )
{
	ODBCDbc * dbc = (ODBCDbc *) hDbc;
	SQLRETURN rc = SQL_SUCCESS;
	char * dsn = NULL;
	char * uid = NULL;
	char * pwd = NULL;
	char * database = NULL;
	char * db = NULL;
	char * schema = NULL;
	char * host = NULL;
	int port = 0;
	int debug = 0;
	char buf[BUFSIZ + 1];
#ifdef WIN32
	char ODBC_INI[] = "ODBC.INI";
#else
	char ODBC_INI[] = "~/.odbc.ini";	/* name and place for the user DSNs */
#endif
	int socket_fd = 0;

	if (! isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	/* check connection state, should not be connected */
	if (dbc->Connected == 1) {
		/* 08002 = Connection already in use */
		addDbcError(dbc, "08002", NULL, 0);
		return SQL_ERROR;
	}
	assert(dbc->Connected == 0);

	/* convert input string parameters to normal null terminated C strings */
	dsn = copyODBCstr2Cstr(szDataSource, nDataSourceLength);
	if (dsn == NULL || strlen(dsn) == 0) {
		/* IM002 = Datasource not found */
		addDbcError(dbc, "IM002", NULL, 0);
		return SQL_ERROR;
	}

	uid = copyODBCstr2Cstr(szUID, nUIDLength);
	if (uid == NULL || strlen(uid) == 0) {
		SQLGetPrivateProfileString(dsn, "USER", "", buf, BUFSIZ, ODBC_INI);
		uid = GDKstrdup(buf);
	}
	pwd = copyODBCstr2Cstr(szPWD, nPWDLength);
	if (uid == NULL || strlen(pwd) == 0) {
		SQLGetPrivateProfileString(dsn, "PASSWORD", "", buf, BUFSIZ, ODBC_INI);
		pwd = GDKstrdup(buf);
	}

	/* get the other information from the ODBC.INI file */
	SQLGetPrivateProfileString(dsn, "DATABASE", "", buf, BUFSIZ, ODBC_INI);
	database = GDKstrdup(buf);
	/* TODO: Provided database/schema are currently not used/implemented */
	SQLGetPrivateProfileString(dsn, "HOST", "localhost", buf, BUFSIZ, ODBC_INI);
	host = GDKstrdup(buf);
/*	host = GDKstrdup("localhost"); */
	SQLGetPrivateProfileString(dsn, "PORT", "0", buf, BUFSIZ, ODBC_INI);
	port = atoi(buf);
/*	port = 45123; */
	SQLGetPrivateProfileString(dsn, "FLAG", "0", buf, BUFSIZ, ODBC_INI);
	debug = atoi(buf);


	/* Retrieved and checked the arguments.
	   Now try to open a connection with the server */
	/* connect to a server on host via port */
	socket_fd = client(host, port);
	if (socket_fd > 0) {
		stream * rs = NULL;
		stream * ws = NULL;
		int  chars_printed;
		char * login = NULL;

		rs = block_stream(socket_rstream(socket_fd, "sql client read"));
		ws = block_stream(socket_wstream(socket_fd, "sql client write"));

		chars_printed = snprintf(buf, BUFSIZ, "api(sql,%d);\n", debug );
		ws->write(ws, buf, chars_printed, 1);
		ws->flush(ws);
		/* read login. The returned login value is not used yet. */
		login = (char *)readblock(rs);

		if (login) free(login);

		chars_printed = snprintf(buf, BUFSIZ, "login(%s,%s);\n", uid, pwd);
		ws->write(ws, buf, chars_printed, 1);
		ws->flush(ws);
		/* read schema */
		db = (char *)readblock(rs);
		if (db) {
			char *s = strrchr(db, ',');
			if (s){ 
				*s = '\0';
				schema = s+1;
				s = strrchr(schema, '\n');
				if (s){ 
					*s = '\0';
				}
			}
		}
		if (schema && strlen(schema) > 0) {
			/* all went ok, store the connection info */
			dbc->socket = socket_fd;
			dbc->Mrs = rs;
			dbc->Mws = ws;
			dbc->Connected = 1;
			dbc->Mdebug = debug;
			/* TODO: if a database name is set, change the current schema to this database name */
		} else {
			/* 08001 = Client unable to establish connection */
			addDbcError(dbc, "08001", "sockets not opened correctly", 0);
			rc = SQL_ERROR;
		}
	} else {
		/* 08001 = Client unable to establish connection */
		addDbcError(dbc, "08001", NULL, 0);
		rc = SQL_ERROR;
	}

	/* store internal information and clean up buffers */
	if (dbc->Connected == 1) {
		if (dbc->DSN != NULL) {
			GDKfree(dbc->DSN);
			dbc->DSN = NULL;
		}
		dbc->DSN = dsn;
		if (dbc->UID != NULL) {
			GDKfree(dbc->UID);
			dbc->UID = NULL;
		}
		dbc->UID = uid;
		if (dbc->PWD != NULL) {
			GDKfree(dbc->PWD);
			dbc->PWD = NULL;
		}
		dbc->PWD = pwd;
		if (dbc->DBNAME != NULL) {
			GDKfree(dbc->DBNAME);
			dbc->DBNAME = NULL;
		}
		dbc->DBNAME = schema;

	} else {
		if (uid != NULL) {
			GDKfree(uid);
		}
		if (pwd != NULL) {
			GDKfree(pwd);
		}
		if (database != NULL) {
			GDKfree(database);
		}
	}
	/* free allocated but not stored strings */
	if (host != NULL) {
		GDKfree(host);
	}

	return rc;
}
