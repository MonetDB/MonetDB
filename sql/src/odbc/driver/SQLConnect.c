/*
 * This code was created by Peter Harvey (mostly during Christmas 98/99).
 * This code is LGPL. Please ensure that this message remains in future
 * distributions and uses of this code (thats about all I get out of it).
 * - Peter Harvey pharvey@codebydesign.com
 * 
 * This file has been modified for the MonetDB project.  See the file
 * Copyright in this directory for more information.
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
#include "ini.h"		/* for __SQLGetPrivateProfileString() */

SQLRETURN
SQLConnect(SQLHDBC hDbc, SQLCHAR *szDataSource, SQLSMALLINT nDataSourceLength,
	   SQLCHAR *szUID, SQLSMALLINT nUIDLength, SQLCHAR *szPWD,
	   SQLSMALLINT nPWDLength)
{
	ODBCDbc *dbc = (ODBCDbc *) hDbc;
	SQLRETURN rc = SQL_SUCCESS;
	char *dsn = NULL;
	char *uid = NULL;
	char *pwd = NULL;
	char *database = NULL;
	char *db = NULL;
	char *schema = NULL;
	char *host = NULL;
	int port = 0;
	int debug = 0;
	int trace = 0;
	char buf[BUFSIZ + 1];

#ifdef WIN32
	char ODBC_INI[] = "ODBC.INI";
#else
	char ODBC_INI[] = "~/.odbc.ini";	/* name and place for the user DSNs */
#endif
	int socket_fd = 0;

	if (!isValidDbc(dbc))
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
	fixODBCstring(szDataSource, nDataSourceLength);
	if (nDataSourceLength == 0) {
		/* IM002 = Datasource not found */
		addDbcError(dbc, "IM002", NULL, 0);
		return SQL_ERROR;
	}
	dsn = dupODBCstring(szDataSource, nDataSourceLength);

	fixODBCstring(szUID, nUIDLength);
	if (nUIDLength == 0) {
		__SQLGetPrivateProfileString(dsn, "USER", "", buf, BUFSIZ,
					     ODBC_INI);
		uid = strdup(buf);
	} else {
		uid = dupODBCstring(szUID, nUIDLength);
	}
	fixODBCstring(szPWD, nPWDLength);
	if (nPWDLength == 0) {
		__SQLGetPrivateProfileString(dsn, "PASSWORD", "", buf, BUFSIZ,
					     ODBC_INI);
		pwd = strdup(buf);
	} else {
		pwd = dupODBCstring(szPWD, nPWDLength);
	}

	/* get the other information from the ODBC.INI file */
	__SQLGetPrivateProfileString(dsn, "DATABASE", "", buf, BUFSIZ,
				     ODBC_INI);
	database = strdup(buf);
	/* TODO: Provided database/schema are currently not used/implemented */
	__SQLGetPrivateProfileString(dsn, "HOST", "localhost", buf, BUFSIZ,
				     ODBC_INI);
	host = strdup(buf);
	__SQLGetPrivateProfileString(dsn, "PORT", "0", buf, BUFSIZ, ODBC_INI);
	port = atoi(buf);
	__SQLGetPrivateProfileString(dsn, "DEBUG", "0", buf, BUFSIZ, ODBC_INI);
	debug = atoi(buf);
	__SQLGetPrivateProfileString(dsn, "TRACE", "0", buf, BUFSIZ, ODBC_INI);
	trace = atoi(buf);

	/* Retrieved and checked the arguments.
	   Now try to open a connection with the server */
	fprintf(stderr, "SQLConnect %s %s %s %d\n", uid, database, host, port);
	/* connect to a server on host via port */
	socket_fd = client(host, port);
	if (socket_fd >= 0) {
		stream *rs = NULL;
		stream *ws = NULL;
		int chars_printed;
		char *login = NULL;

		rs = block_stream(socket_rstream(socket_fd,
						 "sql client read"));
		ws = block_stream(socket_wstream(socket_fd,
						 "sql client write"));
		assert(!stream_errnr(rs));
		assert(!stream_errnr(ws));

		/*
	 	 * client connect sequence
	 	 *
	 	 * 1) socket connect
	 	 * 2) send 'api(sql,debug,trace,reply_size)' api(sql,0,0,-1);
	 	 * 3) receive request for login 
	 	 * 4) send user,passwd
	   	 * 5) receive database,schema
	 	 */
		chars_printed = snprintf(buf, BUFSIZ, "api(sql,%d,%d,-1);\n",
					 debug, trace);
		stream_write(ws, buf, chars_printed, 1);
		stream_flush(ws);
		/* read login. The returned login value is not used yet. */
		login = readblock(rs);

		if (login)
			free(login);

		chars_printed = snprintf(buf, BUFSIZ, "login(%s,%s);\n",
					 uid, pwd);
		stream_write(ws, buf, chars_printed, 1);
		stream_flush(ws);
		/* read schema */
		db = (char *) readblock(rs);
		if (db) {
			char *s = strrchr(db, ',');

			if (s) {
				*s = '\0';
				schema = s + 1;
				s = strrchr(schema, '\n');
				if (s) {
					*s = '\0';
				}
				schema = strdup(schema);
			}
		}
		if (db != NULL) {
			free(db);
		}
		if (schema && *schema) {
			/* all went ok, store the connection info */
			dbc->socket = socket_fd;
			dbc->Mrs = rs;
			dbc->Mws = ws;
			dbc->Connected = 1;
			dbc->Mdebug = debug;
			/* TODO: if a database name is set, change the current schema to this database name */
		} else {
			/* 08001 = Client unable to establish connection */
			addDbcError(dbc, "08001",
				    "sockets not opened correctly", 0);
			rc = SQL_ERROR;
		}
	} else {
		/* 08001 = Client unable to establish connection */
		addDbcError(dbc, "08001", NULL, 0);
		rc = SQL_ERROR;
	}

	/* store internal information and clean up buffers */
	if (dbc->Connected == 1) {
		if (dbc->DSN != NULL)
			free(dbc->DSN);
		dbc->DSN = dsn;
		if (dbc->UID != NULL)
			free(dbc->UID);
		dbc->UID = uid;
		if (dbc->PWD != NULL)
			free(dbc->PWD);
		dbc->PWD = pwd;
		if (dbc->DBNAME != NULL)
			free(dbc->DBNAME);
		dbc->DBNAME = schema;

	} else {
		if (uid != NULL)
			free(uid);
		if (pwd != NULL)
			free(pwd);
		if (database != NULL)
			free(database);
	}
	/* free allocated but not stored strings */
	if (host != NULL)
		free(host);
	return rc;
}
