/**********************************************************************
 * SQLConnect
 *
 **********************************************************************
 *
 * This code was created by Peter Harvey (mostly during Christmas 98/99).
 * This code is LGPL. Please ensure that this message remains in future
 * distributions and uses of this code (thats about all I get out of it).
 * - Peter Harvey pharvey@codebydesign.com
 *
 **********************************************************************/

#include "driver.h"


SQLRETURN SQLConnect(	SQLHDBC        hDrvDbc,
						SQLCHAR        *szDataSource,
						SQLSMALLINT    nDataSourceLength,
						SQLCHAR        *szUID,
						SQLSMALLINT    nUIDLength,
						SQLCHAR        *szPWD,
						SQLSMALLINT    nPWDLength
                          )
{
	char buf[BUFSIZ];
	int i, debug = 0, fd = 0;
	stream *ws = NULL;
	HDRVDBC 	hDbc	= (HDRVDBC)hDrvDbc;
    char    	szDATABASE[INI_MAX_PROPERTY_VALUE+1];
    char    	szUSER[INI_MAX_PROPERTY_VALUE+1];
    char    	szPASSWD[INI_MAX_PROPERTY_VALUE+1];
    char    	szHOST[INI_MAX_PROPERTY_VALUE+1];
    char    	szPORT[INI_MAX_PROPERTY_VALUE+1];
    char    	szFLAG[INI_MAX_PROPERTY_VALUE+1];
    context *lc;

    /* SANITY CHECKS */
    if( SQL_NULL_HDBC == hDbc )
		return SQL_INVALID_HANDLE;

	sprintf( hDbc->szSqlMsg, "hDbc=$%08lX 3zDataSource=(%s)", (long)hDbc, szDataSource );
	logPushMsg( hDbc->hLog, __FILE__, __FILE__, __LINE__, LOG_WARNING, LOG_WARNING, hDbc->szSqlMsg );

    if( hDbc->bConnected == 1 )
    {
		logPushMsg( hDbc->hLog, __FILE__, __FILE__, __LINE__, LOG_WARNING, LOG_WARNING, "SQL_ERROR Already connected" );
        return SQL_ERROR;
    }

    if ( nDataSourceLength == SQL_NTS )
    {
        if ( strlen( szDataSource ) > ODBC_FILENAME_MAX+INI_MAX_OBJECT_NAME )
        {
            logPushMsg( hDbc->hLog, __FILE__, __FILE__, __LINE__, LOG_WARNING, LOG_WARNING, "SQL_ERROR Given Data Source is too long. I consider it suspect." );
            return SQL_ERROR;
        }
    }
    else
    {
        if ( nDataSourceLength > ODBC_FILENAME_MAX+INI_MAX_OBJECT_NAME )
        {
            logPushMsg( hDbc->hLog, __FILE__, __FILE__, __LINE__, LOG_WARNING, LOG_WARNING, "SQL_ERROR Given Data Source is too long. I consider it suspect." );
            return SQL_ERROR;
        }
    }

    /********************
     * gather and use any required DSN properties
	 * - DATABASE
	 * - HOST (localhost assumed if not supplied)
     ********************/
    szDATABASE[0] 		= '\0';
    szUSER[0] 			= '\0';
    szPASSWD[0] 		= '\0';
    szHOST[0] 			= '\0';
    szPORT[0] 			= '\0';
    szFLAG[0] 			= '\0';
	SQLGetPrivateProfileString( szDataSource, "DATABASE", "", szDATABASE, sizeof(szDATABASE), "odbc.ini" );
	if ( szDATABASE[0] == '\0' )
	{
		sprintf( hDbc->szSqlMsg, "SQL_ERROR Could not find Driver entry for %s in system information", szDataSource );
		logPushMsg( hDbc->hLog, __FILE__, __FILE__, __LINE__, LOG_WARNING, LOG_WARNING, hDbc->szSqlMsg );
		return SQL_ERROR;
	}
	SQLGetPrivateProfileString( szDataSource, "USER", "monetdb", szUSER, sizeof(szUSER), "odbc.ini" );
	SQLGetPrivateProfileString( szDataSource, "PASSWORD", "", szPASSWD, sizeof(szPASSWD), "odbc.ini" );
	SQLGetPrivateProfileString( szDataSource, "HOST", "localhost", szHOST, sizeof(szHOST), "odbc.ini" );
	SQLGetPrivateProfileString( szDataSource, "PORT", "0", szPORT, sizeof(szPORT), "odbc.ini" );
	SQLGetPrivateProfileString( szDataSource, "FLAG", "0", szFLAG, sizeof(szFLAG), "odbc.ini" );
	

    /********************
     * 1. initialise structures
     * 2. try connection with database using your native calls
     * 3. store your server handle in the extras somewhere
     * 4. set connection state
     *      hDbc->bConnected = TRUE;
     ********************/

	/*
    mtrace();
    */

	debug = atoi(szFLAG);

	fd = client( szHOST, atoi(szPORT) );
	hDbc->hDbcExtras->fd = fd;
	hDbc->hDbcExtras->cursorinfo = SQL_FD_FETCH_NEXT;
	hDbc->hDbcExtras->rs = block_stream(
				socket_rstream( fd, "sql client read"));
	ws = block_stream( socket_wstream( fd, "sql client write")); 
	lc = &hDbc->hDbcExtras->lc;

	i = snprintf(buf, BUFSIZ, "info(\"%s\", %d, %d);\n", szUSER, debug, 1);
	ws->write( ws, buf, i, 1 );
	ws->flush( ws );

	i = snprintf(buf, BUFSIZ, "milsql();\n" );
	ws->write( ws, buf, i, 1 );
	ws->flush( ws );

	i = snprintf(buf, BUFSIZ, "myc := mvc_create(%d);\n", debug );
	ws->write( ws, buf, i, 1 );
	ws->flush( ws );

	i = snprintf(buf, BUFSIZ, "mvc_login(myc, \"%s\",\"%s\",\"%s\");\n",
				szDATABASE, szUSER, szPASSWD );
	ws->write( ws, buf, i, 1 );
	ws->flush( ws );

	memset(lc, 0, sizeof(context));
	sql_init_context( lc, ws, debug, default_catalog_create() );
	catalog_create_stream( hDbc->hDbcExtras->rs, lc );

	lc->cat->cc_getschema( lc->cat, szDATABASE, "monetdb" );

	if (hDbc->hDbcExtras->rs->errnr || lc->out->errnr){
		printf("sockets not opened correctly\n");
		exit(1);
	}

    hDbc->bConnected = TRUE;

    logPushMsg( hDbc->hLog, __FILE__, __FILE__, __LINE__, LOG_INFO, LOG_INFO, "SQL_SUCCESS" );

    return SQL_SUCCESS;
}


