/**********************************************************************
 * SQLDisconnect
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

SQLRETURN SQLDisconnect( SQLHDBC    hDrvDbc )
{
	HDRVDBC hDbc	= (HDRVDBC)hDrvDbc;

	/* SANITY CHECKS */

    if( NULL == hDbc )
        return SQL_INVALID_HANDLE;

	sprintf( hDbc->szSqlMsg, "hDbc = $%08X", hDbc );
    logPushMsg( hDbc->hLog, __FILE__, __FILE__, __LINE__, LOG_WARNING, LOG_WARNING, hDbc->szSqlMsg );

    if( hDbc->bConnected == 0 )
    {
        logPushMsg( hDbc->hLog, __FILE__, __FILE__, __LINE__, LOG_WARNING, LOG_WARNING, "SQL_SUCCESS_WITH_INFO Connection not open" );
        return SQL_SUCCESS_WITH_INFO;
    }

	if ( hDbc->hFirstStmt != SQL_NULL_HSTMT )
	{
		logPushMsg( hDbc->hLog, __FILE__, __FILE__, __LINE__, LOG_WARNING, LOG_WARNING, "SQL_ERROR Active Statements exist. Can not disconnect." );
		return SQL_ERROR;
	}

    /****************************
     * 1. do driver specific close here
     ****************************/
    { 
	int i, flag = 0;
	stream *rs = hDbc->hDbcExtras->rs;
      	context *sql = &hDbc->hDbcExtras->lc;
	stream *ws = sql->out;
	char buf[BUFSIZ];

	i = snprintf(buf, BUFSIZ, "s0 := mvc_commit(myc, 0, \"\");\n" );
	i += snprintf(buf+i, BUFSIZ-i, "result(Output, s0);\n" );
	ws->write( ws, buf, i, 1 );
	ws->flush( ws );

	if (rs->readInt(rs, &flag)){
		/* todo check if flag == COMM_DONE */
		printf("flag %d\n", flag );
	}

	/* client waves goodbye */
	buf[0] = EOT; 
	ws->write( ws, buf, 1, 1 );
	ws->flush( ws );

    	rs->close(rs);
    	rs->destroy(rs);
    }
    sql_exit_context( &hDbc->hDbcExtras->lc );
    hDbc->bConnected 		= 0;

    logPushMsg( hDbc->hLog, __FILE__, __FILE__, __LINE__, LOG_INFO, LOG_INFO, "SQL_SUCCESS" );

    return SQL_SUCCESS;
}


