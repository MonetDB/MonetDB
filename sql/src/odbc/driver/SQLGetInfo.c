/**********************************************************************
 * SQLGetInfo
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

SQLRETURN SQLGetInfo(
        SQLHDBC         hDrvDbc,
        SQLUSMALLINT    nInfoType,
        SQLPOINTER      pInfoValue,
        SQLSMALLINT     nInfoValueMax,
        SQLSMALLINT     *pnLength)
{
	int value, len = 0;
	HDRVDBC 	hDbc	= (HDRVDBC)hDrvDbc;

	return SQL_ERROR;

    	if( SQL_NULL_HDBC == hDbc )
		return SQL_INVALID_HANDLE;
	
    	switch(nInfoType){
    	case SQL_FETCH_DIRECTION: /* ODBC 1.0 */
		value = SQL_FD_FETCH_NEXT | SQL_FD_FETCH_FIRST |
			SQL_FD_FETCH_LAST | SQL_FD_FETCH_PRIOR |
			SQL_FD_FETCH_ABSOLUTE | SQL_FD_FETCH_RELATIVE |
			SQL_FD_FETCH_BOOKMARK;
		len = 4;
		break;
    	default:
    		return SQL_ERROR;
    	}
	if (len == 4)
		*((SQLINTEGER*)pInfoValue) = (SQLINTEGER)value;
	else if (len == 2)
		*((SQLSMALLINT*)pInfoValue) = (SQLSMALLINT)value;

	*pnLength = len;

	return SQL_SUCCESS;
}


