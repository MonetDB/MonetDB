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
 * SQLGetDescRec()
 * CLI Compliance: ISO 92
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

SQLRETURN SQL_API
SQLGetDescRec(SQLHDESC DescriptorHandle, SQLSMALLINT RecordNumber,
	      SQLCHAR *Name, SQLSMALLINT BufferLength,
	      SQLSMALLINT *StringLength, SQLSMALLINT *Type,
	      SQLSMALLINT *SubType, SQLINTEGER *Length, SQLSMALLINT *Precision,
	      SQLSMALLINT *Scale, SQLSMALLINT *Nullable)
{
	ODBCDesc *desc = (ODBCDesc *) DescriptorHandle;
	ODBCDescRec *rec;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDescRec %d\n", RecordNumber);
#endif

	if (!isValidDesc(desc))
		return SQL_INVALID_HANDLE;

	if (RecordNumber <= 0) {
		addDescError(desc, "07009", NULL, 0);
		return SQL_ERROR;
	}
/*
	if (isIRD(desc) &&
	    (desc->Stmt->State == PREPARED || desc->Stmt->State == EXECUTED) && no open cursor)
		return SQL_NO_DATA;
*/
	if (isIRD(desc) && desc->Stmt->State != PREPARED &&
	    desc->Stmt->State != EXECUTED) {
		/* Associated statement is not prepared */
		addDescError(desc, "HY007", NULL, 0);
		return SQL_ERROR;
	}

	if (RecordNumber > desc->sql_desc_count)
		return SQL_NO_DATA;

	rec = &desc->descRec[RecordNumber];

	if (Type)
		*Type = rec->sql_desc_type;
	if (SubType)
		*SubType = rec->sql_desc_datetime_interval_code;
	if (Length)
		*Length = rec->sql_desc_octet_length;
	if (Precision)
		*Precision = rec->sql_desc_precision;
	if (Scale)
		*Scale = rec->sql_desc_scale;
	if (Nullable && isID(desc))
		*Nullable = rec->sql_desc_nullable;

	if (isID(desc)) {
		copyString(rec->sql_desc_name,
			   Name, BufferLength, StringLength,
			   addDescError, desc);
	}

	return desc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}
