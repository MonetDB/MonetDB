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

static SQLRETURN
SQLGetDescRec_(ODBCDesc *desc, SQLSMALLINT RecordNumber, SQLCHAR *Name,
	       SQLSMALLINT BufferLength, SQLSMALLINT *StringLength,
	       SQLSMALLINT *Type, SQLSMALLINT *SubType, SQLINTEGER *Length,
	       SQLSMALLINT *Precision, SQLSMALLINT *Scale,
	       SQLSMALLINT *Nullable)
{
	ODBCDescRec *rec;

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

SQLRETURN SQL_API
SQLGetDescRec(SQLHDESC DescriptorHandle, SQLSMALLINT RecordNumber,
	      SQLCHAR *Name, SQLSMALLINT BufferLength,
	      SQLSMALLINT *StringLength, SQLSMALLINT *Type,
	      SQLSMALLINT *SubType, SQLINTEGER *Length, SQLSMALLINT *Precision,
	      SQLSMALLINT *Scale, SQLSMALLINT *Nullable)
{
	ODBCDesc *desc = (ODBCDesc *) DescriptorHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDescRec " PTRFMT " %d\n",
		PTRFMTCAST DescriptorHandle, RecordNumber);
#endif

	if (!isValidDesc(desc))
		return SQL_INVALID_HANDLE;

	return SQLGetDescRec_(desc, RecordNumber, Name, BufferLength,
			      StringLength, Type, SubType, Length, Precision,
			      Scale, Nullable);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLGetDescRecW(SQLHDESC DescriptorHandle, SQLSMALLINT RecordNumber,
	       SQLWCHAR *Name, SQLSMALLINT BufferLength,
	       SQLSMALLINT *StringLength, SQLSMALLINT *Type,
	       SQLSMALLINT *SubType, SQLINTEGER *Length,
	       SQLSMALLINT *Precision, SQLSMALLINT *Scale,
	       SQLSMALLINT *Nullable)
{
	ODBCDesc *desc = (ODBCDesc *) DescriptorHandle;
	SQLRETURN rc;
	SQLCHAR *name;
	SQLSMALLINT n;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDescRecW " PTRFMT " %d\n",
		PTRFMTCAST DescriptorHandle, RecordNumber);
#endif

	if (!isValidDesc(desc))
		return SQL_INVALID_HANDLE;

	name = (SQLCHAR *) malloc(BufferLength * 4);

	rc = SQLGetDescRec_(desc, RecordNumber, name, BufferLength, &n, Type,
			    SubType, Length, Precision, Scale, Nullable);

	if (SQL_SUCCEEDED(rc)) {
		char *e = ODBCutf82wchar(name, n, Name, BufferLength, &n);
		if (e)
			rc = SQL_ERROR;
		if (StringLength)
			*StringLength = n;
	}
	free(name);

	return rc;
}
#endif	/* WITH_WCHAR */
