/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _H_ODBCDESC
#define _H_ODBCDESC

#include "ODBCGlobal.h"
#include "ODBCError.h"
#include "ODBCDbc.h"

typedef struct {
	SQLINTEGER sql_desc_auto_unique_value;
	SQLCHAR *sql_desc_base_column_name;
	SQLCHAR *sql_desc_base_table_name;
	SQLINTEGER sql_desc_case_sensitive;
	SQLCHAR *sql_desc_catalog_name;
	SQLSMALLINT sql_desc_concise_type;
	SQLPOINTER sql_desc_data_ptr;
	SQLSMALLINT sql_desc_datetime_interval_code;
	SQLINTEGER sql_desc_datetime_interval_precision;
	SQLLEN sql_desc_display_size;
	SQLSMALLINT sql_desc_fixed_prec_scale;
	SQLLEN *sql_desc_indicator_ptr;
	SQLCHAR *sql_desc_label;
	SQLULEN sql_desc_length;
	SQLCHAR *sql_desc_literal_prefix;
	SQLCHAR *sql_desc_literal_suffix;
	SQLCHAR *sql_desc_local_type_name;
	SQLCHAR *sql_desc_name;
	SQLSMALLINT sql_desc_nullable;
	SQLINTEGER sql_desc_num_prec_radix;
	SQLLEN sql_desc_octet_length;
	SQLLEN *sql_desc_octet_length_ptr;
	SQLSMALLINT sql_desc_parameter_type;
	SQLSMALLINT sql_desc_precision;
	SQLSMALLINT sql_desc_rowver;
	SQLSMALLINT sql_desc_scale;
	SQLCHAR *sql_desc_schema_name;
	SQLSMALLINT sql_desc_searchable;
	SQLCHAR *sql_desc_table_name;
	SQLSMALLINT sql_desc_type;
	SQLCHAR *sql_desc_type_name;
	SQLSMALLINT sql_desc_unnamed;
	SQLSMALLINT sql_desc_unsigned;
	SQLSMALLINT sql_desc_updatable;
	size_t already_returned;
} ODBCDescRec;

typedef struct {
	int Type;
	ODBCError *Error;
	int RetrievedErrors;
	ODBCDbc *Dbc;
	struct tODBCDRIVERSTMT *Stmt;	/* associated statement for impl descr */

	ODBCDescRec *descRec;

	SQLSMALLINT sql_desc_alloc_type;
	SQLULEN sql_desc_array_size;
	SQLUSMALLINT *sql_desc_array_status_ptr;
	SQLLEN *sql_desc_bind_offset_ptr;
	SQLINTEGER sql_desc_bind_type;
	SQLSMALLINT sql_desc_count;
	SQLULEN *sql_desc_rows_processed_ptr;
} ODBCDesc;

#define isID(desc)	((desc)->Stmt != NULL)
#define isAD(desc)	((desc)->Stmt == NULL)
#define isIRD(desc)	(isID(desc) && (desc)->Stmt->ImplRowDescr == (desc))
#define isIPD(desc)	(isID(desc) && (desc)->Stmt->ImplParamDescr == (desc))

ODBCDesc *newODBCDesc(ODBCDbc *dbc);
int isValidDesc(ODBCDesc *desc);
void addDescError(ODBCDesc *desc, const char *SQLState, const char *errMsg, int nativeErrCode);
ODBCError *getDescError(ODBCDesc *desc);

#define clearDescErrors(desc) do {					\
				assert(desc);				\
				if ((desc)->Error) {			\
					deleteODBCErrorList(&(desc)->Error); \
					(desc)->RetrievedErrors = 0;	\
				}					\
			} while (0)
void destroyODBCDesc(ODBCDesc *desc);
void setODBCDescRecCount(ODBCDesc *desc, int count);
ODBCDescRec *addODBCDescRec(ODBCDesc *desc, SQLSMALLINT recno);

SQLULEN ODBCLength(ODBCDescRec *rec, int lengthtype);

SQLRETURN MNDBGetDescField(ODBCDesc *desc, SQLSMALLINT RecordNumber, SQLSMALLINT FieldIdentifier, SQLPOINTER Value, SQLINTEGER BufferLength, SQLINTEGER *StringLength);
SQLRETURN MNDBSetDescField(ODBCDesc *desc, SQLSMALLINT RecordNumber, SQLSMALLINT FieldIdentifier, SQLPOINTER Value, SQLINTEGER BufferLength);

#endif
