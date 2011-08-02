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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _MAL_EXCEPTION_H
#define _MAL_EXCEPTION_H
#include "mal_instruction.h"

/* #define _DEBUG_EXCEPTION_		trace the exception handling */

/* These are the exceptions known, adding new ones here requires to also
 * add the "full" name to the exceptionNames array below */
enum malexception {
	MAL=0,
	ILLARG,
	OUTOFBNDS,
	IO,
	INVCRED,
	OPTIMIZER,
	STKOF,
	SYNTAX,
	TYPE,
	LOADER,
	PARSE,
	ARITH,
	PERMD,
	SQL,
	RDF,
	XQUERY
};

#define MAL_SUCCEED ((str) 0) /* no error */

#define throw \
	return createException
#define rethrow(FCN, TMP, PRV) \
	if ((TMP = PRV) != MAL_SUCCEED) return(TMP);

mal_export str	createException(enum malexception, const char *,
	_In_z_ _Printf_format_string_ const char *, ...)
	__attribute__((__format__(__printf__, 3, 4)));
mal_export void	showException(enum malexception, const char *,
	_In_z_ _Printf_format_string_ const char *, ...)
	__attribute__((__format__(__printf__, 3, 4)));
mal_export str	createScriptException(MalBlkPtr, int, enum malexception, const char *,
	_In_z_ _Printf_format_string_ const char *, ...)
	__attribute__((__format__(__printf__, 5, 6)));
mal_export void	showScriptException(MalBlkPtr, int, enum malexception,
	_In_z_ _Printf_format_string_ const char *, ...)
	__attribute__((__format__(__printf__, 4, 5)));
mal_export int isExceptionVariable(str nme);

mal_export enum malexception	getExceptionType(str);
mal_export str	getExceptionPlace(str);
mal_export str	getExceptionMessage(str);
mal_export str	exceptionToString(enum malexception);
mal_export char *M5OutOfMemory;	/* pointer to constant string */

#include "mal_errors.h"
#endif /*  _MAL_EXCEPTION_H*/
