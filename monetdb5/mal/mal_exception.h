/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _MAL_EXCEPTION_H
#define _MAL_EXCEPTION_H
#include "mal_instruction.h"

/* #define _DEBUG_EXCEPTION_		trace the exception handling */

/* These are the exceptions known, adding new ones here requires to also
 * add the "full" name to the exceptionNames array in mal_exception.c */
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
	SQL
};

#define MAL_SUCCEED ((str) 0) /* no error */

#define throw \
	return createException
#define rethrow(FCN, TMP, PRV) \
	{if ((TMP = PRV) != MAL_SUCCEED) return(TMP);}

mal_export str	createException(enum malexception, const char *,
	_In_z_ _Printf_format_string_ const char *, ...)
	__attribute__((__format__(__printf__, 3, 4)));
/*FIXmal_export str createMalException(MalBlkPtr mb, int pc, enum malexception type, const char *prev, const char *format, ...);*/
mal_export str createMalException(MalBlkPtr , int , enum malexception , 
	_In_z_ _Printf_format_string_ const char *, ...)
	__attribute__((__format__(__printf__, 4, 5)));
mal_export void	showException(stream *out, enum malexception, const char *,
	_In_z_ _Printf_format_string_ const char *, ...)
	__attribute__((__format__(__printf__, 4, 5)));
mal_export void	showScriptException(stream *out, MalBlkPtr, int, enum malexception,
	_In_z_ _Printf_format_string_ const char *, ...)
	__attribute__((__format__(__printf__, 5, 6)));
mal_export int isExceptionVariable(str nme);

mal_export enum malexception	getExceptionType(const char *);
mal_export str	getExceptionPlace(const char *);
mal_export str	getExceptionMessageAndState(const char *);
mal_export str	getExceptionMessage(const char *);
mal_export void dumpExceptionsToStream(stream *out, str msg);
mal_export void freeException(str);

#include "mal_errors.h"
#endif /*  _MAL_EXCEPTION_H*/
