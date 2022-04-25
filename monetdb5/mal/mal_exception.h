/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _MAL_EXCEPTION_H
#define _MAL_EXCEPTION_H
#include "mal_instruction.h"

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
	SQL,
	REMOTE
};

#define MAL_SUCCEED ((str) 0) /* no error */

#define throw \
	return createException
#define rethrow(FCN, TMP, PRV) \
	do { if ((TMP = (PRV)) != MAL_SUCCEED) return(TMP); } while(0)

#if !__has_attribute(__returns_nonnull__)
#define __returns_nonnull__
#endif

mal_export str createException(enum malexception, const char *,
	_In_z_ _Printf_format_string_ const char *, ...)
	__attribute__((__format__(__printf__, 3, 4)))
	__attribute__((__returns_nonnull__));
/*FIXmal_export str createMalException(MalBlkPtr mb, int pc, enum malexception type, const char *prev, const char *format, ...);*/
mal_export str createMalException(MalBlkPtr , int , enum malexception ,
	_In_z_ _Printf_format_string_ const char *, ...)
	__attribute__((__format__(__printf__, 4, 5)))
	__attribute__((__returns_nonnull__));
mal_export char *concatErrors(char *err1, const char *err2)
	__attribute__((__nonnull__(1))) __attribute__((__nonnull__(2)))
	__attribute__((__returns_nonnull__));
mal_export bool isExceptionVariable(const char *nme);

mal_export enum malexception	getExceptionType(const char *);
mal_export str	getExceptionPlace(const char *);
mal_export str	getExceptionMessageAndState(const char *);
mal_export str	getExceptionMessage(const char *);
mal_export void freeException(str);

#include "mal_errors.h"
#endif /*  _MAL_EXCEPTION_H*/
