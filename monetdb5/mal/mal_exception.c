/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * (c) F. Groffen, M. Kersten
 * For documentation see website
 */
#include "monetdb_config.h"
#include "mal_exception.h"
#include "mal_private.h"

static char *exceptionNames[] = {
/* 0 */	"MALException",
/* 1 */	"IllegalArgumentException",
/* 2 */	"OutOfBoundsException",
/* 3 */	"IOException",
/* 4 */	"InvalidCredentialsException",
/* 5 */	"OptimizerException",
/* 6 */	"StackOverflowException",
/* 7 */	"SyntaxException",
/* 8 */	"TypeException",
/* 9 */	"LoaderException",
/*10 */	"ParseException",
/*11 */	"ArithmeticException",
/*12 */	"PermissionDeniedException",
/*13 */	"SQLException",
/*14 */	"Deprecated operation",
/*EOE*/	NULL
};

bool
isExceptionVariable(const char *nme)
{
	if (nme)
		for (int i = 0; exceptionNames[i]; i++)
			if (strcmp(exceptionNames[i], nme) == 0)
				return true;
	return false;
}

static char *M5OutOfMemory = MAL_MALLOC_FAIL;

char *
dupError(const char *err)
{
	char *msg = GDKstrdup(err);

	return msg ? msg : M5OutOfMemory;
}

char *
concatErrors(char *err1, const char *err2)
{
	size_t len = strlen(err1) + strlen(err2) + 1;
	char *new = GDKmalloc(len);
	if (new == NULL)
		return err1;
	strconcat_len(new, len, err1, err2, NULL);
	freeException(err1);
	return new;
}

/**
 * Internal helper function for createException and
 * showException such that they share the same code, because reuse
 * is good.
 */
static str __attribute__((__format__(__printf__, 3, 0), __returns_nonnull__))
createExceptionInternal(enum malexception type, const char *fcn, const char *format, va_list ap)
{
	size_t msglen;
	int len;
	char *msg;
	va_list ap2;
#ifndef NDEBUG
	// if there is an error we allow memory allocation once again
	GDKsetmallocsuccesscount(-1);
#endif
	va_copy(ap2, ap);			/* we need to use it twice */
	msglen = strlen(exceptionNames[type]) + strlen(fcn) + 2;
	len = vsnprintf(NULL, 0, format, ap); /* count necessary length */
	if (len < 0) {
		TRC_CRITICAL(MAL_SERVER, "called with bad arguments");
		len = 0;
	}
	msg = GDKmalloc(msglen + len + 1);
	if (msg != NULL) {
		/* the calls below succeed: the arguments have already been checked */
		(void) strconcat_len(msg, msglen + 1,
							 exceptionNames[type], ":", fcn, ":", NULL);
		if (len > 0)
			(void) vsnprintf(msg + msglen, len + 1, format, ap2);
		va_end(ap2);
		char *q = msg;
		for (char *p = strchr(msg, '\n'); p; q = p + 1, p = strchr(q, '\n'))
			TRC_ERROR(MAL_SERVER, "%.*s\n", (int) (p - q), q);
		if (*q)
			TRC_ERROR(MAL_SERVER, "%s\n", q);
	} else {
		msg = M5OutOfMemory;
	}
	va_end(ap2);

	assert(msg);
	return msg;
}

/**
 * Returns an exception string for the given type of exception, function
 * and additional formatting parameters.  This function will crash the
 * system or return bogus when the malexception enum is not aligned with
 * the exceptionNames array.
 */
str
createException(enum malexception type, const char *fcn, const char *format, ...)
{
	va_list ap;
	str ret = NULL;

	if (GDKerrbuf &&
		(ret = strstr(format, MAL_MALLOC_FAIL)) != NULL &&
		ret[strlen(MAL_MALLOC_FAIL)] != ':' &&
		(strncmp(GDKerrbuf, "GDKmalloc", 9) == 0 ||
		 strncmp(GDKerrbuf, "GDKrealloc", 10) == 0 ||
		 strncmp(GDKerrbuf, "GDKzalloc", 9) == 0 ||
		 strncmp(GDKerrbuf, "GDKstrdup", 9) == 0 ||
		 strncmp(GDKerrbuf, "allocating too much virtual address space", 41) == 0)) {
		/* override errors when the underlying error is memory
		 * exhaustion, but include whatever it is that the GDK level
		 * reported */
		ret = createException(type, fcn, SQLSTATE(HY013) MAL_MALLOC_FAIL ": %s", GDKerrbuf);
		GDKclrerr();
		assert(ret);
		return ret;
	}
	if (strcmp(format, GDK_EXCEPTION) == 0 && GDKerrbuf[0]) {
		/* for GDK errors, report the underlying error */
		char *p = GDKerrbuf;
		if (strncmp(p, GDKERROR, strlen(GDKERROR)) == 0) {
			/* error is "!ERROR: function_name: STATE!error message"
			 * we need to skip everything up to the STATE */
			p += strlen(GDKERROR);
			char *q = strchr(p, ':');
			if (q && q[1] == ' ' && strlen(q) > 8 && q[7] == '!')
				ret = createException(type, fcn, "%s", q + 2);
		}
		if (ret == NULL)
			ret = createException(type, fcn, "GDK reported error: %s", p);
		GDKclrerr();
		assert(ret);
		return ret;
	}
	va_start(ap, format);
	ret = createExceptionInternal(type, fcn, format, ap);
	va_end(ap);
	GDKclrerr();

	assert(ret);
	return ret;
}

void
freeException(str msg)
{
	if (msg != MAL_SUCCEED && msg != M5OutOfMemory)
		GDKfree(msg);
}

/**
 * Internal helper function for createMalException and
 * showScriptException such that they share the same code, because reuse
 * is good.
 */
static str __attribute__((__format__(__printf__, 5, 0), __returns_nonnull__))
createMalExceptionInternal(MalBlkPtr mb, int pc, enum malexception type, char *prev, const char *format, va_list ap)
{
	bool addnl = false;
	const char *s = mb && getInstrPtr(mb,0) ? getModName(mb) : "unknown";
	const char *fcn = mb && getInstrPtr(mb,0) ? getFcnName(mb) : "unknown";
	size_t msglen;

	if (prev) {
		msglen = strlen(prev);
		if (msglen > 0 && prev[msglen - 1] != '\n') {
			addnl = true;
			msglen++;
		}
		msglen += snprintf(NULL, 0, "!%s:%s.%s[%d]:",
						   exceptionNames[type], s, fcn, pc);
	} else if (type == SYNTAX) {
		msglen = strlen(exceptionNames[type]) + 1;
	} else {
		msglen = snprintf(NULL, 0, "%s:%s.%s[%d]:",
						  exceptionNames[type], s, fcn, pc);
	}
	va_list ap2;
	va_copy(ap2, ap);
	int len = vsnprintf(NULL, 0, format, ap);
	if (len < 0)
		len = 0;
	char *msg = GDKmalloc(msglen + len + 1);
	if (msg != NULL) {
		/* the calls below succeed: the arguments have already been checked */
		if (prev) {
			(void) snprintf(msg, msglen + 1, "%s%s!%s:%s.%s[%d]:",
							prev, addnl ? "\n" : "",
							exceptionNames[type], s, fcn, pc);
		} else if (type == SYNTAX) {
			(void) strconcat_len(msg, msglen + 1,
								 exceptionNames[type], ":", NULL);
		} else {
			(void) snprintf(msg, msglen + 1, "%s:%s.%s[%d]:",
							exceptionNames[type], s, fcn, pc);
		}
		if (len > 0)
			(void) vsnprintf(msg + msglen, len + 1, format, ap2);
	} else {
		msg = M5OutOfMemory;
	}
	va_end(ap2);
	freeException(prev);
	return msg;
}

/**
 * Returns an exception string for the MAL instructions.  These
 * exceptions are newline terminated, and determine module and function
 * from the given MalBlkPtr.  An old exception can be given, such that
 * this exception is chained to the previous one.  Conceptually this
 * creates a "stack" of exceptions.
 * This function will crash the system or return bogus when the
 * malexception enum is not aligned with the exceptionNames array.
 */
str
createMalException(MalBlkPtr mb, int pc, enum malexception type, const char *format, ...)
{
	va_list ap;
	str ret;

	va_start(ap, format);
	ret = createMalExceptionInternal(mb, pc, type, mb->errors, format, ap);
	va_end(ap);

	return(ret);
}

/**
 * Returns the malexception number for the given exception string.  If no
 * exception could be found in the string, MAL is returned indicating a
 * generic MALException.
 */
enum malexception
getExceptionType(const char *exception)
{
	enum malexception ret = MAL;
	const char *s;
	size_t len;
	enum malexception i;

	if ((s = strchr(exception, ':')) != NULL)
		len = s - exception;
	else
		len = strlen(exception);

	for (i = MAL; exceptionNames[i] != NULL; i++) {
		if (strncmp(exceptionNames[i], exception, len) == 0 &&
			exceptionNames[i][len] == '\0') {
			ret = i;
			break;
		}
	}

	return(ret);
}

/**
 * Returns the location the exception was raised, if known.  It
 * depends on how the exception was created, what the location looks
 * like.  The returned string is mallocced with GDKmalloc, and hence
 * needs to be GDKfreed.
 */
str
getExceptionPlace(const char *exception)
{
	str ret;
	const char *s, *t;
	enum malexception i;
	size_t l;

	for (i = MAL; exceptionNames[i] != NULL; i++) {
		l = strlen(exceptionNames[i]);
		if (strncmp(exceptionNames[i], exception, l) == 0 &&
			exception[l] == ':') {
			s = exception + l + 1;
			if ((t = strchr(s, ':')) != NULL) {
				if ((ret = GDKmalloc(t - s + 1)) == NULL)
					return NULL;
				strcpy_len(ret, s, t - s + 1);
				return ret;
			}
			break;
		}
	}
	return GDKstrdup("(unknown)");
}

/**
 * Returns the informational message of the exception given.
 */
str
getExceptionMessageAndState(const char *exception)
{
	const char *s, *t;
	enum malexception i;
	size_t l;

	for (i = MAL; exceptionNames[i] != NULL; i++) {
		l = strlen(exceptionNames[i]);
		if (strncmp(exceptionNames[i], exception, l) == 0 &&
			exception[l] == ':') {
			s = exception + l + 1;
			if ((t = strpbrk(s, ":\n")) != NULL && *t == ':')
				return (str) (t + 1);
			return (str) s;
		}
	}
	if (strncmp(exception, "!ERROR: ", 8) == 0)
		return (str) (exception + 8);
	return (str) exception;
}

str
getExceptionMessage(const char *exception)
{
	char *msg = getExceptionMessageAndState(exception);

	if (strlen(msg) > 6 && msg[5] == '!' &&
		(isdigit((unsigned char) msg[0]) ||
	     (msg[0] >= 'A' && msg[0] <= 'Z')) &&
	    (isdigit((unsigned char) msg[1]) ||
	     (msg[1] >= 'A' && msg[1] <= 'Z')) &&
	    (isdigit((unsigned char) msg[2]) ||
	     (msg[2] >= 'A' && msg[2] <= 'Z')) &&
	    (isdigit((unsigned char) msg[3]) ||
	     (msg[3] >= 'A' && msg[3] <= 'Z')) &&
	    (isdigit((unsigned char) msg[4]) ||
	     (msg[4] >= 'A' && msg[4] <= 'Z')))
		msg += 6;
	return msg;
}
