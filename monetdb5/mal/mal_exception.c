/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
/*15 */	"Deprecated operation",
/*EOE*/	NULL
};

int
isExceptionVariable(str nme){
	int i;
	if( nme)
		for(i=0; exceptionNames[i]; i++)
		if( strcmp(exceptionNames[i],nme)==0)
			return 1;
	return 0;
}

static char *M5OutOfMemory = MAL_MALLOC_FAIL;

/**
 * Internal helper function for createException and
 * showException such that they share the same code, because reuse
 * is good.
 */
static str createExceptionInternal(enum malexception type, const char *fcn, const char *format, va_list ap)
	__attribute__((__format__(__printf__, 3, 0)));
static str
createExceptionInternal(enum malexception type, const char *fcn, const char *format, va_list ap)
{
	char *message;
	int len;
	// if there is an error we allow memory allocation once again
#ifndef NDEBUG
	GDKsetmallocsuccesscount(-1);
#endif
	message = GDKmalloc(GDKMAXERRLEN);
	if (message == NULL)
		return M5OutOfMemory;	/* last resort */
	len = snprintf(message, GDKMAXERRLEN, "%s:%s:", exceptionNames[type], fcn);
	if (len >= GDKMAXERRLEN)	/* shouldn't happen */
		return message;
	len += vsnprintf(message + len, GDKMAXERRLEN - len, format, ap);
	/* realloc to reduce amount of allocated memory (GDKMAXERRLEN is
	 * way more than what is normally needed) */
	if (len < GDKMAXERRLEN) {
		/* in the extremely unlikely case that GDKrealloc fails, the
		 * original pointer is still valid, so use that and don't
		 * overwrite */
		char *newmsg = GDKrealloc(message, len + 1);
		if (newmsg != NULL)
			message = newmsg;
	}
	return message;
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
	str ret;

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
		ret = createException(type, fcn, SQLSTATE(HY001) MAL_MALLOC_FAIL ": %s", GDKerrbuf);
		GDKclrerr();
		return ret;
	}
	if (strcmp(format, GDK_EXCEPTION) == 0 && GDKerrbuf[0]) {
		/* for GDK errors, report the underlying error */
		char *p = GDKerrbuf;
		if (strncmp(p, GDKERROR, strlen(GDKERROR)) == 0)
			p += strlen(GDKERROR);
		if (strlen(p) > 6 && p[5] == '!')
			ret = createException(type, fcn, "%s", p);
		else
			ret = createException(type, fcn, "GDK reported error: %s", p);
		GDKclrerr();
		return ret;
	}
	va_start(ap, format);
	ret = createExceptionInternal(type, fcn, format, ap);
	va_end(ap);
	GDKclrerr();

	return ret;
}

void
freeException(str msg)
{
	if (msg != MAL_SUCCEED && msg != M5OutOfMemory)
		GDKfree(msg);
}

/**
 * Internal helper function to properly emit the given string to out,
 * thereby abiding to all the protocol laws.
 */
void
dumpExceptionsToStream(stream *out, str whatever) {
	size_t i;
	size_t last = 0;
	size_t len ;

	if (whatever == NULL)
		return;
	len = strlen(whatever);
	/* make sure each line starts with a ! */
	for (i = 0; i < len; i++) {
		if (whatever[i] == '\n') {
			whatever[i] = '\0';
			if (i - last > 0) { /* skip empty lines */
				if (whatever[last] == '!') /* no need for double ! */
					last++;
				mnstr_printf(out, "!%s\n", whatever + last);
			}
			last = i + 1;
		}
	}
	/* flush last part */
	if (i - last > 0) /* skip if empty */
		mnstr_printf(out, "!%s\n", whatever + last);
}

/**
 * Dump an error message using the exception structure 
 */
void
showException(stream *out, enum malexception type, const char *fcn, const char *format, ...)
{
	va_list ap;
	str msg;

	va_start(ap, format);
	msg = createExceptionInternal(type, fcn, format, ap);
	va_end(ap);

	dumpExceptionsToStream(out, msg);
	freeException(msg);
}

/**
 * Internal helper function for createMalException and
 * showScriptException such that they share the same code, because reuse
 * is good.
 */
static str
createMalExceptionInternal(MalBlkPtr mb, int pc, enum malexception type, char *prev, const char *format, va_list ap)
	__attribute__((__format__(__printf__, 5, 0)));
static str
createMalExceptionInternal(MalBlkPtr mb, int pc, enum malexception type, char *prev, const char *format, va_list ap)
{
	char buf[GDKMAXERRLEN];
	size_t i;
	str s, fcn;

	s = mb ? getModName(mb) : "unknown";
	fcn = mb ? getFcnName(mb) : "unknown";
	i = 0;

	if (prev){
		if( *prev){
			i += snprintf(buf + i, GDKMAXERRLEN - 1 - i, "%s", prev);
			if( buf[i-1] != '\n')
				buf[i++]= '\n';
		}
		i += snprintf(buf + i, GDKMAXERRLEN - 1 - i, "!%s:%s.%s[%d]:",
				exceptionNames[type], s, fcn, pc);
		freeException(prev);
	} else if( type == SYNTAX)
		i += snprintf(buf + i, GDKMAXERRLEN - 1 - i, "%s:",
				exceptionNames[type]);
	else
		i += snprintf(buf + i, GDKMAXERRLEN - 1 - i, "%s:%s.%s[%d]:",
				exceptionNames[type], s, fcn, pc);
	i += vsnprintf(buf + i, GDKMAXERRLEN - 1 - i, format, ap);
	if( buf[i-1] != '\n')
		buf[i++]= '\n';
	buf[i] = '\0';

	s = GDKstrdup(buf);
	if (s == NULL)				/* make sure we always return something */
		s = M5OutOfMemory;
	return s;
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
 * Sends the exception as generated by a call to
 * createMalException(mb, pc, type, NULL, format, ...) to a stream
 */
void
showScriptException(stream *out, MalBlkPtr mb, int pc, enum malexception type, const char *format, ...)
{
	va_list ap;
	str msg;

	va_start(ap, format);
	msg = createMalExceptionInternal(mb, pc, type, NULL, format, ap);
	va_end(ap);

	dumpExceptionsToStream(out,msg);
	freeException(msg);
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
				strncpy(ret, s, t - s);
				ret[t - s] = 0;
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
			if ((t = strchr(s, ':')) != NULL)
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
		((msg[0] >= '0' && msg[0] <= '9') ||
	     (msg[0] >= 'A' && msg[0] <= 'Z')) &&
	    ((msg[1] >= '0' && msg[1] <= '9') ||
	     (msg[1] >= 'A' && msg[1] <= 'Z')) &&
	    ((msg[2] >= '0' && msg[2] <= '9') ||
	     (msg[2] >= 'A' && msg[2] <= 'Z')) &&
	    ((msg[3] >= '0' && msg[3] <= '9') ||
	     (msg[3] >= 'A' && msg[3] <= 'Z')) &&
	    ((msg[4] >= '0' && msg[4] <= '9') ||
	     (msg[4] >= 'A' && msg[4] <= 'Z')))
		msg += 6;
	return msg;
}
