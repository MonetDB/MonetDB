/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/*
 * (c) F. Groffen, M. Kersten
 * For documentation see website
 */
#include "monetdb_config.h"
#include "mal_exception.h"
#include "mal_private.h"

static const char *exceptionNames[] = {
/* 0 */ "MALException",
/* 1 */ "IllegalArgumentException",
/* 2 */ "OutOfBoundsException",
/* 3 */ "IOException",
/* 4 */ "InvalidCredentialsException",
/* 5 */ "OptimizerException",
/* 6 */ "StackOverflowException",
/* 7 */ "SyntaxException",
/* 8 */ "TypeException",
/* 9 */ "LoaderException",
/*10 */ "ParseException",
/*11 */ "ArithmeticException",
/*12 */ "PermissionDeniedException",
/*13 */ "SQLException",
/*14 */ "RemoteException",
/*15 */ "Deprecated operation",
	 /*EOE*/ NULL
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

static char M5OutOfMemory[] = MAL_MALLOC_FAIL;

char *
concatErrors(const char *err1, const char *err2)
{
	/* in case either one of the input errors comes from the exception
	 * buffer, we make temporary copies of both */
	allocator *ta = MT_thread_getallocator();
	allocator_state ta_state = ma_open(ta);
	char *err1cp = ma_strdup(ta, err1);
	char *err2cp = ma_strdup(ta, err2);
	bool addnl = err1[strlen(err1cp) - 1] != '\n';
	char *new = MT_thread_get_exceptbuf();
	strconcat_len(new, GDKMAXERRLEN, err1cp, addnl ? "\n" : "", err2cp, NULL);
	ma_close(ta, &ta_state);
	return new;
}

/**
 * Internal helper function for createException and
 * showException such that they share the same code, because reuse
 * is good.
 */
__attribute__((__format__(__printf__, 3, 0), __returns_nonnull__))
static str
createExceptionInternal(bool append, enum malexception type, const char *fcn,
						const char *format, va_list ap)
{
	int len;
	char *msg;
	va_list ap2;
	size_t buflen = GDKMAXERRLEN;

	va_copy(ap2, ap);			/* we need to use it twice */
	len = vsnprintf(NULL, 0, format, ap);	/* count necessary length */
	if (len < 0) {
		TRC_CRITICAL(MAL_SERVER, "called with bad arguments");
		len = 0;
	}
	msg = MT_thread_get_exceptbuf();
	if (msg != NULL) {
		if (append) {
			size_t mlen = strlen(msg);
			msg += mlen;
			buflen -= mlen;
			if (buflen < 64)
				return MT_thread_get_exceptbuf();
		}
		/* the calls below succeed: the arguments have already been checked */
		size_t msglen = strconcat_len(msg, buflen, exceptionNames[type],
									  ":", fcn, ":", NULL);
		if (len > 0 && msglen < buflen) {
			int prlen = vsnprintf(msg + msglen, buflen - msglen, format, ap2);
			if (msglen + prlen >= buflen)
				strcpy(msg + buflen - 5, "...\n");
		}
		char *q = msg + strlen(msg);
		if (q[-1] != '\n') {
			/* make sure message ends with newline */
			if (q >= msg + buflen - 1) {
				strcpy(msg + buflen - 5, "...\n");
			} else {
				*q++ = '\n';
				*q = '\0';
			}
		}
		q = msg;
		for (char *p = strchr(msg, '\n'); p; q = p + 1, p = strchr(q, '\n'))
			TRC_ERROR(MAL_SERVER, "%.*s\n", (int) (p - q), q);
		if (*q)
			TRC_ERROR(MAL_SERVER, "%s\n", q);
		msg = MT_thread_get_exceptbuf();
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
createException(enum malexception type, const char *fcn, const char *format,
				...)
{
	va_list ap;
	str ret = NULL, localGDKerrbuf = GDKerrbuf;

	if (localGDKerrbuf &&
		(ret = strstr(format, MAL_MALLOC_FAIL)) != NULL &&
		ret[strlen(MAL_MALLOC_FAIL)] != ':' &&
		(strncmp(localGDKerrbuf, "GDKmalloc", 9) == 0 ||
		 strncmp(localGDKerrbuf, "GDKrealloc", 10) == 0 ||
		 strncmp(localGDKerrbuf, "GDKzalloc", 9) == 0 ||
		 strncmp(localGDKerrbuf, "GDKstrdup", 9) == 0 ||
		 strncmp(localGDKerrbuf, "allocating too much virtual address space",
				 41) == 0)) {
		/* override errors when the underlying error is memory
		 * exhaustion, but include whatever it is that the GDK level
		 * reported */
		ret = createException(type, fcn, SQLSTATE(HY013) MAL_MALLOC_FAIL ": %s",
							  localGDKerrbuf);
		GDKclrerr();
		assert(ret);
		return ret;
	}
	if (localGDKerrbuf && localGDKerrbuf[0]
		&& strcmp(format, GDK_EXCEPTION) == 0) {
		/* for GDK errors, report the underlying error */
		char *p = localGDKerrbuf;
		if (strncmp(p, GDKERROR, strlen(GDKERROR)) == 0) {
			/* error is "!ERROR: function_name: STATE!error message"
			 * we need to skip everything up to the STATE */
			p += strlen(GDKERROR);
			char *q = strchr(p, ':');
			if (q && q[1] == ' ' && strlen(q) > 8 && q[7] == '!')
				ret = createException(type, fcn, "%s", q + 2);
		}
		if (ret == NULL)
			ret = createException(type, fcn, "GDK reported%s: %s",
								  strstr(p, EXITING_MSG) ? "" : " error", p);
		GDKclrerr();
		assert(ret);
		return ret;
	}
	va_start(ap, format);
	ret = createExceptionInternal(false, type, fcn, format, ap);
	va_end(ap);
	GDKclrerr();

	assert(ret);
	return ret;
}

str
appendException(enum malexception type, const char *fcn, const char *format,
				...)
{
	va_list ap;
	va_start(ap, format);
	str ret = createExceptionInternal(true, type, fcn, format, ap);
	va_end(ap);
	GDKclrerr();

	assert(ret);
	return ret;
}

void
freeException(str msg)
{
	(void)msg;
	//if (msg != MAL_SUCCEED && msg != M5OutOfMemory)
	//	GDKfree(msg);
}

/**
 * Internal helper function for createMalException and
 * showScriptException such that they share the same code, because reuse
 * is good.
 */
__attribute__((__format__(__printf__, 5, 0), __returns_nonnull__))
static str
createMalExceptionInternal(MalBlkPtr mb, int pc, enum malexception type,
						   const char *prev, const char *format, va_list ap)
{
	bool addnl = false;
	const char *mod = getInstrPtr(mb, 0) ? getModName(mb) : "unknown";
	const char *fcn = getInstrPtr(mb, 0) ? getFcnName(mb) : "unknown";
	char *buf = MT_thread_get_exceptbuf();
	size_t buflen = GDKMAXERRLEN;

	if (prev) {
		size_t msglen = strlen(prev);
		assert(msglen < buflen);
		if (prev != buf) {
			strcpy_len(buf, prev, buflen);
		}
		buf += msglen;
		buflen -= msglen;
		if (msglen > 0 && prev[msglen - 1] != '\n') {
			addnl = true;
			msglen++;
		}
	}
	if (type == SYNTAX) {
		size_t msglen = strconcat_len(buf, buflen,
									  exceptionNames[type], ":", NULL);
		if (msglen < buflen) {
			buf += msglen;
			buflen -= msglen;
		} else {
			buflen = 0;
		}
	} else {
		int msglen = snprintf(buf, buflen, "%s!%s:%s.%s[%d]:",
							  addnl ? "\n" : "",
							  exceptionNames[type], mod, fcn, pc);
		if ((size_t) msglen < buflen) {
			buf += msglen;
			buflen -= (size_t) msglen;
		} else {
			buflen = 0;
		}
	}
	if (buflen > 0)
		(void) vsnprintf(buf, buflen, format, ap);
	return MT_thread_get_exceptbuf();
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
createMalException(MalBlkPtr mb, int pc, enum malexception type,
				   const char *format, ...)
{
	va_list ap;
	str ret;

	va_start(ap, format);
	ret = createMalExceptionInternal(mb, pc, type, mb->errors, format, ap);
	va_end(ap);

	return (ret);
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

	return (ret);
}

/**
 * Returns the location the exception was raised, if known.  It
 * depends on how the exception was created, what the location looks
 * like.  The returned string is mallocced with GDKmalloc, and hence
 * needs to be GDKfreed.
 */
str
getExceptionPlace(allocator *ma, const char *exception)
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
				if ((ret = ma_alloc(ma, t - s + 1)) == NULL)
					return NULL;
				strcpy_len(ret, s, t - s + 1);
				return ret;
			}
			break;
		}
	}
	return ma_strdup(ma, "(unknown)");
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
		(isdigit((unsigned char) msg[4]) || (msg[4] >= 'A' && msg[4] <= 'Z')))
		msg += 6;
	return msg;
}
