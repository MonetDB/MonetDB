/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 *  M. Kersten
 *  Y. Zhang
 * The URL module
 * The URL module contains a collection of commands to manipulate
 * Uniform Resource Locators - a resource on the World Wide Web-
 * represented as a string in Monet. The URL can represent
 * anything from a file, a directory or a complete movie.
 * This module is geared towards manipulation of their name only.
 * A complementary module can be used to gain access.[IOgate]
 *
 * The URL syntax is specified in RFC2396, Uniform Resource Identifiers
 * (URI): Generic Syntax. The URL syntax is dependent upon the scheme.
 * In general, a URL has the form <scheme>:<scheme-specific-part>.
 * Thus, accepting a valid URL is a simple proccess, unless the scheme
 * is known and schema-specific syntax is checked (e.g., http or ftp
 * scheme). For the URL module implemented here, we assume some common
 * fields of the <scheme-specific-part> that are shared among different
 * schemes.
 *
 * The core of the extension involves several operators to extract
 * portions of the URLs for further manipulation. In particular,
 * the domain, the server, and the protocol, and the file extension
 * can be extracted without copying the complete URL from the heap
 * into a string variable first.
 *
 * The commands provided are based on the corresponding Java class.
 *
 * A future version should use a special atom, because this may save
 * considerable space. Alternatively, break the URL strings into
 * components and represent them with a bunch of BATs. An intermediate
 * step would be to refine the atom STR, then it would be possible to
 * redefine hashing.
 */

#include "monetdb_config.h"
#include "mal.h"
#include "url.h"
#include "mal_exception.h"

static char x2c(char *what);

/* SCHEME "://" AUTHORITY [ PATH ] [ "?" SEARCH ] [ "#" FRAGMENT ]
 * AUTHORITY is: [ USER [ ":" PASSWORD ] "@" ] HOST [ ":" PORT ] */

/* return pointer to string after the scheme and colon; input: pointer
 * to start of URI */
static const char *
skip_scheme(const char *uri)
{
	if (('a' <= *uri && *uri <= 'z') || ('A' <= *uri && *uri <= 'Z')) {
		uri++;
		while (('a' <= *uri && *uri <= 'z') ||
			   ('A' <= *uri && *uri <= 'Z') ||
			   ('0' <= *uri && *uri <= '9') ||
			   *uri == '+' || *uri == '-' || *uri == '.')
			uri++;
		if (*uri == ':')
			return uri + 1;
	}
	return NULL;
}

#define ishex(c) (('0' <= (c) && (c) <= '9') || \
				  ('a' <= (c) && (c) <= 'f') || \
				  ('A' <= (c) && (c) <= 'F'))
#define isreserved(c)	((c) == ';' || (c) == '/' || (c) == '?' || \
						 (c) == ':' || (c) == '@' || (c) == '&' || \
						 (c) == '=' || (c) == '+' || (c) == '$' || \
						 (c) == ',')
#define isunreserved(c) (('a' <= (c) && (c) <= 'z') || \
						 ('A' <= (c) && (c) <= 'Z') || \
						 ('0' <= (c) && (c) <= '9') || \
						 (c) == '-' || (c) == '_' || (c) == '.' || \
						 (c) == '!' || (c) == '~' || (c) == '*' || \
						 (c) == '\'' || (c) == '(' || (c) == ')')

/* return pointer to string after the authority, filling in pointers
 * to start of user, password, host, and port, if provided; input:
 * result of skip_scheme() */
static const char *
skip_authority(const char *uri, const char **userp, const char **passp, const char **hostp, const char **portp)
{
	const char *user = NULL, *pass = NULL, *host = NULL, *port = NULL;

	if (uri[0] == '/' && uri[1] == '/') {
		uri += 2;
		user = host = uri;
		while (isunreserved(*uri) ||
			   (*uri == '%' && ishex(uri[1]) && ishex(uri[2])) ||
			   *uri == ';' || *uri == ':' || *uri == '=' || *uri == '+'|| *uri == '$' || *uri == ',' ||
			   *uri == '@') {
			if (*uri == ':') {
				if (user == host)
					port = pass = uri + 1;
				else
					port = uri + 1;
			} else if (*uri == '@')
				host = uri + 1;
			uri += *uri == '%' ? 3 : 1;
		}
		if (user == host) {
			/* no "@", so no user info */
			if (userp)
				*userp = NULL;
			if (passp)
				*passp = NULL;
		} else {
			if (*userp)
				*userp = user;
			if (*passp)
				*passp = pass;
		}
		if (portp)
			*portp = port;
		if (hostp)
			*hostp = host;
		return uri;
	}
	return NULL;
}

/* return pointer to string after the path, filling in pointer to
 * start of last component and extension of that component; input:
 * result of skip_authority() */
static const char *
skip_path(const char *uri, const char **basep, const char **extp)
{
	const char *base = NULL, *ext = NULL;

	if (*uri == '/') {
		uri++;
		base = uri;
		while (isunreserved(*uri) ||
			   (*uri == '%' && ishex(uri[1]) && ishex(uri[2])) ||
			   *uri == ':' || *uri == '@' || *uri == '&' || *uri == '=' || *uri == '+' || *uri == '$' || *uri == ',' ||
			   *uri == ';' ||
			   *uri == '/') {
			if (*uri == '/') {
				base = uri + 1;
				ext = NULL;
			} else if (*uri == '.' && ext == NULL && uri != base) {
				ext = uri;
			}
			uri += *uri == '%' ? 3 : 1;
		}
	}
	if (basep)
		*basep = base;
	if (extp)
		*extp = ext;
	return uri;
}

/* return pointer to string after the search string; input: result of
 * skip_path() */
static const char *
skip_search(const char *uri)
{
	if (*uri == '?') {
		uri++;
		while (isreserved(*uri) || isunreserved(*uri) ||
			   (*uri == '%' && ishex(uri[1]) && ishex(uri[2]))) {
			uri += *uri == '%' ? 3 : 1;
		}
	}
	return uri;
}

static int needEscape(char c){
	if( isalnum((int)c) )
		return 0;
	if( c == '#' || c == '-' || c == '_' || c == '.' || c == '!' ||
		c == '~' || c == '*' || c == '\'' || c == '(' || c == ')' )
		return 0;
	return 1;
}

/* COMMAND "escape": this function applies the URI escaping rules defined in
 * section 2 of [RFC 3986] to the string supplied as 's'.
 * The effect of the function is to escape a set of identified characters in
 * the string. Each such character is replaced in the string by an escape
 * sequence, which is formed by encoding the character as a sequence of octets
 * in UTF-8, and then reprensenting each of these octets in the form %HH.
 *
 * All characters are escaped other than:
 * [a-z], [A-Z], [0-9], "#", "-", "_", ".", "!", "~", "*", "'", "(", ")"
 *
 * This function must always generate hexadecimal values using the upper-case
 * letters A-F.
 *
 * SIGNATURE: escape(str) : str; */
str
escape_str(str *retval, str s)
{
	int x, y;
	str res;

	if (!s)
		throw(ILLARG, "url.escape", "url missing");

	if (!( res = (str) GDKmalloc( strlen(s) * 3 ) ))
		throw(MAL, "url.escape", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	for (x = 0, y = 0; s[x]; ++x, ++y) {
		if (needEscape(s[x])) {
			if (s[x] == ' ') {
				res[y] = '+';
			} else {
				sprintf(res+y, "%%%2x", s[x]);
				y += 2;
			}
		} else {
			res[y] = s[x];
		}
	}
	res[y] = '\0';

	if ((*retval = GDKrealloc(res, strlen(res)+1)) == NULL) {
		GDKfree(res);
		throw(MAL, "url.escape", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	return MAL_SUCCEED;
}

/* COMMAND "unescape": Convert hexadecimal representations to ASCII characters.
 *                     All sequences of the form "% HEX HEX" are unescaped.
 * SIGNATURE: unescape(str) : str; */
str
unescape_str(str *retval, str s)
{
	int x, y;
	str res;

	if (!s)
		throw(ILLARG, "url.escape", "url missing");

	res = (str) GDKmalloc(strlen(s));
	if (!res)
		throw(MAL, "url.unescape", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	for (x = 0, y = 0; s[x]; ++x, ++y) {
		if (s[x] == '%') {
			res[y] = x2c(&s[x + 1]);
			x += 2;
		} else {
			res[y] = s[x];
		}
	}
	res[y] = '\0';

	if ((*retval = GDKrealloc(res, strlen(res)+1)) == NULL) {
		GDKfree(res);
		throw(MAL, "url.unescape", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	return MAL_SUCCEED;
}

/*
 * Utilities
 */

#define LF 10
#define CR 13

static char
x2c(char *what)
{
	char digit;

	digit = (what[0] >= 'A' ? ((what[0] & 0xdf) - 'A') + 10 : (what[0] - '0'));
	digit *= 16;
	digit += (what[1] >= 'A' ? ((what[1] & 0xdf) - 'A') + 10 : (what[1] - '0'));
	return (digit);
}

/*
 * Wrapping
 * Here you find the wrappers around the V4 url library included above.
 */

ssize_t
URLfromString(const char *src, size_t *len, str *u)
{
	size_t l = strlen(src) + 1;

	if (*len < l || *u == NULL) {
		GDKfree(*u);
		*u = GDKmalloc(l);
		if (*u == NULL)
			return -1;
		*len = l;
	}

	/* actually parse the message for valid url */

	memcpy(*u, src, l);
	return (ssize_t) l - 1;
}

ssize_t
URLtoString(str *s, size_t *len, const char *src)
{
	size_t l;

	if (GDK_STRNIL(src)) {
		*s = GDKstrdup("nil");
		return *s ? 1 : -1;
	}
	l = strlen(src) + 3;
	/* if( !*s) *s= (str)GDKmalloc(*len = l); */

	if (l >= *len || *s == NULL) {
		GDKfree(*s);
		*s = (str) GDKmalloc(l);
		if (*s == NULL)
			return -1;
	}
	snprintf(*s, l, "\"%s\"", src);
	*len = l - 1;
	return (ssize_t) *len;
}

/* COMMAND "getAnchor": Extract an anchor (reference) from the URL
 * SIGNATURE: getAnchor(url) : str; */
str
URLgetAnchor(str *retval, url *val)
{
	const char *s;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getAnchor", "url missing");
	if ((s = skip_scheme(*val)) == NULL ||
		(s = skip_authority(s, NULL, NULL, NULL, NULL)) == NULL ||
		(s = skip_path(s, NULL, NULL)) == NULL ||
		(s = skip_search(s)) == NULL)
		throw(ILLARG, "url.getAnchor", "bad url");
	if (*s == '#')
		s++;
	else
		s = str_nil;
	if ((*retval = GDKstrdup(s)) == NULL)
		throw(MAL, "url.getAnchor", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getBasename": Extract the base of the last file name of the URL,
 *                        thus, excluding the file extension.
 * SIGNATURE: getBasename(str) : str; */
str
URLgetBasename(str *retval, url *val)
{
	const char *s;
	const char *b = NULL;
	const char *e = NULL;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getBasename", "url missing");
	if ((s = skip_scheme(*val)) == NULL ||
		(s = skip_authority(s, NULL, NULL, NULL, NULL)) == NULL ||
		(s = skip_path(s, &b, &e)) == NULL)
		throw(ILLARG, "url.getBasename", "bad url");
	if (b == NULL) {
		*retval = GDKstrdup(str_nil);
	} else {
		size_t l;

		if (e != NULL) {
			l = e - b;
		} else {
			l = s - b;
		}
		if ((*retval = GDKmalloc(l + 1)) != NULL) {
			strncpy(*retval, b, l);
			(*retval)[l] = 0;
		}
	}
	if (*retval == NULL)
		throw(MAL, "url.getBasename", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getContent": Retrieve the file referenced
 * SIGNATURE: getContent(str) : str; */
str
URLgetContent(str *retval, url *Str1)
{
	stream *f;
	str retbuf = NULL;
	str oldbuf = NULL;
	char *buf[8096];
	ssize_t len;
	size_t rlen;

	if ((f = open_urlstream(*Str1)) == NULL)
		throw(MAL, "url.getContent", "failed to open urlstream");

	if (mnstr_errnr(f) != 0) {
		str err = createException(MAL, "url.getContent",
				"opening stream failed: %s", mnstr_error(f));
		mnstr_destroy(f);
		*retval = NULL;
		return err;
	}

	rlen = 0;
	while ((len = mnstr_read(f, buf, 1, sizeof(buf))) > 0) {
		if (retbuf != NULL) {
			oldbuf = retbuf;
			retbuf = GDKrealloc(retbuf, rlen + len + 1);
		} else {
			retbuf = GDKmalloc(len + 1);
		}
		if (retbuf == NULL) {
			if (oldbuf != NULL)
				GDKfree(oldbuf);
			mnstr_destroy(f);
			throw(MAL, "url.getContent", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		oldbuf = NULL;
		(void)memcpy(retbuf + rlen, buf, len);
		rlen += len;
	}
	mnstr_destroy(f);
	if (len < 0) {
		GDKfree(retbuf);
		throw(MAL, "url.getContent", "read error");
	}
	retbuf[rlen] = '\0';

	*retval = retbuf;
	return MAL_SUCCEED;
}

/* COMMAND "getContext": Extract the path context from the URL
 * SIGNATURE: getContext(str) : str; */
str
URLgetContext(str *retval, url *val)
{
	const char *s;
	const char *p;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getContext", "url missing");
	if ((s = skip_scheme(*val)) == NULL ||
		(p = skip_authority(s, NULL, NULL, NULL, NULL)) == NULL ||
		(s = skip_path(p, NULL, NULL)) == NULL)
		throw(ILLARG, "url.getContext", "bad url");
	if (p == s) {
		*retval = GDKstrdup(str_nil);
	} else if ((*retval = GDKmalloc(s - p + 1)) != NULL) {
		strncpy(*retval, p, s - p);
		(*retval)[s - p] = 0;
	}
	if (*retval == NULL)
		throw(MAL, "url.getContext", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getExtension": Extract the file extension of the URL
 * SIGNATURE: getExtension(str) : str; */
str
URLgetExtension(str *retval, url *val)
{
	const char *s;
	const char *e = NULL;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getExtension", "url missing");
	if ((s = skip_scheme(*val)) == NULL ||
		(s = skip_authority(s, NULL, NULL, NULL, NULL)) == NULL ||
		(s = skip_path(s, NULL, &e)) == NULL)
		throw(ILLARG, "url.getExtension", "bad url");
	if (e == NULL) {
		*retval = GDKstrdup(str_nil);
	} else {
		size_t l = s - e;

		assert(*e == '.');
		if ((*retval = GDKmalloc(l)) != NULL) {
			strncpy(*retval, e + 1, l - 1);
			(*retval)[l - 1] = 0;
		}
	}
	if (*retval == NULL)
		throw(MAL, "url.getExtension", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getFile": Extract the last file name of the URL
 * SIGNATURE: getFile(str) : str; */
str
URLgetFile(str *retval, url *val)
{
	const char *s;
	const char *b = NULL;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getFile", "url missing");
	if ((s = skip_scheme(*val)) == NULL ||
		(s = skip_authority(s, NULL, NULL, NULL, NULL)) == NULL ||
		(s = skip_path(s, &b, NULL)) == NULL)
		throw(ILLARG, "url.getFile", "bad url");
	if (b == NULL) {
		*retval = GDKstrdup(str_nil);
	} else {
		size_t l;

		l = s - b;
		if ((*retval = GDKmalloc(l + 1)) != NULL) {
			strncpy(*retval, b, l);
			(*retval)[l] = 0;
		}
	}
	if (*retval == NULL)
		throw(MAL, "url.getFile", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getHost": Extract the server identity from the URL */
/* SIGNATURE: getHost(str) : str; */
str
URLgetHost(str *retval, url *val)
{
	const char *s;
	const char *h = NULL;
	const char *p = NULL;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getHost", "url missing");
	if ((s = skip_scheme(*val)) == NULL ||
		(s = skip_authority(s, NULL, NULL, &h, &p)) == NULL)
		throw(ILLARG, "url.getHost", "bad url");
	if (h == NULL) {
		*retval = GDKstrdup(str_nil);
	} else {
		size_t l;

		if (p != NULL) {
			l = p - h - 1;
		} else {
			l = s - h;
		}
		if ((*retval = GDKmalloc(l + 1)) != NULL) {
			strncpy(*retval, h, l);
			(*retval)[l] = 0;
		}
	}
	if (*retval == NULL)
		throw(MAL, "url.getHost", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getDomain": Extract the Internet domain from the URL
 * SIGNATURE: getDomain(str) : str; */
str
URLgetDomain(str *retval, url *val)
{
	const char *s;
	const char *h = NULL;
	const char *p = NULL;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getDomain", "url missing");
	if ((s = skip_scheme(*val)) == NULL ||
		(s = skip_authority(s, NULL, NULL, &h, &p)) == NULL)
		throw(ILLARG, "url.getDomain", "bad url");
	if (h == NULL) {
		*retval = GDKstrdup(str_nil);
	} else {
		size_t l;

		if (p != NULL)
			p--;
		else
			p = s;
		l = 0;
		while (p > h && p[-1] != '.') {
			p--;
			l++;
		}
		if ((*retval = GDKmalloc(l + 1)) != NULL) {
			strncpy(*retval, p, l);
			(*retval)[l] = 0;
		}
	}
	if (*retval == NULL)
		throw(MAL, "url.getDomain", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getPort": Extract the port id from the URL
 * SIGNATURE: getPort(str) : str; */
str
URLgetPort(str *retval, url *val)
{
	const char *s;
	const char *p = NULL;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getPort", "url missing");
	if ((s = skip_scheme(*val)) == NULL ||
		(s = skip_authority(s, NULL, NULL, NULL, &p)) == NULL)
		throw(ILLARG, "url.getPort", "bad url");
	if (p == NULL) {
		*retval = GDKstrdup(str_nil);
	} else {
		size_t l = s - p;

		if ((*retval = GDKmalloc(l + 1)) != NULL) {
			strncpy(*retval, p, l);
			(*retval)[l] = 0;
		}
	}
	if (*retval == NULL)
		throw(MAL, "url.getPort", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getProtocol": Extract the protocol from the URL
 * SIGNATURE: getProtocol(str) : str; */
str
URLgetProtocol(str *retval, url *val)
{
	const char *s;
	size_t l;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getProtocol", "url missing");
	if ((s = skip_scheme(*val)) == NULL)
		throw(ILLARG, "url.getProtocol", "bad url");
	l = s - *val;
	if ((*retval = GDKmalloc(l)) == NULL)
		throw(MAL, "url.getProtocol", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	strncpy(*retval, *val, l - 1);
	(*retval)[l - 1] = 0;
	return MAL_SUCCEED;
}

/* COMMAND "getQuery": Extract the query part from the URL
 * SIGNATURE: getQuery(str) : str; */
str
URLgetQuery(str *retval, url *val)
{
	const char *s;
	const char *q;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getQuery", "url missing");
	if ((s = skip_scheme(*val)) == NULL ||
		(s = skip_authority(s, NULL, NULL, NULL, NULL)) == NULL ||
		(q = skip_path(s, NULL, NULL)) == NULL ||
		(s = skip_search(q)) == NULL)
		throw(ILLARG, "url.getQuery", "bad url");
	if (*q == '?') {
		size_t l;

		q++;
		l = s - q;
		if ((*retval = GDKmalloc(l + 1)) != NULL) {
			strncpy(*retval, q, l);
			(*retval)[l] = 0;
		}
	} else {
		*retval = GDKstrdup(str_nil);
	}
	if (*retval == NULL)
		throw(MAL, "url.getQuery", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getRobotURL": Extract the location of the robot control file
 * SIGNATURE: getRobotURL(str) : str; */
str
URLgetRobotURL(str *retval, url *val)
{
	const char *s;
	size_t l;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getQuery", "url missing");
	if ((s = skip_scheme(*val)) == NULL ||
		(s = skip_authority(s, NULL, NULL, NULL, NULL)) == NULL)
		throw(ILLARG, "url.getQuery", "bad url");
	l = s - *val;
	if ((*retval = GDKmalloc(l + sizeof("/robots.txt"))) == NULL)
		throw(MAL, "url.getQuery", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	sprintf(*retval, "%.*s/robots.txt", (int) l, *val);
	return MAL_SUCCEED;
}


/* COMMAND "getUser": Extract the user identity from the URL
 * SIGNATURE: getUser(str) : str; */
str
URLgetUser(str *retval, url *val)
{
	const char *s;
	const char *p;
	const char *u;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getUser", "url missing");
	if ((s = skip_scheme(*val)) == NULL ||
		(p = skip_authority(s, NULL, NULL, NULL, NULL)) == NULL ||
		(s = skip_path(p, NULL, NULL)) == NULL)
		throw(ILLARG, "url.getUser", "bad url");
	if (p == s || *p != '/' || p[1] != '~') {
		*retval = GDKstrdup(str_nil);
	} else {
		size_t l;

		u = p + 2;
		for (p = u; p < s && *p != '/'; p++)
			;
		l = p - u;
		if ((*retval = GDKmalloc(l + 1)) != NULL) {
			strncpy(*retval, u, l);
			(*retval)[l] = 0;
		}
	}
	if (*retval == NULL)
		throw(MAL, "url.getUser", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "isaURL": Check conformity of the URL syntax
 * SIGNATURE: isaURL(str) : bit; */
str
URLisaURL(bit *retval, url *val)
{
	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.isaURL", "url missing");
	*retval = skip_scheme(*val) != NULL;
	return MAL_SUCCEED;
}

str
URLnew(url *u, str *val)
{
	*u = GDKstrdup(*val);
	return MAL_SUCCEED;
}

str
URLnew3(url *u, str *protocol, str *server, str *file)
{
	size_t l;

	l = GDK_STRLEN(*file) + GDK_STRLEN(*server) + GDK_STRLEN(*protocol) + 10;
	*u = GDKmalloc(l);
	if (*u == NULL)
		throw(MAL, "url.newurl", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	snprintf(*u, l, "%s://%s/%s", *protocol, *server, *file);
	return MAL_SUCCEED;
}

str
URLnew4(url *u, str *protocol, str *server, int *port, str *file)
{
	str Protocol = *protocol;
	str Server = *server;
	str File = *file;
	size_t l;

	if (GDK_STRNIL(File))
		File = "";
	else if (*File == '/')
		File++;
	if (GDK_STRNIL(Server))
		Server = "";
	if (GDK_STRNIL(Protocol))
		Protocol = "";
	l = strlen(File) + strlen(Server) + strlen(Protocol) + 20;
	*u = GDKmalloc(l);
	if (*u == NULL)
		throw(MAL, "url.newurl", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	snprintf(*u, l, "%s://%s:%d/%s", Protocol, Server, *port, File);
	return MAL_SUCCEED;
}

str URLnoop(url *u, url *val)
{
	*u = GDKstrdup(*val);
	return MAL_SUCCEED;
}
