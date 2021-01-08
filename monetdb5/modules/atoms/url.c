/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "gdk.h"
#include <ctype.h>
#include "mal_exception.h"

typedef str url;

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
			   isdigit((unsigned char) *uri) ||
			   *uri == '+' || *uri == '-' || *uri == '.')
			uri++;
		if (*uri == ':')
			return uri + 1;
	}
	return NULL;
}

#define ishex(c)		isxdigit((unsigned char) (c))
#define isreserved(c)	((c) == ';' || (c) == '/' || (c) == '?' || \
						 (c) == ':' || (c) == '@' || (c) == '&' || \
						 (c) == '=' || (c) == '+' || (c) == '$' || \
						 (c) == ',')
#define isunreserved(c) (('a' <= (c) && (c) <= 'z') || \
						 ('A' <= (c) && (c) <= 'Z') || \
						 isdigit((unsigned char) (c)) || \
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
			if (userp)
				*userp = user;
			if (passp)
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

#if 0
/*
 * Utilities
 */

static char
x2c(char *what)
{
	char digit;

	digit = (what[0] >= 'A' ? ((what[0] & 0xdf) - 'A') + 10 : (what[0] - '0'));
	digit *= 16;
	digit += (what[1] >= 'A' ? ((what[1] & 0xdf) - 'A') + 10 : (what[1] - '0'));
	return (digit);
}

static int needEscape(char c){
	if( isalnum((unsigned char)c) )
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
static str
escape_str(str *retval, str s)
{
	int x, y;
	str res;

	if (!s)
		throw(ILLARG, "url.escape", "url missing");

	if (!( res = (str) GDKmalloc( strlen(s) * 3 ) ))
		throw(MAL, "url.escape", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	for (x = 0, y = 0; s[x]; ++x, ++y) {
		if (needEscape(s[x])) {
			if (s[x] == ' ') {
				res[y] = '+';
			} else {
				sprintf(res+y, "%%%2x", (uint8_t) s[x]);
				y += 2;
			}
		} else {
			res[y] = s[x];
		}
	}
	res[y] = '\0';

	if ((*retval = GDKrealloc(res, strlen(res)+1)) == NULL) {
		GDKfree(res);
		throw(MAL, "url.escape", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	return MAL_SUCCEED;
}

/* COMMAND "unescape": Convert hexadecimal representations to ASCII characters.
 *                     All sequences of the form "% HEX HEX" are unescaped.
 * SIGNATURE: unescape(str) : str; */
static str
unescape_str(str *retval, str s)
{
	int x, y;
	str res;

	if (!s)
		throw(ILLARG, "url.escape", "url missing");

	res = (str) GDKmalloc(strlen(s));
	if (!res)
		throw(MAL, "url.unescape", SQLSTATE(HY013) MAL_MALLOC_FAIL);

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
		throw(MAL, "url.unescape", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	return MAL_SUCCEED;
}
#endif

/*
 * Wrapping
 * Here you find the wrappers around the V4 url library included above.
 */

static ssize_t
URLfromString(const char *src, size_t *len, void **U, bool external)
{
	char **u = (char **) U;
	size_t l = strlen(src) + 1;

	if (*len < l || *u == NULL) {
		GDKfree(*u);
		*u = GDKmalloc(l);
		if (*u == NULL)
			return -1;
		*len = l;
	}

	/* actually parse the message for valid url */

	if (external && strcmp(src, "nil") == 0)
		strcpy(*u, str_nil);
	else
		memcpy(*u, src, l);
	return (ssize_t) l - 1;
}

static ssize_t
URLtoString(str *s, size_t *len, const void *SRC, bool external)
{
	const char *src = SRC;
	size_t l = strlen(src);

	if (external)
		l += 2;
	if (l >= *len || *s == NULL) {
		GDKfree(*s);
		*s = GDKmalloc(l + 1);
		if (*s == NULL)
			return -1;
		*len = l + 1;
	}

	if (external) {
		if (strNil(src)) {
			strcpy(*s, "nil");
			return 3;
		}
		snprintf(*s, l + 1, "\"%s\"", src);
	} else {
		strcpy(*s, src);
	}
	return (ssize_t) l;
}

/* COMMAND "getAnchor": Extract an anchor (reference) from the URL
 * SIGNATURE: getAnchor(url) : str; */
static str
URLgetAnchor(str *retval, url *val)
{
	const char *s;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getAnchor", "url missing");

	if (strNil(*val)) {
		s = str_nil;
	} else {
		if ((s = skip_scheme(*val)) == NULL ||
			(s = skip_authority(s, NULL, NULL, NULL, NULL)) == NULL ||
			(s = skip_path(s, NULL, NULL)) == NULL ||
			(s = skip_search(s)) == NULL)
			throw(ILLARG, "url.getAnchor", "bad url");
		if (*s == '#')
			s++;
		else
			s = str_nil;
	}

	if ((*retval = GDKstrdup(s)) == NULL)
		throw(MAL, "url.getAnchor", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getBasename": Extract the base of the last file name of the URL,
 *                        thus, excluding the file extension.
 * SIGNATURE: getBasename(str) : str; */
static str
URLgetBasename(str *retval, url *val)
{
	const char *s;
	const char *b = NULL;
	const char *e = NULL;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getBasename", "url missing");

	if (strNil(*val)) {
		*retval = GDKstrdup(str_nil);
	} else {
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
				strcpy_len(*retval, b, l + 1);
			}
		}
	}

	if (*retval == NULL)
		throw(MAL, "url.getBasename", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getContext": Extract the path context from the URL
 * SIGNATURE: getContext(str) : str; */
static str
URLgetContext(str *retval, url *val)
{
	const char *s;
	const char *p;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getContext", "url missing");

	if (strNil(*val)) {
		*retval = GDKstrdup(str_nil);
	} else {
		if ((s = skip_scheme(*val)) == NULL ||
			(p = skip_authority(s, NULL, NULL, NULL, NULL)) == NULL ||
			(s = skip_path(p, NULL, NULL)) == NULL)
			throw(ILLARG, "url.getContext", "bad url");
		if (p == s) {
			*retval = GDKstrdup(str_nil);
		} else if ((*retval = GDKmalloc(s - p + 1)) != NULL) {
			strcpy_len(*retval, p, s - p + 1);
		}
	}

	if (*retval == NULL)
		throw(MAL, "url.getContext", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getExtension": Extract the file extension of the URL
 * SIGNATURE: getExtension(str) : str; */
static str
URLgetExtension(str *retval, url *val)
{
	const char *s;
	const char *e = NULL;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getExtension", "url missing");

	if (strNil(*val)) {
		*retval = GDKstrdup(str_nil);
	} else {
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
				strcpy_len(*retval, e + 1, l);
			}
		}
	}

	if (*retval == NULL)
		throw(MAL, "url.getExtension", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getFile": Extract the last file name of the URL
 * SIGNATURE: getFile(str) : str; */
static str
URLgetFile(str *retval, url *val)
{
	const char *s;
	const char *b = NULL;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getFile", "url missing");

	if (strNil(*val)) {
		*retval = GDKstrdup(str_nil);
	} else {
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
				strcpy_len(*retval, b, l + 1);
			}
		}
	}

	if (*retval == NULL)
		throw(MAL, "url.getFile", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getHost": Extract the server identity from the URL */
/* SIGNATURE: getHost(str) : str; */
static str
URLgetHost(str *retval, url *val)
{
	const char *s;
	const char *h = NULL;
	const char *p = NULL;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getHost", "url missing");

	if (strNil(*val)) {
		*retval = GDKstrdup(str_nil);
	} else {
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
				strcpy_len(*retval, h, l + 1);
			}
		}
	}

	if (*retval == NULL)
		throw(MAL, "url.getHost", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getDomain": Extract the Internet domain from the URL
 * SIGNATURE: getDomain(str) : str; */
static str
URLgetDomain(str *retval, url *val)
{
	const char *s;
	const char *h = NULL;
	const char *p = NULL;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getDomain", "url missing");

	if (strNil(*val)) {
		*retval = GDKstrdup(str_nil);
	} else {
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
				strcpy_len(*retval, p, l + 1);
			}
		}
	}

	if (*retval == NULL)
		throw(MAL, "url.getDomain", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getPort": Extract the port id from the URL
 * SIGNATURE: getPort(str) : str; */
static str
URLgetPort(str *retval, url *val)
{
	const char *s;
	const char *p = NULL;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getPort", "url missing");

	if (strNil(*val)) {
		*retval = GDKstrdup(str_nil);
	} else {
		if ((s = skip_scheme(*val)) == NULL ||
			(s = skip_authority(s, NULL, NULL, NULL, &p)) == NULL)
			throw(ILLARG, "url.getPort", "bad url");
		if (p == NULL) {
			*retval = GDKstrdup(str_nil);
		} else {
			size_t l = s - p;

			if ((*retval = GDKmalloc(l + 1)) != NULL) {
				strcpy_len(*retval, p, l + 1);
			}
		}
	}

	if (*retval == NULL)
		throw(MAL, "url.getPort", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getProtocol": Extract the protocol from the URL
 * SIGNATURE: getProtocol(str) : str; */
static str
URLgetProtocol(str *retval, url *val)
{
	const char *s;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getProtocol", "url missing");

	if (strNil(*val)) {
		*retval = GDKstrdup(str_nil);
	} else {
		if ((s = skip_scheme(*val)) == NULL)
			throw(ILLARG, "url.getProtocol", "bad url");
		size_t l = s - *val;

		if ((*retval = GDKmalloc(l)) != NULL) {
			strcpy_len(*retval, *val, l);
		}
	}

	if (*retval == NULL)
		throw(MAL, "url.getProtocol", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getQuery": Extract the query part from the URL
 * SIGNATURE: getQuery(str) : str; */
static str
URLgetQuery(str *retval, url *val)
{
	const char *s;
	const char *q;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getQuery", "url missing");

	if (strNil(*val)) {
		*retval = GDKstrdup(str_nil);
	} else {
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
				strcpy_len(*retval, q, l + 1);
			}
		} else {
			*retval = GDKstrdup(str_nil);
		}
	}

	if (*retval == NULL)
		throw(MAL, "url.getQuery", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getRobotURL": Extract the location of the robot control file
 * SIGNATURE: getRobotURL(str) : str; */
static str
URLgetRobotURL(str *retval, url *val)
{
	const char *s;
	size_t l;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getQuery", "url missing");

	if (strNil(*val)) {
		*retval = GDKstrdup(str_nil);
	} else {
		if ((s = skip_scheme(*val)) == NULL ||
			(s = skip_authority(s, NULL, NULL, NULL, NULL)) == NULL)
			throw(ILLARG, "url.getQuery", "bad url");
		l = s - *val;

		if ((*retval = GDKmalloc(l + sizeof("/robots.txt"))) != NULL) {
			sprintf(*retval, "%.*s/robots.txt", (int) l, *val);
		}
	}

	if (*retval == NULL)
		throw(MAL, "url.getQuery", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "getUser": Extract the user identity from the URL
 * SIGNATURE: getUser(str) : str; */
static str
URLgetUser(str *retval, url *val)
{
	const char *s, *h, *u, *p;

	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.getUser", "url missing");

	if (strNil(*val)) {
		*retval = GDKstrdup(str_nil);
	} else {
		if ((s = skip_scheme(*val)) == NULL ||
			(s = skip_authority(s, &u, &p, &h, NULL)) == NULL)
			throw(ILLARG, "url.getHost", "bad url");
		if (u == NULL || h == NULL) {
			*retval = GDKstrdup(str_nil);
		} else {
			size_t l;

			if (p) {
				l = p - u - 1;
			} else {
				l = h - u - 1;
			}
			if ((*retval = GDKmalloc(l + 1)) != NULL) {
				strcpy_len(*retval, u, l + 1);
			}
		}
	}

	if (*retval == NULL)
		throw(MAL, "url.getUser", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* COMMAND "isaURL": Check conformity of the URL syntax
 * SIGNATURE: isaURL(str) : bit; */
static str
URLisaURL(bit *retval, str *val)
{
	if (val == NULL || *val == NULL)
		throw(ILLARG, "url.isaURL", "url missing");
	if (strNil(*val))
		*retval = bit_nil;
	else
		*retval = skip_scheme(*val) != NULL;
	return MAL_SUCCEED;
}

static str
URLnew(url *u, str *val)
{
	*u = GDKstrdup(*val);
	if (*u == NULL)
		throw(MAL, "url.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
URLnew3(url *u, str *protocol, str *server, str *file)
{
	size_t l;

	l = strLen(*file) + strLen(*server) + strLen(*protocol) + 10;
	*u = GDKmalloc(l);
	if (*u == NULL)
		throw(MAL, "url.newurl", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	snprintf(*u, l, "%s://%s/%s", *protocol, *server, *file);
	return MAL_SUCCEED;
}

static str
URLnew4(url *u, str *protocol, str *server, int *port, str *file)
{
	str Protocol = *protocol;
	str Server = *server;
	str File = *file;
	size_t l;

	if (strNil(File))
		File = "";
	else if (*File == '/')
		File++;
	if (strNil(Server))
		Server = "";
	if (strNil(Protocol))
		Protocol = "";
	l = strlen(File) + strlen(Server) + strlen(Protocol) + 20;
	*u = GDKmalloc(l);
	if (*u == NULL)
		throw(MAL, "url.newurl", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	snprintf(*u, l, "%s://%s:%d/%s", Protocol, Server, *port, File);
	return MAL_SUCCEED;
}

static str URLnoop(url *u, url *val)
{
	*u = GDKstrdup(*val);
	if (*u == NULL)
		throw(MAL, "url.noop", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

#include "mel.h"
mel_atom url_init_atoms[] = {
 { .name="url", .basetype="str", .fromstr=URLfromString, .tostr=URLtoString, },  { .cmp=NULL }
};
mel_func url_init_funcs[] = {
 command("url", "url", URLnew, false, "Create an URL from a string literal", args(1,2, arg("",url),arg("s",str))),
 command("url", "url", URLnoop, false, "Create an URL from a string literal", args(1,2, arg("",url),arg("s",url))),
 command("calc", "url", URLnew, false, "Create an URL from a string literal", args(1,2, arg("",url),arg("s",str))),
 command("calc", "url", URLnoop, false, "Create an URL from a string literal", args(1,2, arg("",url),arg("s",url))),
 command("url", "getAnchor", URLgetAnchor, false, "Extract the URL anchor (reference)", args(1,2, arg("",str),arg("u",url))),
 command("url", "getBasename", URLgetBasename, false, "Extract the URL base file name", args(1,2, arg("",str),arg("u",url))),
 command("url", "getContext", URLgetContext, false, "Get the path context of a URL", args(1,2, arg("",str),arg("u",url))),
 command("url", "getDomain", URLgetDomain, false, "Extract Internet domain from the URL", args(1,2, arg("",str),arg("u",url))),
 command("url", "getExtension", URLgetExtension, false, "Extract the file extension of the URL", args(1,2, arg("",str),arg("u",url))),
 command("url", "getFile", URLgetFile, false, "Extract the last file name of the URL", args(1,2, arg("",str),arg("u",url))),
 command("url", "getHost", URLgetHost, false, "Extract the server name from the URL", args(1,2, arg("",str),arg("u",url))),
 command("url", "getPort", URLgetPort, false, "Extract the port id from the URL", args(1,2, arg("",str),arg("u",url))),
 command("url", "getProtocol", URLgetProtocol, false, "Extract the protocol from the URL", args(1,2, arg("",str),arg("u",url))),
 command("url", "getQuery", URLgetQuery, false, "Extract the query string from the URL", args(1,2, arg("",str),arg("u",url))),
 command("url", "getUser", URLgetUser, false, "Extract the user identity from the URL", args(1,2, arg("",str),arg("u",url))),
 command("url", "getRobotURL", URLgetRobotURL, false, "Extract the location of the robot control file", args(1,2, arg("",str),arg("u",url))),
 command("url", "isaURL", URLisaURL, false, "Check conformity of the URL syntax", args(1,2, arg("",bit),arg("u",str))),
 command("url", "new", URLnew4, false, "Construct URL from protocol, host, port, and file", args(1,5, arg("",url),arg("p",str),arg("h",str),arg("prt",int),arg("f",str))),
 command("url", "new", URLnew3, false, "Construct URL from protocol, host,and file", args(1,4, arg("",url),arg("prot",str),arg("host",str),arg("fnme",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_url_mal)
{ mal_module("url", url_init_atoms, url_init_funcs); }
