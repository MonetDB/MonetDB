/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * (c) 2013 Martin Kersten
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "mal.h"
#include "mal_client.h"
#include "mal_instruction.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

typedef enum JSONkind {
	JSON_OBJECT=1,
	JSON_ARRAY,
	JSON_ELEMENT,
	JSON_VALUE,
	JSON_STRING,
	JSON_NUMBER,
	JSON_BOOL,
	JSON_NULL
} JSONkind;

/* The JSON index structure is meant for short lived versions */
typedef struct JSONterm {
	JSONkind kind;
	char *name; /* exclude the quotes */
	size_t namelen;
	const char *value; /* start of string rep */
	size_t valuelen;
	int child, next, tail; /* next offsets allow you to walk array/object chains
							  and append quickly */
	/* An array or object item has a number of components */
} JSONterm;

typedef struct JSON{
    JSONterm *elm;
    str error;
    int size;
    int free;
} JSON;

typedef str json;

// just validate the string according to www.json.org
// A straightforward recursive solution
#define skipblancs(J)							\
	do {										\
		for(; *(J); (J)++)						\
			if (*(J) != ' ' &&					\
				*(J) != '\n' &&					\
				*(J) != '\t' &&					\
				*(J) != '\f' &&					\
				*(J) != '\r')					\
				break;							\
	} while (0)

#define CHECK_JSON(jt)													\
	do {																\
		if (jt == NULL || jt->error) {									\
			char *msg;													\
			if (jt) {													\
				msg = jt->error;										\
				jt->error = NULL;										\
				JSONfree(jt);											\
			} else {													\
				msg = createException(MAL, "json.new", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
			}															\
			return msg;													\
		}																\
	} while (0)

int TYPE_json;

/* Internal constructors. */
static int jsonhint = 8;
static JSON *JSONparse(const char *j);

static JSON *
JSONnewtree(void)
{
	JSON *js;

	js = (JSON *) GDKzalloc(sizeof(JSON));
	if (js == NULL)
		return NULL;
	js->elm = (JSONterm *) GDKzalloc(sizeof(JSONterm) * jsonhint);
	if (js->elm == NULL) {
		GDKfree(js);
		return NULL;
	}
	js->size = jsonhint;
	return js;
}

static int
JSONnew(JSON *js)
{
	JSONterm *term;

	if (js->free == js->size) {
		term = (JSONterm *) GDKrealloc(js->elm, sizeof(JSONterm) * (js->size + 8));
		if (term == NULL) {
			js->error = createException(MAL, "json.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return js->free - 1;
		}
		js->elm = term;
		memset(term + js->size, 0, 8 * sizeof(JSONterm));
		js->size += 8;
		if (jsonhint < js->size)
			jsonhint = js->size;
	}
	return js->free++;
}

/* Delete a JSON structure. */
static void
JSONfree(JSON *c)
{
	if (c == 0)
		return;
	freeException(c->error);
	GDKfree(c->elm);
	GDKfree(c);
}

static ssize_t
JSONfromString(const char *src, size_t *len, void **J, bool external)
{
	json *j = (json *) J;
	size_t slen = strlen(src);
	JSON *jt;

	if (strNil(src) || (external && strncmp(src, "nil", 3) == 0)) {
		if (*len < 2 || *j == NULL) {
			GDKfree(*j);
			if ((*j = GDKmalloc(2)) == NULL)
				return -1;
			*len = 2;
		}
		strcpy(*j, str_nil);
		return strNil(src) ? 1 : 3;
	}
	if (*len <= slen || *j == NULL) {
		GDKfree(*j);
		if ((*j = GDKmalloc(slen + 1)) == NULL)
			return -1;
		*len = slen + 1;
	}
	strcpy(*j, src);
	jt = JSONparse(*j);
	if (jt == NULL)
		return -1;
	if (jt->error) {
		GDKerror("%s", getExceptionMessageAndState(jt->error));
		JSONfree(jt);
		return -1;
	}
	JSONfree(jt);

	return (ssize_t) slen;
}

static ssize_t
JSONtoString(str *s, size_t *len, const void *SRC, bool external)
{
	const char *src = SRC;
	size_t cnt;
	const char *c;
	char *dst;

	if (strNil(src)) {
		if (*s == NULL || *len < 4) {
			GDKfree(*s);
			*len = 4;
			*s = GDKmalloc(4);
			if (*s == NULL)
				return -1;
		}
		if (external) {
			return (ssize_t) strcpy_len(*s, "nil", 4);
		}
		strcpy(*s, str_nil);
		return 1;
	}
	/* count how much space we need for the output string */
	if (external) {
		cnt = 3;		/* two times " plus \0 */
		for (c = src; *c; c++)
			switch (*c) {
			case '"':
			case '\\':
			case '\n':
				cnt++;
				/* fall through */
			default:
				cnt++;
				break;
			}
	} else {
		cnt = strlen(src) + 1;	/* just the \0 */
	}

	if (cnt > (size_t) *len) {
		GDKfree(*s);
		*s = GDKmalloc(cnt);
		if (*s == NULL)
			return -1;
		*len = cnt;
	}
	dst = *s;
	if (external) {
		*dst++ = '"';
		for (c = src; *c; c++) {
			switch (*c) {
			case '"':
			case '\\':
				*dst++ = '\\';
				/* fall through */
			default:
				*dst++ = *c;
				break;
			case '\n':
				*dst++ = '\\';
				*dst++ = 'n';
				break;
			}
		}
		*dst++ = '"';
		*dst = 0;
	} else {
		dst += snprintf(dst, cnt, "%s", src);
	}
	return (ssize_t) (dst - *s);
}

static BAT *
JSONdumpInternal(JSON *jt, int depth)
{
	int i, idx;
	JSONterm *je;
	size_t buflen = 1024;
	char *buffer = GDKmalloc(buflen);
	BAT *bn = COLnew(0, TYPE_str, 0, TRANSIENT);
	if (bn == NULL)
		return NULL;

	for (idx = 0; idx < jt->free; idx++) {
		size_t datlen = 0;
		je = jt->elm + idx;

		if (datlen + depth*4 + 512 > buflen) {
			do {
				buflen += 1024;
			} while (datlen + depth*4 + 512 > buflen);
			char *newbuf = GDKrealloc(buffer, buflen);
			if (newbuf == NULL) {
				GDKfree(buffer);
				BBPreclaim(bn);
				return NULL;
			}
			buffer = newbuf;
		}
		datlen += snprintf(buffer + datlen, buflen - datlen, "%*s", depth * 4, "");
		datlen += snprintf(buffer + datlen, buflen - datlen, "[%d] ", idx);
		switch (je->kind) {
		case JSON_OBJECT:
			datlen += snprintf(buffer + datlen, buflen - datlen, "object ");
			break;
		case JSON_ARRAY:
			datlen += snprintf(buffer + datlen, buflen - datlen, "array ");
			break;
		case JSON_ELEMENT:
			datlen += snprintf(buffer + datlen, buflen - datlen, "element ");
			break;
		case JSON_VALUE:
			datlen += snprintf(buffer + datlen, buflen - datlen, "value ");
			break;
		case JSON_STRING:
			datlen += snprintf(buffer + datlen, buflen - datlen, "string ");
			break;
		case JSON_NUMBER:
			datlen += snprintf(buffer + datlen, buflen - datlen, "number ");
			break;
		case JSON_BOOL:
			datlen += snprintf(buffer + datlen, buflen - datlen, "bool ");
			break;
		case JSON_NULL:
			datlen += snprintf(buffer + datlen, buflen - datlen, "null ");
			break;
		default:
			datlen += snprintf(buffer + datlen, buflen - datlen, "unknown %d ", (int) je->kind);
		}
		datlen += snprintf(buffer + datlen, buflen - datlen, "child %d list ", je->child);
		for (i = je->next; i; i = jt->elm[i].next) {
			if (datlen + 10 > buflen) {
				buflen += 1024;
				char *newbuf = GDKrealloc(buffer, buflen);
				if (newbuf == NULL) {
					GDKfree(buffer);
					BBPreclaim(bn);
					return NULL;
				}
				buffer = newbuf;
			}
			datlen += snprintf(buffer + datlen, buflen - datlen, "%d ", i);
		}
		if (je->name) {
			if (datlen + 10 + je->namelen > buflen) {
				do {
					buflen += 1024;
				} while (datlen + 10 + je->namelen > buflen);
				char *newbuf = GDKrealloc(buffer, buflen);
				if (newbuf == NULL) {
					GDKfree(buffer);
					BBPreclaim(bn);
					return NULL;
				}
				buffer = newbuf;
			}
			datlen += snprintf(buffer + datlen, buflen - datlen, "%.*s : ", (int) je->namelen, je->name);
		}
		if (je->value) {
			if (datlen + 10 + je->valuelen > buflen) {
				do {
					buflen += 1024;
				} while (datlen + 10 + je->valuelen > buflen);
				char *newbuf = GDKrealloc(buffer, buflen);
				if (newbuf == NULL) {
					GDKfree(buffer);
					BBPreclaim(bn);
					return NULL;
				}
				buffer = newbuf;
			}
			datlen += snprintf(buffer + datlen, buflen - datlen, "%.*s", (int) je->valuelen, je->value);
		}
		if (BUNappend(bn, buffer, false) != GDK_SUCCEED) {
			BBPreclaim(bn);
			GDKfree(buffer);
			return NULL;
		}
	}
	GDKfree(buffer);
	return bn;
}

static str
JSONdump(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) cntxt;

	bat *ret = getArgReference_bat(stk, pci, 0);
	json *val = (json*) getArgReference(stk, pci, 1);
	JSON *jt = JSONparse(*val);

	CHECK_JSON(jt);
	BAT *bn = JSONdumpInternal(jt, 0);
	JSONfree(jt);
	if (bn == NULL)
		throw(MAL, "json.dump", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
JSONjson2str(str *ret, json *j)
{
	char *s = *j, *c;

	if (*s == '"')
		s++;
	if ((s = GDKstrdup(s)) == NULL)
		throw(MAL, "json.str", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	c = s + strlen(s) - 1;
	if (*c == '"')
		*c = 0;
	*ret = s;
	return MAL_SUCCEED;
}

static str
JSONstr2json(json *ret, str *j)
{
	JSON *jt = JSONparse(*j);

	CHECK_JSON(jt);
	JSONfree(jt);
	if ((*ret = GDKstrdup(*j)) == NULL)
		throw(MAL, "json.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
JSONisvalid(bit *ret, str *j)
{
	if (strNil(*j)) {
		*ret = bit_nil;
	} else {
		JSON *jt = JSONparse(*j);
		if (jt == NULL)
			throw(MAL, "json.isvalid", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		*ret = jt->error == MAL_SUCCEED;
		JSONfree(jt);
	}
	return MAL_SUCCEED;
}

static str
JSONisobject(bit *ret, json *js)
{
	if (strNil(*js)) {
		*ret = bit_nil;
	} else {
		char *j = *js;

		skipblancs(j);
		*ret = *j == '{';
	}
	return MAL_SUCCEED;
}

static str
JSONisarray(bit *ret, json *js)
{
	if (strNil(*js)) {
		*ret = bit_nil;
	} else {
		char *j = *js;

		skipblancs(j);
		*ret = *j == '[';
	}
	return MAL_SUCCEED;
}

static str
JSONprelude(void *ret)
{
	(void) ret;
	TYPE_json = ATOMindex("json");
	return MAL_SUCCEED;
}

static void
JSONappend(JSON *jt, int idx, int nxt)
{
	int chld;

	if (jt->elm[nxt].kind == JSON_OBJECT || jt->elm[nxt].kind == JSON_ARRAY) {
		chld = JSONnew(jt);
		if (jt->error)
			return;
		jt->elm[chld].kind = jt->elm[nxt].kind;
		jt->elm[chld].name = jt->elm[nxt].name;
		jt->elm[chld].namelen = jt->elm[nxt].namelen;
		jt->elm[chld].value = jt->elm[nxt].value;
		jt->elm[chld].valuelen = jt->elm[nxt].valuelen;
		jt->elm[chld].child = jt->elm[nxt].child;
		jt->elm[chld].next = jt->elm[nxt].next;
		jt->elm[chld].tail = jt->elm[nxt].tail;
		jt->elm[chld].child = nxt;

		jt->elm[nxt].child = 0;
		jt->elm[nxt].next = 0;
		jt->elm[nxt].tail = 0;
		nxt = chld;
	}
	if (jt->elm[idx].next == 0)
		jt->elm[idx].next = jt->elm[idx].tail = nxt;
	else {
		jt->elm[jt->elm[idx].tail].next = nxt;
		jt->elm[idx].tail = nxt;
	}
}

/*
 * The JSON filter operation takes a path expression which is
 * purposely kept simple, It provides step (.), multistep (..) and
 * indexed ([nr]) access to the JSON elements.  A wildcard * can be
 * used as placeholder for a step identifier.
 *
 * A path expression is always validated upfront and can only be
 * applied to valid json strings.
 * Path samples:
 * .store.book
 * .store.book[0]
 * .store.book.author
 * ..author
 */
#define MAXTERMS 256

typedef enum path_token {
	ROOT_STEP,
	CHILD_STEP,
	INDEX_STEP,
	ANY_STEP,
	END_STEP
} path_token;

typedef struct {
	path_token token;
	char *name;
	size_t namelen;
	int index;
	int first, last;
} pattern;

static str
JSONcompile(char *expr, pattern terms[])
{
	int t = 0;
	char *s, *beg;

	for (s = expr; *s; s++) {
		terms[t].token = CHILD_STEP;
		terms[t].index = INT_MAX;
		terms[t].first = INT_MAX;
		terms[t].last = INT_MAX;
		if (*s == '$') {
			if (t && terms[t - 1].token != END_STEP)
				throw(MAL, "json.compile", "Root node must be first");
			if (!(*(s + 1) == '.' || *(s + 1) == '[' || *(s + 1) == 0))
				throw(MAL, "json.compile", "Root node must be first");
			s++;
			if (*s == 0)
				terms[t].token = ROOT_STEP;
		}
		if (*s == '.' && *(s + 1) == '.') {
			terms[t].token = ANY_STEP;
			s += 2;
			if (*s == '.')
				throw(MAL, "json.compile", "Step identifier expected");
		} else if (*s == '.')
			s++;

		// child step
		if (*s != '[') {
			for (beg = s; *s; s++)
				if (*s == '.' || *s == '[' || *s == ',')
					break;
			terms[t].name = GDKzalloc(s - beg + 1);
			if(terms[t].name == NULL)
				throw(MAL, "json.compile", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			terms[t].namelen = s - beg;
			strncpy(terms[t].name, beg, s - beg);
			if (*s == '.')
				s--;
			if (*s == 0) {
				t++;
				break;
			}
		}
		if (*s == '[') {
			// array step
			bool closed = false;
			s++;
			skipblancs(s);
			if (*s != '*') {
				if (isdigit((unsigned char) *s)) {
					terms[t].index = atoi(s);
					terms[t].first = terms[t].last = atoi(s);
				} else
					throw(MAL, "json.path", "'*' or digit expected");
			}
			for (; *s; s++)
				if (*s == ']') {
					closed = true;
					break;
				}
			if (*s == 0) {
				if (!closed) {
					throw(MAL, "json.path", "] expected");
				}
				t++;
				break;
			}
			if (*s != ']')
				throw(MAL, "json.path", "] expected");
		}
		if (*s == ',') {
			if (++t == MAXTERMS)
				throw(MAL, "json.path", "too many terms");
			terms[t].token = END_STEP;
		}
		if (++t == MAXTERMS)
			throw(MAL, "json.path", "too many terms");
	}
	if (t >= MAXTERMS - 1)
		throw(MAL, "json.path", "too many terms");
	terms[t].token = END_STEP;
	return MAL_SUCCEED;
}

static str
JSONgetValue(JSON *jt, int idx)
{
	str s;

	if (jt->elm[idx].valuelen == 0)
		return GDKstrdup(str_nil);
	s = GDKzalloc(jt->elm[idx].valuelen + 1);
	if (s)
		strncpy(s, jt->elm[idx].value, jt->elm[idx].valuelen);
	return s;
}

static str
JSONglue(str res, str r, char sep)
{
	size_t len, l;
	str n;

	if (r == 0 || *r == 0) {
		GDKfree(r);
		return res;
	}
	len = strlen(r);
	if (res == 0)
		return r;
	l = strlen(res);
	n = GDKzalloc(l + len + 3);
	if( n == NULL) {
		GDKfree(res);
		GDKfree(r);
		return NULL;
	}
	snprintf(n, l + len + 3, "%s%s%s", res, sep ? "," : "", r);
	GDKfree(res);
	GDKfree(r);
	return n;
}

/* return NULL on no match, return (str) -1 on (malloc) failure */
static str
JSONmatch(JSON *jt, int ji, pattern * terms, int ti)
{
	str r = NULL, res = NULL;
	int i;
	int cnt;

	if (ti >= MAXTERMS)
		return res;

	if (terms[ti].token == ROOT_STEP) {
		if (ti + 1 == MAXTERMS)
			return NULL;
		if (terms[ti + 1].token == END_STEP) {
			res = JSONgetValue(jt, 0);
			if (res == NULL)
				res = (str) -1;
			return res;
		}
		ti++;
	}

	switch (jt->elm[ji].kind) {
	case JSON_ARRAY:
		if (terms[ti].name != 0 && terms[ti].token != ANY_STEP) {
			if (terms[ti].token == END_STEP) {
				res = JSONgetValue(jt, ji);
				if (res == NULL)
					res = (str) -1;
			}
			return res;
		}
		cnt = 0;
		for (i = jt->elm[ji].next; i && cnt >= 0; i = jt->elm[i].next, cnt++) {
			if (terms[ti].index == INT_MAX || (cnt >= terms[ti].first && cnt <= terms[ti].last)) {
				if (terms[ti].token == ANY_STEP) {
					if (jt->elm[i].child)
						r = JSONmatch(jt, jt->elm[i].child, terms, ti);
					else
						r = 0;
				} else if (ti + 1 == MAXTERMS) {
					return NULL;
				} else if (terms[ti + 1].token == END_STEP) {
					if (jt->elm[i].kind == JSON_VALUE)
						r = JSONgetValue(jt, jt->elm[i].child);
					else
						r = JSONgetValue(jt, i);
					if (r == NULL)
						r = (str) -1;
				} else
					r = JSONmatch(jt, jt->elm[i].child, terms, ti + 1);
				if (r == (str) -1) {
					GDKfree(res);
					return r;
				}
				res = JSONglue(res, r, ',');
			}
		}
		break;
	case JSON_OBJECT:
		cnt = 0;
		for (i = jt->elm[ji].next; i && cnt >= 0; i = jt->elm[i].next) {
			// check the element label
			if ((terms[ti].name &&
				 jt->elm[i].valuelen == terms[ti].namelen &&
				 strncmp(terms[ti].name, jt->elm[i].value, terms[ti].namelen) == 0) ||
				terms[ti].name == 0 ||
				terms[ti].name[0] == '*') {
				if (terms[ti].index == INT_MAX ||
					(cnt >= terms[ti].first && cnt <= terms[ti].last)) {
					if (ti + 1 == MAXTERMS)
						return NULL;
					if (terms[ti + 1].token == END_STEP) {
						r = JSONgetValue(jt, jt->elm[i].child);
						if (r == NULL)
							r = (str) -1;
					} else
						r = JSONmatch(jt, jt->elm[i].child, terms, ti + 1);
					if (r == (str) -1) {
						GDKfree(res);
						return r;
					}
					res = JSONglue(res, r, ',');
				}
				cnt++;
			} else if (terms[ti].token == ANY_STEP && jt->elm[i].child) {
				r = JSONmatch(jt, jt->elm[i].child, terms, ti);
				if (r == (str) -1) {
					GDKfree(res);
					return r;
				}
				res = JSONglue(res, r, ',');
				cnt++;
			}
		}
		break;
	default:
		res = NULL;
	}
	return res;
}

static str
JSONfilterInternal(json *ret, json *js, str *expr, str other)
{
	pattern terms[MAXTERMS];
	int tidx = 0;
	JSON *jt;
	str j = *js, msg = MAL_SUCCEED, s;
	json result = 0;
	size_t l;

	(void) other;
	if (strNil(j)) {
		*ret = GDKstrdup(j);
		if (*ret == NULL)
			throw(MAL,"JSONfilterInternal", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	jt = JSONparse(j);
	CHECK_JSON(jt);
	memset(terms, 0, sizeof(terms));
	msg = JSONcompile(*expr, terms);
	if (msg)
		goto bailout;

	result = s = JSONmatch(jt, 0, terms, tidx);
	if (s == (char *) -1) {
		msg = createException(MAL,"JSONfilterInternal", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	// process all other PATH expression
	for (tidx++; tidx < MAXTERMS && terms[tidx].token; tidx++)
		if (terms[tidx].token == END_STEP && tidx + 1 < MAXTERMS && terms[tidx + 1].token) {
			s = JSONmatch(jt, 0, terms, ++tidx);
			if (s == (char *) -1) {
				msg = createException(MAL,"JSONfilterInternal", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			result = JSONglue(result, s, ',');
		}
	if (result) {
		l = strlen(result);
		if (result[l - 1] == ',')
			result[l - 1] = 0;
	} else
		l = 3;
	s = GDKzalloc(l + 3);
	if (s == NULL) {
		GDKfree(result);
		throw(MAL,"JSONfilterInternal", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	snprintf(s, l + 3, "[%s]", (result ? result : ""));
	GDKfree(result);
	*ret = s;

  bailout:
	for (l = 0; l < MAXTERMS; l++)
		if (terms[l].name)
			GDKfree(terms[l].name);
	JSONfree(jt);
	return msg;
}


static str
JSONstringParser(const char *j, const char **next)
{
	unsigned int u;
	bool seensurrogate = false;

	assert(*j == '"');
	j++;
	for (; *j; j++) {
		switch (*j) {
		case '\\':
			// parse all escapes
			j++;
			switch (*j) {
			case '"':
			case '\\':
			case '/':
			case 'b':
			case 'f':
			case 'n':
			case 'r':
			case 't':
				if (seensurrogate)
					throw(MAL, "json.parser", "illegal escape char");
				continue;
			case 'u':
				u = 0;
				for (int i = 0; i < 4; i++) {
					u <<= 4;
					j++;
					if ('0' <= *j && *j <= '9')
						u |= *j - '0';
					else if ('a' <= *j && *j <= 'f')
						u |= *j - 'a' + 10;
					else if ('A' <= *j && *j <= 'F')
						u |= *j - 'A' + 10;
					else
						throw(MAL, "json.parser", "illegal escape char");
				}
				if (seensurrogate) {
					if ((u & 0xFC00) == 0xDC00)
						seensurrogate = false;
					else
						throw(MAL, "json.parser", "illegal escape char");
				} else {
					if ((u & 0xFC00) == 0xD800)
						seensurrogate = true;
					else if ((u & 0xFC00) == 0xDC00)
						throw(MAL, "json.parser", "illegal escape char");
				}
				break;
			default:
				*next = j;
				throw(MAL, "json.parser", "illegal escape char");
			}
			break;
		case '"':
			if (seensurrogate)
				throw(MAL, "json.parser", "illegal escape char");
			j++;
			*next = j;
			return MAL_SUCCEED;
		default:
			if (seensurrogate)
				throw(MAL, "json.parser", "illegal escape char");
			break;
		}
	}
	*next = j;
	throw(MAL, "json.parser", "Nonterminated string");
}

static bool
JSONintegerParser(const char *j, const char **next) {
	if (*j == '-')
		j++;

	// skipblancs(j);
	if (!isdigit((unsigned char)*j)) {
		*next = j;
		return false;
	}

	if (*j == '0') {
		*next = ++j;
		return true;
	}

	for(; *j; j++)
		if (!(isdigit((unsigned char) *j)))
			break;
	*next = j;

	return true;
}

static bool
JSONfractionParser(const char *j, const char **next) {
	if (*j != '.')
		return false;

	// skip the period character
	j++;
	for (; *j; j++)
		if (!isdigit((unsigned char)*j))
			break;
	*next = j;

	return true;
}

static bool
JSONexponentParser(const char *j, const char **next) {
	const char *s = j;
	bool saw_digit = false;

	if (*j != 'e' && *j != 'E') {
		return false;
	}

	j++;
	if (*j == '-' || *j == '+')
		j++;

	for (; *j; j++) {
		if (!isdigit((unsigned char)*j))
			break;
		saw_digit = true;
	}


	if (!saw_digit) {
		j = s;
		return false;
	}


	*next = j;

	return true;
}

static str
JSONnumberParser(const char *j, const char **next)
{
	if (!JSONintegerParser(j, next)) {
		throw(MAL, "json.parser", "Number expected");
	}

	j = *next;
	// backup = j;
	// skipblancs(j);

	if (!JSONfractionParser(j, next)) {
		*next = j;
	}

	j = *next;

	if (!JSONexponentParser(j, next)) {
		*next = j;
	}
	return MAL_SUCCEED;
}

static int
JSONtoken(JSON *jt, const char *j, const char **next)
{
	str msg;
	int nxt, idx = JSONnew(jt);
	const char *string_start = j;

	if (jt->error)
		return idx;
	skipblancs(j);
	switch (*j) {
	case '{':
		jt->elm[idx].kind = JSON_OBJECT;
		jt->elm[idx].value = j;
		j++;
		while (*j) {
			skipblancs(j);
			if (*j == '}')
				break;
			nxt = JSONtoken(jt, j, next);
			if (jt->error)
				return idx;
			j = *next;
			skipblancs(j);
			if (jt->elm[nxt].kind != JSON_STRING || *j != ':') {
				jt->error = createException(MAL, "json.parser", "JSON syntax error: element expected at offset %td", jt->elm[nxt].value - string_start);
				return idx;
			}
			j++;
			skipblancs(j);
			jt->elm[nxt].kind = JSON_ELEMENT;
			/* do in two steps since JSONtoken may realloc jt->elm */
			int chld = JSONtoken(jt, j, next);
			if (jt->error)
				return idx;
			jt->elm[nxt].child = chld;
			jt->elm[nxt].value++;
			jt->elm[nxt].valuelen -= 2;
			JSONappend(jt, idx, nxt);
			if (jt->error)
				return idx;
			j = *next;
			skipblancs(j);
			if (*j == '}')
				break;
			if (*j != '}' && *j != ',') {
				jt->error = createException(MAL, "json.parser", "JSON syntax error: ',' or '}' expected at offset %td", j - string_start);
				return idx;
			}
			j++;
		}
		if (*j != '}') {
			jt->error = createException(MAL, "json.parser", "JSON syntax error: '}' expected at offset %td", j - string_start);
			return idx;
		} else
			j++;
		*next = j;
		jt->elm[idx].valuelen = *next - jt->elm[idx].value;
		return idx;
	case '[':
		jt->elm[idx].kind = JSON_ARRAY;
		jt->elm[idx].value = j;
		j++;
		while (*j) {
			skipblancs(j);
			if (*j == ']')
				break;
			nxt = JSONtoken(jt, j, next);
			if (jt->error)
				return idx;
			switch (jt->elm[nxt].kind) {
			case JSON_ELEMENT:{
				int k = JSONnew(jt);
				if (jt->error)
					return idx;
				jt->elm[k].kind = JSON_OBJECT;
				jt->elm[k].child = nxt;
				nxt = k;
			}
				/* fall through */
			case JSON_OBJECT:
			case JSON_ARRAY:
				if (jt->elm[nxt].kind == JSON_OBJECT || jt->elm[nxt].kind == JSON_ARRAY) {
					int k = JSONnew(jt);
					if (jt->error)
						return idx;
					JSONappend(jt, idx, k);
					if (jt->error)
						return idx;
					jt->elm[k].kind = JSON_VALUE;
					jt->elm[k].child = nxt;
				}
				break;
			default:
				JSONappend(jt, idx, nxt);
				if (jt->error)
					return idx;
			}
			j = *next;
			skipblancs(j);
			if (*j == ']')
				break;
			if (jt->elm[nxt].kind == JSON_ELEMENT) {
				jt->error = createException(MAL, "json.parser", "JSON syntax error: Array value expected at offset %td", j - string_start);
				return idx;
			}
			if (*j != ']' && *j != ',') {
				jt->error = createException(MAL, "json.parser", "JSON syntax error: ',' or ']' expected at offset %td (context: %c%c%c)", j - string_start, *(j - 1), *j, *(j + 1));
				return idx;
			}
			j++;
			skipblancs(j);
		}
		if (*j != ']') {
			jt->error = createException(MAL, "json.parser", "JSON syntax error: ']' expected at offset %td", j - string_start);
		} else
			j++;
		*next = j;
		jt->elm[idx].valuelen = *next - jt->elm[idx].value;
		return idx;
	case '"':
		msg = JSONstringParser(j, next);
		if (msg) {
			jt->error = msg;
			return idx;
		}
		jt->elm[idx].kind = JSON_STRING;
		jt->elm[idx].value = j;
		jt->elm[idx].valuelen = *next - j;
		return idx;
	case 'n':
		if (strncmp("null", j, 4) == 0) {
			*next = j + 4;
			jt->elm[idx].kind = JSON_NULL;
			jt->elm[idx].value = j;
			jt->elm[idx].valuelen = 4;
			return idx;
		}
		jt->error = createException(MAL, "json.parser", "JSON syntax error: NULL expected at offset %td", j - string_start);
		return idx;
	case 't':
		if (strncmp("true", j, 4) == 0) {
			*next = j + 4;
			jt->elm[idx].kind = JSON_NUMBER;
			jt->elm[idx].value = j;
			jt->elm[idx].valuelen = 4;
			return idx;
		}
		jt->error = createException(MAL, "json.parser", "JSON syntax error: True expected at offset %td", j - string_start);
		return idx;
	case 'f':
		if (strncmp("false", j, 5) == 0) {
			*next = j + 5;
			jt->elm[idx].kind = JSON_NUMBER;
			jt->elm[idx].value = j;
			jt->elm[idx].valuelen = 5;
			return idx;
		}
		jt->error = createException(MAL, "json.parser", "JSON syntax error: False expected at offset %td", j - string_start);
		return idx;
	default:
		if (*j == '-' || isdigit((unsigned char) *j)) {
			jt->elm[idx].value = j;
			msg = JSONnumberParser(j, next);
			if (msg)
				jt->error = msg;
			jt->elm[idx].kind = JSON_NUMBER;
			jt->elm[idx].valuelen = *next - jt->elm[idx].value;
			return idx;
		}
		jt->error = createException(MAL, "json.parser", "JSON syntax error: value expected at offset %td", j - string_start);
		return idx;
	}
}


static JSON *
JSONparse(const char *j)
{
	JSON *jt = JSONnewtree();

	if (jt == NULL)
		return NULL;
	skipblancs(j);
	JSONtoken(jt, j, &j);
	if (jt->error)
		return jt;
	skipblancs(j);
	if (*j)
		jt->error = createException(MAL, "json.parser", "JSON syntax error: json parse failed");
	return jt;
}

static str
JSONlength(int *ret, json *j)
{
	int i, cnt = 0;
	JSON *jt = JSONparse(*j);

	CHECK_JSON(jt);
	for (i = jt->elm[0].next; i; i = jt->elm[i].next)
		cnt++;
	*ret = cnt;
	JSONfree(jt);
	return MAL_SUCCEED;
}

static str
JSONfilterArrayDefault(json *ret, json *js, lng index, str other)
{
	char expr[BUFSIZ], *s = expr;
	snprintf(expr, BUFSIZ, "[" LLFMT "]", index);
	return JSONfilterInternal(ret, js, &s, other);
}

static str
JSONfilterArray_bte(json *ret, json *js, bte *index)
{
	return JSONfilterArrayDefault(ret, js, (lng) *index, 0);
}

static str
JSONfilterArrayDefault_bte(json *ret, json *js, bte *index, str *other)
{
	return JSONfilterArrayDefault(ret, js, (lng) *index, *other);
}

static str
JSONfilterArray_sht(json *ret, json *js, sht *index)
{
	return JSONfilterArrayDefault(ret, js, (lng) *index, 0);
}

static str
JSONfilterArrayDefault_sht(json *ret, json *js, sht *index, str *other)
{
	return JSONfilterArrayDefault(ret, js, (lng) *index, *other);
}

static str
JSONfilterArray_int(json *ret, json *js, int *index)
{
	return JSONfilterArrayDefault(ret, js, (lng) *index, 0);
}

static str
JSONfilterArrayDefault_int(json *ret, json *js, int *index, str *other)
{
	return JSONfilterArrayDefault(ret, js, (lng) *index, *other);
}

static str
JSONfilterArray_lng(json *ret, json *js, lng *index)
{
	return JSONfilterArrayDefault(ret, js, (lng) *index, 0);
}

static str
JSONfilterArrayDefault_lng(json *ret, json *js, lng *index, str *other)
{
	return JSONfilterArrayDefault(ret, js, (lng) *index, *other);
}

#ifdef HAVE_HGE
static str
JSONfilterArray_hge(json *ret, json *js, hge *index)
{
	if (*index < (hge) GDK_lng_min || *index > (hge) GDK_lng_max)
		throw(MAL, "json.filter", "index out of range");
	return JSONfilterArrayDefault(ret, js, (lng) *index, 0);
}

static str
JSONfilterArrayDefault_hge(json *ret, json *js, hge *index, str *other)
{
	if (*index < (hge) GDK_lng_min || *index > (hge) GDK_lng_max)
		throw(MAL, "json.filter", "index out of range");
	return JSONfilterArrayDefault(ret, js, (lng) *index, *other);
}
#endif

static str
JSONfilter(json *ret, json *js, str *expr)
{
	return JSONfilterInternal(ret, js, expr, 0);
}

// glue all values together with an optional separator
// The json string should be valid

static char *
JSONplaintext(char **r, size_t *l, size_t *ilen, JSON *jt, int idx, str sep, size_t sep_len)
{
	int i;
	unsigned int j, u;

	switch (jt->elm[idx].kind) {
	case JSON_OBJECT:
		for (i = jt->elm[idx].next; i; i = jt->elm[i].next)
			if (jt->elm[i].child)
				*r = JSONplaintext(r, l, ilen, jt, jt->elm[i].child, sep, sep_len);
		break;
	case JSON_ARRAY:
		for (i = jt->elm[idx].next; i; i = jt->elm[i].next)
			*r = JSONplaintext(r, l, ilen, jt, i, sep, sep_len);
		break;
	case JSON_ELEMENT:
	case JSON_VALUE:
		if (jt->elm[idx].child)
			*r = JSONplaintext(r, l, ilen, jt, jt->elm[idx].child, sep, sep_len);
		break;
	case JSON_STRING:
		// Make sure there is enough space for the value plus the separator plus the NULL byte
		if (*l < jt->elm[idx].valuelen - 2 + sep_len + 1) {
			char *p = *r - *ilen + *l;
			*ilen *= 2;
			*r = GDKrealloc(p, *ilen);
			*r += *l;
			*l = *ilen - *l;
		}
		for (j = 1; j < jt->elm[idx].valuelen - 1; j++) {
			if (jt->elm[idx].value[j] == '\\') {
				switch (jt->elm[idx].value[++j]) {
				case '"':
				case '\\':
				case '/':
					*(*r)++ = jt->elm[idx].value[j];
					(*l)--;
					break;
				case 'b':
					*(*r)++ = '\b';
					(*l)--;
					break;
				case 'f':
					*(*r)++ = '\f';
					(*l)--;
					break;
				case 'r':
					*(*r)++ = '\r';
					(*l)--;
					break;
				case 'n':
					*(*r)++ = '\n';
					(*l)--;
					break;
				case 't':
					*(*r)++ = '\t';
					(*l)--;
					break;
				case 'u':
					u = 0;
					for (int i = 0;i < 4; i++) {
						char c = jt->elm[idx].value[++j];
						u <<= 4;
						if ('0' <= c && c <= '9')
							u |= c - '0';
						else if ('a' <= c && c <= 'f')
							u |= c - 'a' + 10;
						else /* if ('A' <= c && c <= 'F') */
							u |= c - 'A' + 10;
					}
					if (u <= 0x7F) {
						*(*r)++ = (char) u;
						(*l)--;
					} else if (u <= 0x7FF) {
						*(*r)++ = 0xC0 | (u >> 6);
						*(*r)++ = 0x80 | (u & 0x3F);
						(*l) -= 2;
					} else if ((u & 0xFC00) == 0xD800) {
						/* high surrogate; must be followed by low surrogate */
						*(*r)++ = 0xF0 | (((u & 0x03C0) + 0x0040) >> 8);
						*(*r)++ = 0x80 | ((((u & 0x03C0) + 0x0040) >> 2) & 0x3F);
						**r = 0x80 | ((u & 0x0003) << 4); /* no increment */
						(*l) -= 2;
					} else if ((u & 0xFC00) == 0xDC00) {
						/* low surrogate; must follow high surrogate */
						*(*r)++ |= (u & 0x03C0) >> 6; /* amend last value */
						*(*r)++ = 0x80 | (u & 0x3F);
						(*l) -= 2;
					} else /* if (u <= 0xFFFF) */ {
						*(*r)++ = 0xE0 | (u >> 12);
						*(*r)++ = 0x80 | ((u >> 6) & 0x3F);
						*(*r)++ = 0x80 | (u & 0x3F);
						(*l) -= 3;
					}
				}
			} else {
				*(*r)++ = jt->elm[idx].value[j];
				(*l)--;
			}
		}
		memcpy(*r, sep, sep_len);
		*l -= sep_len;
		*r += sep_len;
		break;
	default:
		if (*l < jt->elm[idx].valuelen + sep_len + 1) {
			size_t offset = *ilen - *l;
			char *p = *r - offset;
			*ilen *= 2;
			*r = GDKrealloc(p, *ilen);
			*r += offset;
			*l = *ilen - offset;
		}
		memcpy(*r, jt->elm[idx].value, jt->elm[idx].valuelen);
		*l -= jt->elm[idx].valuelen;
		*r += jt->elm[idx].valuelen;
		memcpy(*r, sep, sep_len);
		*l -= sep_len;
		*r += sep_len;
	}
	assert(*l > 0);
	**r = 0;
	return *r;
}

static str
JSONjson2textSeparator(str *ret, json *js, str *sep)
{
	JSON *jt;
	size_t l, ilen, sep_len;
	str s;

	jt = JSONparse(*js);

	CHECK_JSON(jt);
	sep_len = strlen(*sep);
	ilen = l = strlen(*js) + 1;
	s = GDKmalloc(l);
	if(s == NULL) {
		JSONfree(jt);
		throw(MAL,"json2txt", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	s = JSONplaintext(&s, &l, &ilen, jt, 0, *sep, sep_len);
	s -= ilen - l;
	l = strlen(s);
	if (l && sep_len)
		s[l - sep_len] = 0;
	*ret = s;
	JSONfree(jt);
	return MAL_SUCCEED;
}

static str
JSONjson2text(str *ret, json *js)
{
	char *sep = " ";
	return JSONjson2textSeparator(ret, js, &sep);
}

static str
JSONjson2numberInternal(void **ret, json *js, void (*str2num)(void **ret, const char *nptr, size_t len)) {
	JSON *jt;

	jt = JSONparse(*js);
	CHECK_JSON(jt);
	switch (jt->elm[0].kind) {
	case JSON_NUMBER:
		str2num(ret, jt->elm[0].value, jt->elm[0].valuelen);
		break;
	case JSON_ARRAY:
		if (jt->free == 2) {
			str2num(ret, jt->elm[1].value, jt->elm[1].valuelen);
		}
		else {
			*ret = NULL;
		}
		break;
	case JSON_OBJECT:
		if (jt->free == 3) {
			str2num(ret, jt->elm[2].value, jt->elm[2].valuelen);
		}
		else {
			*ret = NULL;
		}
		break;
	default:
		*ret = NULL;
	}
	JSONfree(jt);

	return MAL_SUCCEED;
}

static void
strtod_wrapper(void **ret, const char *nptr, size_t len) {
	char *rest;
	dbl val;

	val = strtod(nptr, &rest);
	if(rest && (size_t)(rest - nptr) != len) {
		*ret = NULL;
	}
	else {
		**(dbl **)ret = val;
	}
}

static  void
strtol_wrapper(void **ret, const char *nptr, size_t len) {
	char *rest;
	lng val;

	val = strtol(nptr, &rest, 0);
	if(rest && (size_t)(rest - nptr) != len) {
		*ret = NULL;
	}
	else {
		**(lng **)ret = val;
	}
}

static str
JSONjson2number(dbl *ret, json *js)
{
	dbl val = 0;
	dbl *val_ptr = &val;
	str tmp;
	rethrow(__func__, tmp, JSONjson2numberInternal((void **)&val_ptr, js, strtod_wrapper));

	if (val_ptr == NULL) {
		*ret = dbl_nil;
	}
	else {
		*ret = val;
	}

	return MAL_SUCCEED;
}

static str
JSONjson2integer(lng *ret, json *js)
{
	lng val = 0;
	lng *val_ptr = &val;
	str tmp;

	rethrow(__func__, tmp, JSONjson2numberInternal((void **)&val_ptr, js, strtol_wrapper));
	if (val_ptr == NULL) {
		*ret = lng_nil;
	}
	else {
		*ret = val;
	}

	return MAL_SUCCEED;
}

static str
JSONunfoldContainer(JSON *jt, int idx, BAT *bo, BAT *bk, BAT *bv, oid *o)
{
	int i, last;
	int cnt = 0;
	char *r;

	last = jt->elm[idx].tail;
	if (jt->elm[idx].kind == JSON_OBJECT) {
		for (i = jt->elm[idx].next; i; i = jt->elm[i].next) {
			if ((r = JSONgetValue(jt, i)) == NULL)
				goto memfail;
			if (BUNappend(bk, r, false) != GDK_SUCCEED) {
				GDKfree(r);
				goto memfail;
			}
			GDKfree(r);
			if ((r = JSONgetValue(jt, jt->elm[i].child)) == NULL)
				goto memfail;
			if (BUNappend(bv, r, false) != GDK_SUCCEED) {
				GDKfree(r);
				goto memfail;
			}
			GDKfree(r);
			if (bo) {
				if (BUNappend(bo, o, false) != GDK_SUCCEED)
					goto memfail;
			}
			(*o)++;
			if (i == last)
				break;
		}
	} else if (jt->elm[idx].kind == JSON_ARRAY) {
		for (i = jt->elm[idx].next; i; i = jt->elm[i].next) {
			if (BUNappend(bk, str_nil, false) != GDK_SUCCEED)
				goto memfail;
			if (jt->elm[i].kind == JSON_VALUE)
				r = JSONgetValue(jt, jt->elm[i].child);
			else
				r = JSONgetValue(jt, i);
			if (r == NULL)
				goto memfail;
			if (BUNappend(bv, r, false) != GDK_SUCCEED) {
				GDKfree(r);
				goto memfail;
			}
			GDKfree(r);
			if (bo) {
				if (BUNappend(bo, o, false) != GDK_SUCCEED)
					goto memfail;
			}
			(*o)++;
			cnt++;
			if (i == last)
				break;
		}
	}
	return MAL_SUCCEED;

  memfail:
	throw(MAL, "json.unfold", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
JSONunfoldInternal(bat *od, bat *key, bat *val, json *js)
{
	BAT *bo = NULL, *bk, *bv;
	oid o = 0;
	str msg = MAL_SUCCEED;

	JSON *jt = JSONparse(*js);

	CHECK_JSON(jt);
	bk = COLnew(0, TYPE_str, 64, TRANSIENT);
	if (bk == NULL) {
		JSONfree(jt);
		throw(MAL, "json.unfold", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	bk->tsorted = true;
	bk->trevsorted = false;
	bk->tnonil = true;

	if (od) {
		bo = COLnew(0, TYPE_oid, 64, TRANSIENT);
		if (bo == NULL) {
			BBPreclaim(bk);
			JSONfree(jt);
			throw(MAL, "json.unfold", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		bo->tsorted = true;
		bo->trevsorted = false;
		bo->tnonil = true;
	}

	bv = COLnew(0, TYPE_json, 64, TRANSIENT);
	if (bv == NULL) {
		JSONfree(jt);
		BBPreclaim(bo);
		BBPreclaim(bk);
		throw(MAL, "json.unfold", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	bv->tsorted = true;
	bv->trevsorted = false;
	bv->tnonil = true;

	if (jt->elm[0].kind == JSON_ARRAY || jt->elm[0].kind == JSON_OBJECT)
		msg = JSONunfoldContainer(jt, 0, (od ? bo : 0), bk, bv, &o);
	else
		msg = createException(MAL, "json.unfold", "JSON object or array expected");
	JSONfree(jt);
	if (msg) {
		BBPreclaim(bk);
		BBPreclaim(bo);
		BBPreclaim(bv);
	} else {
		BBPkeepref(*key = bk->batCacheid);
		BBPkeepref(*val = bv->batCacheid);
		if (od)
			BBPkeepref(*od = bo->batCacheid);
	}
	return msg;
}



static str
JSONkeyTable(bat *ret, json *js)
{
	BAT *bn;
	char *r;
	int i;
	JSON *jt;

	jt = JSONparse(*js);		// already validated
	CHECK_JSON(jt);
	bn = COLnew(0, TYPE_str, 64, TRANSIENT);
	if (bn == NULL) {
		JSONfree(jt);
		throw(MAL, "json.keys", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	bn->tsorted = true;
	bn->trevsorted = false;
	bn->tnonil = true;

	for (i = jt->elm[0].next; i; i = jt->elm[i].next) {
		r = JSONgetValue(jt, i);
		if (r == NULL ||
			BUNappend(bn, r, false) != GDK_SUCCEED) {
			GDKfree(r);
			JSONfree(jt);
			BBPreclaim(bn);
			throw(MAL, "json.keys", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		GDKfree(r);
	}
	JSONfree(jt);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
JSONkeyArray(json *ret, json *js)
{
	char *result = NULL;
	str r;
	int i;
	JSON *jt;

	jt = JSONparse(*js);		// already validated

	CHECK_JSON(jt);
	if (jt->elm[0].kind == JSON_OBJECT) {
		for (i = jt->elm[0].next; i; i = jt->elm[i].next) {
			if (jt->elm[i].valuelen) {
				r = GDKzalloc(jt->elm[i].valuelen + 3);
				if (r == NULL) {
					JSONfree(jt);
					goto memfail;
				}
				strncpy(r, jt->elm[i].value - 1, jt->elm[i].valuelen + 2);
			} else {
				r = GDKstrdup("\"\"");
				if(r == NULL) {
					JSONfree(jt);
					goto memfail;
				}
			}
			result = JSONglue(result, r, ',');
			if (result == NULL) {
				JSONfree(jt);
				goto memfail;
			}
		}
		JSONfree(jt);
	} else {
		JSONfree(jt);
		throw(MAL, "json.keyarray", "Object expected");
	}
	r = GDKstrdup("[");
	if (r == NULL)
		goto memfail;
	result = JSONglue(r, result, 0);
	if (result == NULL)
		goto memfail;
	r = GDKstrdup("]");
	if (r == NULL)
		goto memfail;
	result = JSONglue(result, r, 0);
	if (result == NULL)
		goto memfail;
	*ret = result;
	return MAL_SUCCEED;

  memfail:
	GDKfree(result);
	throw(MAL, "json.keyarray", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}


static str
JSONvalueTable(bat *ret, json *js)
{
	BAT *bn;
	char *r;
	int i;
	JSON *jt;

	jt = JSONparse(*js);		// already validated
	CHECK_JSON(jt);
	bn = COLnew(0, TYPE_json, 64, TRANSIENT);
	if (bn == NULL) {
		JSONfree(jt);
		throw(MAL, "json.values", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	bn->tsorted = true;
	bn->trevsorted = false;
	bn->tnonil = true;

	for (i = jt->elm[0].next; i; i = jt->elm[i].next) {
		if (jt->elm[i].kind == JSON_ELEMENT)
			r = JSONgetValue(jt, jt->elm[i].child);
		else
			r = JSONgetValue(jt, i);
		if (r == NULL ||
			BUNappend(bn, r, false) != GDK_SUCCEED) {
			GDKfree(r);
			BBPreclaim(bn);
			JSONfree(jt);
			throw(MAL, "json.values", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		GDKfree(r);
	}
	JSONfree(jt);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
JSONvalueArray(json *ret, json *js)
{
	char *result = NULL;
	str r;
	int i;
	JSON *jt;

	jt = JSONparse(*js);		// already validated

	CHECK_JSON(jt);
	if (jt->elm[0].kind == JSON_OBJECT) {
		for (i = jt->elm[0].next; i; i = jt->elm[i].next) {
			r = JSONgetValue(jt, jt->elm[i].child);
			if (r == NULL) {
				JSONfree(jt);
				goto memfail;
			}
			result = JSONglue(result, r, ',');
			if (result == NULL) {
				JSONfree(jt);
				goto memfail;
			}
		}
		JSONfree(jt);
	} else {
		JSONfree(jt);
		throw(MAL, "json.valuearray", "Object expected");
	}
	r = GDKstrdup("[");
	if (r == NULL)
		goto memfail;
	result = JSONglue(r, result, 0);
	if (result == NULL)
		goto memfail;
	r = GDKstrdup("]");
	if (r == NULL)
		goto memfail;
	result = JSONglue(result, r, 0);
	if (result == NULL)
		goto memfail;
	*ret = result;
	return MAL_SUCCEED;

  memfail:
	GDKfree(result);
	throw(MAL, "json.valuearray", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static BAT **
JSONargumentlist(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, error = 0, bats = 0;
	BUN cnt = 0;
	BAT **bl;

	bl = (BAT **) GDKzalloc(sizeof(*bl) * pci->argc);
	if (bl == NULL)
		return NULL;
	for (i = pci->retc; i < pci->argc; i++)
		if (isaBatType(getArgType(mb, pci, i))) {
			bats++;
			bl[i] = BATdescriptor(stk->stk[getArg(pci, i)].val.bval);
			if (bl[i] == NULL || (cnt > 0 && BATcount(bl[i]) != cnt)) {
				error = 1;
				break;
			}
			cnt = BATcount(bl[i]);
		}
	if (error || bats == 0) {
		for (i = pci->retc; i < pci->argc; i++)
			if (bl[i])
				BBPunfix(bl[i]->batCacheid);
		GDKfree(bl);
		return NULL;
	}
	return bl;
}

static void
JSONfreeArgumentlist(BAT **bl, InstrPtr pci)
{
	int i;

	for (i = pci->retc; i < pci->argc; i++)
		if (bl[i])
			BBPunfix(bl[i]->batCacheid);
	GDKfree(bl);
}

static str
JSONrenderRowObject(BAT **bl, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, BUN idx)
{
	int i, tpe;
	char *row, *row2, *name = 0, *val = 0;
	size_t len, lim, l;
	void *p;
	BATiter bi;

	row = GDKmalloc(lim = BUFSIZ);
	if (row == NULL)
		return NULL;
	row[0] = '{';
	row[1] = 0;
	len = 1;
	for (i = pci->retc; i < pci->argc; i += 2) {
		name = stk->stk[getArg(pci, i)].val.sval;
		bi = bat_iterator(bl[i + 1]);
		p = BUNtail(bi, idx);
		tpe = getBatType(getArgType(mb, pci, i + 1));
		if ((val = ATOMformat(tpe, p)) == NULL) {
			GDKfree(row);
			return NULL;
		}
		if (strncmp(val, "nil", 3) == 0) {
			GDKfree(val);
			val = NULL;
			l = 4;
		} else {
			l = strlen(val);
		}
		l += strlen(name) + 4;
		while (l > lim - len)
			lim += BUFSIZ;
		row2 = GDKrealloc(row, lim);
		if (row2 == NULL) {
			GDKfree(row);
			GDKfree(val);
			return NULL;
		}
		row = row2;
		snprintf(row + len, lim - len, "\"%s\":%s,", name, val ? val : "null");
		len += l;
		GDKfree(val);
	}
	if (row[1])
		row[len - 1] = '}';
	else {
		row[1] = '}';
		row[2] = 0;
	}
	return row;
}

static str
JSONrenderobject(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT **bl;
	char *result, *row;
	int i;
	size_t len, lim, l;
	json *ret;
	BUN j, cnt;

	(void) cntxt;
	bl = JSONargumentlist(mb, stk, pci);
	if (bl == 0)
		throw(MAL, "json.renderobject", "Non-aligned BAT sizes");
	for (i = pci->retc; i < pci->argc; i += 2) {
		if (getArgType(mb, pci, i) != TYPE_str) {
			JSONfreeArgumentlist(bl, pci);
			throw(MAL, "json.renderobject", "Keys missing");
		}
	}

	cnt = BATcount(bl[pci->retc + 1]);
	result = (char *) GDKmalloc(lim = BUFSIZ);
	if (result == NULL) {
		JSONfreeArgumentlist(bl, pci);
		throw(MAL,"json.renderobject", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	result[0] = '[';
	result[1] = 0;
	len = 1;

	for (j = 0; j < cnt; j++) {
		char *result2;
		row = JSONrenderRowObject(bl, mb, stk, pci, j);
		if (row == NULL)
			goto memfail;
		l = strlen(row);
		while (l + 2 > lim - len)
			lim = cnt * l <= lim ? cnt * l : lim + BUFSIZ;
		result2 = GDKrealloc(result, lim);
		if (result2 == NULL)
			goto memfail;
		result = result2;
		strcpy(result + len, row);
		GDKfree(row);
		len += l;
		result[len++] = ',';
		result[len] = 0;
	}
	result[len - 1] = ']';
	ret = getArgReference_TYPE(stk, pci, 0, json);
	*ret = result;
	JSONfreeArgumentlist(bl, pci);
	return MAL_SUCCEED;

  memfail:
	GDKfree(result);
	GDKfree(row);
	JSONfreeArgumentlist(bl, pci);
	throw(MAL,"json.renderobject", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
JSONrenderRowArray(BAT **bl, MalBlkPtr mb, InstrPtr pci, BUN idx)
{
	int i, tpe;
	char *row, *row2, *val = 0;
	size_t len, lim, l;
	void *p;
	BATiter bi;

	row = GDKmalloc(lim = BUFSIZ);
	if (row == NULL)
		return NULL;
	row[0] = '[';
	row[1] = 0;
	len = 1;
	for (i = pci->retc; i < pci->argc; i++) {
		bi = bat_iterator(bl[i]);
		p = BUNtail(bi, idx);
		tpe = getBatType(getArgType(mb, pci, i));
		if ((val = ATOMformat(tpe, p)) == NULL) {
			goto memfail;
		}
		if (strcmp(val, "nil") == 0) {
			GDKfree(val);
			val = NULL;
			l = 4;
		} else {
			l = strlen(val);
		}
		while (len + l > lim)
			lim += BUFSIZ;
		row2 = GDKrealloc(row, lim);
		if (row2 == NULL)
			goto memfail;
		row = row2;
		snprintf(row + len, lim - len, "%s,", val ? val : "null");
		len += l + 1;
		GDKfree(val);
		val = NULL;
	}
	if (row[1])
		row[len - 1] = ']';
	else {
		row[1] = '}';
		row[2] = 0;
	}
	return row;

  memfail:
	GDKfree(row);
	GDKfree(val);
	return NULL;
}

static str
JSONrenderarray(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT **bl;
	char *result, *row;
	size_t len, lim, l;
	str *ret;
	BUN j, cnt;

	(void) cntxt;
	bl = JSONargumentlist(mb, stk, pci);
	if (bl == 0)
		throw(MAL, "json.renderrray", "Non-aligned BAT sizes");

	cnt = BATcount(bl[pci->retc + 1]);
	result = GDKmalloc(lim = BUFSIZ);
	if( result == NULL) {
		goto memfail;
	}
	result[0] = '[';
	result[1] = 0;
	len = 1;

	for (j = 0; j < cnt; j++) {
		char *result2;
		row = JSONrenderRowArray(bl, mb, pci, j);
		if (row == NULL) {
			goto memfail;
		}
		l = strlen(row);
		while (l + 2 > lim - len)
			lim = cnt * l <= lim ? cnt * l : lim + BUFSIZ;
		result2 = GDKrealloc(result, lim);
		if (result2 == NULL) {
			GDKfree(row);
			goto memfail;
		}
		result = result2;
		strcpy(result + len, row);
		GDKfree(row);
		len += l;
		result[len++] = ',';
		result[len] = 0;
	}
	result[len - 1] = ']';
	ret = getArgReference_TYPE(stk, pci, 0, json);
	*ret = result;
	JSONfreeArgumentlist(bl, pci);
	return MAL_SUCCEED;

  memfail:
	GDKfree(result);
	JSONfreeArgumentlist(bl, pci);
	throw(MAL,"json.renderArray", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
JSONfoldKeyValue(str *ret, const bat *id, const bat *key, const bat *values)
{
	BAT *bo = 0, *bk = 0, *bv;
	BATiter bki, bvi;
	int tpe;
	char *row, *val = 0, *nme = 0;
	BUN i, cnt;
	size_t len, lim, l;
	void *p;
	oid o = 0;

	if (key) {
		bk = BATdescriptor(*key);
		if (bk == NULL) {
			throw(MAL, "json.fold", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
	}

	bv = BATdescriptor(*values);
	if (bv == NULL) {
		if (bk)
			BBPunfix(bk->batCacheid);
		throw(MAL, "json.fold", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	tpe = bv->ttype;
	cnt = BATcount(bv);
	bki = bat_iterator(bk);
	bvi = bat_iterator(bv);
	if (id) {
		bo = BATdescriptor(*id);
		if (bo == NULL) {
			if (bk)
				BBPunfix(bk->batCacheid);
			BBPunfix(bv->batCacheid);
			throw(MAL, "json.nest", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
	}

	row = GDKmalloc(lim = BUFSIZ);
	if (row == NULL) {
		goto memfail;
	}
	row[0] = '[';
	row[1] = 0;
	len = 1;
	if (id) {
		o = BUNtoid(bo, 0);
	}

	for (i = 0; i < cnt; i++) {
		if (id && bk) {
			if (BUNtoid(bo, i) != o) {
				snprintf(row + len, lim - len, ", ");
				len += 2;
				o = BUNtoid(bo, i);
			}
		}

		if (bk) {
			nme = (str) BUNtvar(bki, i);
			l = strlen(nme);
			while (l + 3 > lim - len)
				lim = (lim / (i + 1)) * cnt + BUFSIZ + l + 3;
			p = GDKrealloc(row, lim);
			if (p == NULL) {
				goto memfail;
			}
			row = p;
			if (!strNil(nme)) {
				snprintf(row + len, lim - len, "\"%s\":", nme);
				len += l + 3;
			}
		}

		bvi = bat_iterator(bv);
		p = BUNtail(bvi, i);
		if (tpe == TYPE_json)
			val = p;
		else {
			if ((val = ATOMformat(tpe, p))  == NULL)
				goto memfail;
			if (strcmp(val, "nil") == 0) {
				GDKfree(val);
				val = NULL;
			}
		}
		l = val ? strlen(val) : 4;
		while (l > lim - len)
			lim = (lim / (i + 1)) * cnt + BUFSIZ + l + 3;
		p = GDKrealloc(row, lim);
		if (p == NULL) {
			if (tpe != TYPE_json)
				GDKfree(val);
			goto memfail;
		}
		row = p;
		strncpy(row + len, val ? val : "null", l);
		len += l;
		row[len++] = ',';
		row[len] = 0;
		if (tpe != TYPE_json)
			GDKfree(val);
	}
	if (row[1]) {
		row[len - 1] = ']';
		row[len] = 0;
	} else {
		row[1] = ']';
		row[2] = 0;
	}
	if (bo)
		BBPunfix(bo->batCacheid);
	if (bk)
		BBPunfix(bk->batCacheid);
	BBPunfix(bv->batCacheid);
	*ret = row;
	return MAL_SUCCEED;

  memfail:
	GDKfree(row);
	if (bo)
		BBPunfix(bo->batCacheid);
	if (bk)
		BBPunfix(bk->batCacheid);
	BBPunfix(bv->batCacheid);
	throw(MAL, "json.fold", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
JSONunfold(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *id = 0, *key = 0, *val = 0;
	json *js;

	(void) cntxt;
	(void) mb;

	switch (pci->retc) {
	case 2:
		key = getArgReference_bat(stk, pci, 0);
		val = getArgReference_bat(stk, pci, 1);
		break;
	case 3:
		id = getArgReference_bat(stk, pci, 0);
		key = getArgReference_bat(stk, pci, 1);
		val = getArgReference_bat(stk, pci, 2);
		break;
	default:
		assert(0);
		throw(MAL, "json.unfold", ILLEGAL_ARGUMENT);
	}
	js = getArgReference_TYPE(stk, pci, pci->retc, json);
	return JSONunfoldInternal(id, key, val, js);
}

static str
JSONfold(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *id = 0, *key = 0, *val = 0;
	str *ret;

	(void) cntxt;
	(void) mb;

	assert(pci->retc == 1);
	switch (pci->argc - pci->retc) {
	case 1:
		val = getArgReference_bat(stk, pci, 1);
		break;
	case 2:
		key = getArgReference_bat(stk, pci, 1);
		val = getArgReference_bat(stk, pci, 2);
		break;
	case 3:
		id = getArgReference_bat(stk, pci, 1);
		key = getArgReference_bat(stk, pci, 2);
		val = getArgReference_bat(stk, pci, 3);
		break;
	default:
		assert(0);
		throw(MAL, "json.fold", ILLEGAL_ARGUMENT);
	}
	ret = getArgReference_TYPE(stk, pci, 0, json);
	return JSONfoldKeyValue(ret, id, key, val);
}

static str
JSONgroupStr(str *ret, const bat *bid)
{
	BAT *b;
	BUN p, q;
	const char *t = NULL;
	size_t len, size = BUFSIZ, offset, cnt = 0;
	str buf = GDKmalloc(size);
	BATiter bi;
	const char *err = NULL;
	char temp[128] = "";
	const double *val = NULL;

	if (buf == NULL)
		throw(MAL, "json.group", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(buf);
		throw(MAL, "json.agg", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	assert(b->ttype == TYPE_str || b->ttype == TYPE_dbl);

	strcpy(buf, str_nil);
	offset = 0;
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		int n = 0, nil = 0;

		switch (b->ttype) {
		case TYPE_str:
			t = (const char *) BUNtvar(bi, p);
			nil = (strNil(t));
			break;
		case TYPE_dbl:
			val = (const double *) BUNtloc(bi, p);
			nil = is_dbl_nil(*val);
			if (!nil)
				snprintf(temp, sizeof(temp), "%f", *val);
			t = (const char *) temp;
			break;
		}

		if (nil)
			continue;
		if (!cnt)
			offset = snprintf(buf, size, "[ ");
		len = strlen(t) + 1 + 4; /* closing bracket and optional ',' */
		if (len >= size - offset) {
			str nbuf;
			size += len + 128;
			nbuf = GDKrealloc(buf, size);
			if (nbuf == NULL) {
				err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
				goto failed;
			}
			buf = nbuf;
		}
		if (cnt)
			offset += snprintf(buf + offset, size - offset, ", ");
		switch (b->ttype) {
		case TYPE_str:
			n = snprintf(buf + offset, size - offset, "\"%s\"", t);
			break;
		case TYPE_dbl:
			n = snprintf(buf + offset, size - offset, "%s", t);
			break;
		}
		cnt++;
		offset += n;
	}
	if (cnt)
		offset += snprintf(buf + offset, size - offset, " ]");
	BBPunfix(b->batCacheid);
	*ret = buf;
	return MAL_SUCCEED;
  failed:
	BBPunfix(b->batCacheid);
	GDKfree(buf);
	throw(MAL, "json.agg", "%s", err);
}

static const char *
JSONjsonaggr(BAT **bnp, BAT *b, BAT *g, BAT *e, BAT *s, int skip_nils)
{
	BAT *bn = NULL, *t1, *t2 = NULL;
	BATiter bi;
	oid min, max;
	BUN ngrp;
	BUN nils = 0;
	int isnil;
	struct canditer ci;
	BUN ncand;
	const char *v = NULL;
	const oid *grps, *map;
	oid mapoff = 0;
	oid prev;
	BUN p, q;
	int freeb = 0, freeg = 0;
	char *buf = NULL, *buf2;
	size_t buflen, maxlen, len;
	const char *err;
	char temp[128] = "";
	const double *val = NULL;

	assert(b->ttype == TYPE_str || b->ttype == TYPE_dbl);
	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		return err;
	}
	if (BATcount(b) == 0 || ngrp == 0) {
		bn = BATconstant(ngrp == 0 ? 0 : min, TYPE_str, ATOMnilptr(TYPE_str), ngrp, TRANSIENT);
		if (bn == NULL)
			return SQLSTATE(HY013) MAL_MALLOC_FAIL;
		*bnp = bn;
		return NULL;
	}
	if (s) {
		b = BATproject(s, b);
		if (b == NULL) {
			err = "internal project failed";
			goto out;
		}
		freeb = 1;
		if (g) {
			g = BATproject(s, g);
			if (g == NULL) {
				err = "internal project failed";
				goto out;
			}
			freeg = 1;
		}
	}

	maxlen = BUFSIZ;
	if ((buf = GDKmalloc(maxlen)) == NULL) {
		err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
		goto out;
	}
	buflen = 0;
	bn = COLnew(min, TYPE_str, ngrp, TRANSIENT);
	if (bn == NULL) {
		err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
		goto out;
	}
	bi = bat_iterator(b);
	if (g) {
		/* stable sort g */
		if (BATsort(&t1, &t2, NULL, g, NULL, NULL, false, false, true) != GDK_SUCCEED) {
			err = "internal sort failed";
			goto out;
		}
		if (freeg)
			BBPunfix(g->batCacheid);
		g = t1;
		freeg = 1;
		if (t2->ttype == TYPE_void) {
			map = NULL;
		} else {
			map = (const oid *) Tloc(t2, 0);
			mapoff = t2->tseqbase;
		}
		if (g && BATtdense(g)) {
			for (p = 0, q = BATcount(g); p < q; p++) {
				switch (b->ttype) {
				case TYPE_str:
					v = (const char *) BUNtvar(bi, (map ? (BUN) map[p] - mapoff : p));
					break;
				case TYPE_dbl:
					val = (const double *) BUNtloc(bi, (map ? (BUN) map[p] - mapoff : p));
					if (!is_dbl_nil(*val)) {
						snprintf(temp, sizeof(temp), "%f", *val);
						v = (const char *) temp;
					} else {
						v = NULL;
					}
					break;
				}
				if (strNil(v)) {
					if (skip_nils) {
						/*
						 * if q is 1 and the value is
						 * null, then we need to fill
						 * in a value. Otherwise
						 * BATproject will fail.
						 */
						if ((p == 0) && (q == 1)) {
							strcpy(buf, "[ null ]");
							isnil = 1;
						} else {
							continue;
						}
					} else {
						strcpy(buf, str_nil);
						isnil = 1;
					}
				} else {
					len = strlen(v);
					if (len + 7 >= maxlen) {
						maxlen += len + BUFSIZ;
						buf2 = GDKrealloc(buf, maxlen);
						if (buf2 == NULL) {
							err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
							goto bunins_failed;
						}
						buf = buf2;
					}
					switch (b->ttype) {
					case TYPE_str:
						snprintf(buf, maxlen, "[ \"%s\" ]", v);
						break;
					case TYPE_dbl:
						snprintf(buf, maxlen, "[ %s ]", v);
						break;
					}
				}
				if (bunfastapp_nocheckVAR(bn, BUNlast(bn), buf, Tsize(bn)) != GDK_SUCCEED)
					goto bunins_failed;
			}
			bn->tnil = nils != 0;
			bn->tnonil = nils == 0;
			bn->tsorted = BATcount(bn) <= 1;
			bn->trevsorted = BATcount(bn) <= 1;
			bn->tkey = BATcount(bn) <= 1;
			goto out;
		}
		grps = (const oid *) Tloc(g, 0);
		prev = grps[0];
		isnil = 0;
		for (p = 0, q = BATcount(g); p <= q; p++) {
			if (p == 0) {
				strncpy(buf + buflen, "[ ", maxlen - buflen);
				buflen += 2;
			}
			if (p == q || grps[p] != prev) {
				strncpy(buf + buflen, " ]", maxlen - buflen);
				buflen += 2;
				while (BATcount(bn) < prev - min) {
					if (bunfastapp_nocheckVAR(bn, BUNlast(bn), str_nil, Tsize(bn)) != GDK_SUCCEED)
						goto bunins_failed;
					nils++;
				}
				if (bunfastapp_nocheckVAR(bn, BUNlast(bn), buf, Tsize(bn)) != GDK_SUCCEED)
					goto bunins_failed;
				nils += strNil(buf);
				strncpy(buf + buflen, str_nil, maxlen - buflen);
				buflen = 0;
				if (p == q)
					break;
				prev = grps[p];
				strncpy(buf + buflen, "[ ", maxlen - buflen);
				buflen += 2;
				isnil = 0;
			}
			if (isnil)
				continue;
			switch (b->ttype) {
			case TYPE_str:
				v = (const char *) BUNtvar(bi, (map ? (BUN) map[p] : p + mapoff));
				break;
			case TYPE_dbl:
				val = (const double *) BUNtloc(bi, (map ? (BUN) map[p] : p + mapoff));
				if (!is_dbl_nil(*val)) {
					snprintf(temp, sizeof(temp), "%f", *val);
					v = (const char *) temp;
				} else {
					v = NULL;
				}
				break;
			}
			if (strNil(v)) {
				if (skip_nils)
					continue;
				strncpy(buf, str_nil, buflen);
				isnil = 1;
			} else {
				len = strlen(v);
				if (len >= maxlen - buflen) {
					maxlen += len + BUFSIZ;
					buf2 = GDKrealloc(buf, maxlen);
					if (buf2 == NULL) {
						err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
						goto bunins_failed;
					}
					buf = buf2;
				}
				switch (b->ttype) {
				case TYPE_str:
					if (buflen == 2) {
						len = snprintf(buf + buflen, maxlen - buflen, "\"%s\"", v);
						buflen += len;
					} else {
						len = snprintf(buf + buflen, maxlen - buflen, ", \"%s\"", v);
						buflen += len;
					}
					break;
				case TYPE_dbl:
					if (buflen == 2) {
						len = snprintf(buf + buflen, maxlen - buflen, "%s", v);
						buflen += len;
					} else {
						len = snprintf(buf + buflen, maxlen - buflen, ", %s", v);
						buflen += len;
					}
					break;
				}
			}
		}
		BBPunfix(t2->batCacheid);
		t2 = NULL;
	} else {
		for (p = 0, q = p + BATcount(b); p < q; p++) {
			switch (b->ttype) {
			case TYPE_str:
				v = (const char *) BUNtvar(bi, p);
				break;
			case TYPE_dbl:
				val = (const double *) BUNtloc(bi, p);
				if (!is_dbl_nil(*val)) {
					snprintf(temp, sizeof(temp), "%f", *val);
					v = (const char *) temp;
				} else {
					v = NULL;
				}
				break;
			}

			if (strNil(v)) {
				if (skip_nils)
					continue;
				strncpy(buf, str_nil, buflen);
				nils++;
				break;
			}
			len = strlen(v);
			if (len >= maxlen - buflen) {
				maxlen += len + BUFSIZ;
				buf2 = GDKrealloc(buf, maxlen);
				if (buf2 == NULL) {
					err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
					goto bunins_failed;
				}
				buf = buf2;
			}
			switch (b->ttype) {
			case TYPE_str:
				if (buflen == 2) {
					len = snprintf(buf + buflen, maxlen - buflen, "\"%s\"", v);
					buflen += len;
				} else {
					len = snprintf(buf + buflen, maxlen - buflen, ", \"%s\"", v);
					buflen += len;
				}
				break;
			case TYPE_dbl:
				if (buflen == 2) {
					len = snprintf(buf + buflen, maxlen - buflen, "%s", v);
					buflen += len;
				} else {
					len = snprintf(buf + buflen, maxlen - buflen, ", %s", v);
					buflen += len;
				}
				break;
			}
		}
		if (bunfastapp_nocheckVAR(bn, BUNlast(bn), buf, Tsize(bn)) != GDK_SUCCEED)
			goto bunins_failed;
	}
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tkey = BATcount(bn) <= 1;

  out:
	if (bn)
		bn->theap->dirty |= BATcount(bn) > 0;
	if (t2)
		BBPunfix(t2->batCacheid);
	if (freeb)
		BBPunfix(b->batCacheid);
	if (freeg)
		BBPunfix(g->batCacheid);
	if (buf)
		GDKfree(buf);
	if (err && bn) {
		BBPreclaim(bn);
		bn = NULL;
	}
	*bnp = bn;
	return err;

  bunins_failed:
	if (err == NULL)
		err = SQLSTATE(HY013) MAL_MALLOC_FAIL;	/* insertion into result BAT failed */
	goto out;
}

static str
JSONsubjsoncand(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils)
{
	BAT *b, *g, *e, *s, *bn = NULL;
	const char *err;

	b = BATdescriptor(*bid);
	g = gid ? BATdescriptor(*gid) : NULL;
	e = eid ? BATdescriptor(*eid) : NULL;
	s = sid ? BATdescriptor(*sid) : NULL;
	if (b == NULL ||
		(gid != NULL && g == NULL) ||
		(eid != NULL && e == NULL) ||
		(sid != NULL && s == NULL)) {
		err = SQLSTATE(HY002) RUNTIME_OBJECT_MISSING;
	} else {
		err = JSONjsonaggr(&bn, b, g, e, s, *skip_nils);
	}
	if (b)
		BBPunfix(b->batCacheid);
	if (g)
		BBPunfix(g->batCacheid);
	if (e)
		BBPunfix(e->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (err != NULL)
		throw(MAL, "aggr.subjson", "%s", err);

	*retval = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

static str
JSONsubjson(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils)
{
	return JSONsubjsoncand(retval, bid, gid, eid, NULL, skip_nils);
}

#include "mel.h"
static mel_atom json_init_atoms[] = {
 { .name="json", .basetype="str", .fromstr=JSONfromString, .tostr=JSONtoString, },  { .cmp=NULL }
};
static mel_func json_init_funcs[] = {
 command("json", "new", JSONstr2json, false, "Convert string to its JSON. Dealing with escape characters", args(1,2, arg("",json),arg("j",str))),
 command("calc", "json", JSONstr2json, false, "Convert string to its JSON. Dealing with escape characters", args(1,2, arg("",json),arg("j",str))),
 command("json", "str", JSONjson2str, false, "Convert JSON to its string equivalent. Dealing with escape characters", args(1,2, arg("",str),arg("j",json))),
 command("json", "text", JSONjson2text, false, "Convert JSON values to their plain string equivalent.", args(1,2, arg("",str),arg("j",json))),
 command("json", "text", JSONjson2textSeparator, false, "Convert JSON values to their plain string equivalent, injecting a separator.", args(1,3, arg("",str),arg("j",json),arg("s",str))),
 command("json", "number", JSONjson2number, false, "Convert simple JSON values to a double, return nil upon error.", args(1,2, arg("",dbl),arg("j",json))),
 command("json", "integer", JSONjson2integer, false, "Convert simple JSON values to an integer, return nil upon error.", args(1,2, arg("",lng),arg("j",json))),
 pattern("json", "dump", JSONdump, false, "", args(1,2, batarg("",str),arg("j",json))),
 command("json", "filter", JSONfilter, false, "Filter all members of an object by a path expression, returning an array.\nNon-matching elements are skipped.", args(1,3, arg("",json),arg("name",json),arg("pathexpr",str))),
 command("json", "filter", JSONfilterArray_bte, false, "", args(1,3, arg("",json),arg("name",json),arg("idx",bte))),
 command("json", "filter", JSONfilterArrayDefault_bte, false, "", args(1,4, arg("",json),arg("name",json),arg("idx",bte),arg("other",str))),
 command("json", "filter", JSONfilterArray_sht, false, "", args(1,3, arg("",json),arg("name",json),arg("idx",sht))),
 command("json", "filter", JSONfilterArrayDefault_sht, false, "", args(1,4, arg("",json),arg("name",json),arg("idx",sht),arg("other",str))),
 command("json", "filter", JSONfilterArray_int, false, "", args(1,3, arg("",json),arg("name",json),arg("idx",int))),
 command("json", "filter", JSONfilterArrayDefault_int, false, "", args(1,4, arg("",json),arg("name",json),arg("idx",int),arg("other",str))),
 command("json", "filter", JSONfilterArray_lng, false, "", args(1,3, arg("",json),arg("name",json),arg("idx",lng))),
 command("json", "filter", JSONfilterArrayDefault_lng, false, "Extract a single array element", args(1,4, arg("",json),arg("name",json),arg("idx",lng),arg("other",str))),
#ifdef HAVE_HGE
 command("json", "filter", JSONfilterArray_hge, false, "", args(1,3, arg("",json),arg("name",json),arg("idx",hge))),
 command("json", "filter", JSONfilterArrayDefault_hge, false, "Extract a single array element", args(1,4, arg("",json),arg("name",json),arg("idx",hge),arg("other",str))),
#endif
 command("json", "isobject", JSONisobject, false, "Validate the string as a valid JSON object", args(1,2, arg("",bit),arg("val",json))),
 command("json", "isarray", JSONisarray, false, "Validate the string as a valid JSON array", args(1,2, arg("",bit),arg("val",json))),
 command("json", "isvalid", JSONisvalid, false, "Validate the string as a valid JSON document", args(1,2, arg("",bit),arg("val",str))),
 command("json", "length", JSONlength, false, "Returns the number of elements in the outermost JSON object.", args(1,2, arg("",int),arg("val",json))),
 pattern("json", "unfold", JSONunfold, false, "Expands the outermost JSON object into key-value pairs.", args(2,3, batarg("k",str),batarg("v",json),arg("val",json))),
 pattern("json", "unfold", JSONunfold, false, "Expands the outermost JSON object into key-value pairs.", args(3,4, batarg("o",oid),batarg("k",str),batarg("v",json),arg("val",json))),
 pattern("json", "fold", JSONfold, false, "Combine the key-value pairs into a single json object list.", args(1,4, arg("",json),batarg("o",oid),batarg("k",str),batargany("v",0))),
 pattern("json", "fold", JSONfold, false, "Combine the key-value pairs into a single json object list.", args(1,3, arg("",json),batarg("k",str),batargany("v",0))),
 pattern("json", "fold", JSONfold, false, "Combine the value list into a single json array object.", args(1,2, arg("",json),batargany("v",0))),
 command("json", "keyarray", JSONkeyArray, false, "Expands the outermost JSON object keys into a JSON value array.", args(1,2, arg("",json),arg("val",json))),
 command("json", "valuearray", JSONvalueArray, false, "Expands the outermost JSON object values into a JSON value array.", args(1,2, arg("",json),arg("val",json))),
 command("json", "keys", JSONkeyTable, false, "Expands the outermost JSON object names.", args(1,2, batarg("",str),arg("val",json))),
 command("json", "values", JSONvalueTable, false, "Expands the outermost JSON values.", args(1,2, batarg("",json),arg("val",json))),
 command("json", "prelude", JSONprelude, false, "", noargs),
 pattern("json", "renderobject", JSONrenderobject, false, "", args(1,2, arg("",json),varargany("val",0))),
 pattern("json", "renderarray", JSONrenderarray, false, "", args(1,2, arg("",json),varargany("val",0))),
 command("aggr", "jsonaggr", JSONgroupStr, false, "Aggregate the string values to array.", args(1,2, arg("",str),batarg("val",str))),
 command("aggr", "jsonaggr", JSONgroupStr, false, "Aggregate the double values to array.", args(1,2, arg("",str),batarg("val",dbl))),
 command("aggr", "subjsonaggr", JSONsubjson, false, "Grouped aggregation of values.", args(1,5, batarg("",str),batarg("val",str),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subjsonaggr", JSONsubjson, false, "Grouped aggregation of values.", args(1,5, batarg("",str),batarg("val",dbl),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("aggr", "subjsonaggr", JSONsubjsoncand, false, "Grouped aggregation of values with candidates list.", args(1,6, batarg("",str),batarg("val",str),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("aggr", "subjsonaggr", JSONsubjsoncand, false, "Grouped aggregation of values with candidates list.", args(1,6, batarg("",str),batarg("val",dbl),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_json_mal)
{ mal_module("json", json_init_atoms, json_init_funcs); }
