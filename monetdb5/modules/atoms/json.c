/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (c) 2013 Martin Kersten
 */
#include "monetdb_config.h"
#include "json.h"
#include "mal.h"
#include <mal_instruction.h>
#include <mal_interpreter.h>

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

#define hex(J)													\
	do {														\
		if ((*(J) >='0' && *(J) <='9') ||						\
			(*(J) >='a' && *(J) <='f') ||						\
			(*(J) >='A' && *(J) <='F'))							\
			(J)++;												\
		else													\
			throw(MAL, "json.parser", "illegal escape char");	\
	} while (0)

#define CHECK_JSON(jt)													\
	if (jt == NULL || jt->error) {										\
		char *msg;														\
		if (jt) {														\
			msg = jt->error;											\
			jt->error = NULL;											\
			JSONfree(jt);												\
		} else {														\
			msg = createException(MAL, "json.new",						\
								  SQLSTATE(HY001) MAL_MALLOC_FAIL);		\
		}																\
		return msg;														\
	}

#define SEPARATOR ' '

int TYPE_json;

#define JSONlast(J) ((J)->free-1)

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
			js->error = createException(MAL, "json.new", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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

ssize_t
JSONfromString(const char *src, size_t *len, json *j)
{
	size_t slen = strlen(src);
	JSON *jt;

	if (GDK_STRNIL(src)) {
		if (*len < 2 || *j == NULL) {
			GDKfree(*j);
			if ((*j = GDKmalloc(2)) == NULL)
				return -1;
			*len = 2;
		}
		strcpy(*j, str_nil);
		return 1;
	}

	if ((jt = JSONparse(src)) == NULL)
		return -1;
	if (jt->error) {
		GDKerror("%s", getExceptionMessageAndState(jt->error));
		JSONfree(jt);
		return -1;
	}
	JSONfree(jt);

	if (*len <= slen || *j == NULL) {
		GDKfree(*j);
		if ((*j = GDKmalloc(slen + 1)) == NULL)
			return -1;
		*len = slen + 1;
	}
	if (GDKstrFromStr((unsigned char *) *j,
					  (const unsigned char *) src, (ssize_t) slen) < 0)
		return -1;

	return (ssize_t) strlen(*j);
}

ssize_t
JSONtoString(str *s, size_t *len, const char *src)
{
	size_t cnt;
	const char *c;
	char *dst;

	if (GDK_STRNIL(src)) {
		if (*s == NULL || *len < 4) {
			GDKfree(*s);
			*len = 4;
			*s = GDKmalloc(4);
			if (*s == NULL)
				return -1;
		}
		strncpy(*s, "nil", 4);
		return 3;
	}
	/* count how much space we need for the output string */
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

	if (cnt > (size_t) *len) {
		GDKfree(*s);
		*s = GDKmalloc(cnt);
		if (*s == NULL)
			return -1;
		*len = cnt;
	}
	dst = *s;
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
	*dst++ = 0;
	assert((size_t) (dst - *s) == cnt);
	return cnt - 1;				/* length without \0 */
}

#define tab(D)									\
	do {										\
		int kk;									\
		for (kk = 0; kk < (D) * 4; kk++)		\
			mnstr_printf(fd, " ");				\
	} while (0)

static void
JSONdumpInternal(JSON *jt, int depth)
{
	int i, idx;
	JSONterm *je;
	stream *fd = GDKout;

	for (idx = 0; idx < jt->free; idx++) {
		je = jt->elm + idx;

		tab(depth);
		mnstr_printf(fd, "[%d] ", idx);
		switch (je->kind) {
		case JSON_OBJECT:
			mnstr_printf(fd, "object ");
			break;
		case JSON_ARRAY:
			mnstr_printf(fd, "array ");
			break;
		case JSON_ELEMENT:
			mnstr_printf(fd, "element ");
			break;
		case JSON_VALUE:
			mnstr_printf(fd, "value ");
			break;
		case JSON_STRING:
			mnstr_printf(fd, "string ");
			break;
		case JSON_NUMBER:
			mnstr_printf(fd, "number ");
			break;
		case JSON_BOOL:
			mnstr_printf(fd, "bool ");
			break;
		case JSON_NULL:
			mnstr_printf(fd, "null ");
			break;
		default:
			mnstr_printf(fd, "unknown %d ", je->kind);
		}
		mnstr_printf(fd, "child %d list ", je->child);
		for (i = je->next; i; i = jt->elm[i].next)
			mnstr_printf(fd, "%d ", i);
		if (je->name) {
			mnstr_printf(fd, "%.*s : ", (int) je->namelen, je->name);
		}
		if (je->value)
			mnstr_printf(fd, "%.*s", (int) je->valuelen, je->value);
		mnstr_printf(fd, "\n");
	}
}

str
JSONdump(void *ret, json *val)
{
	JSON *jt = JSONparse(*val);

	CHECK_JSON(jt);
	(void) ret;
	JSONdumpInternal(jt, 0);
	JSONfree(jt);
	return MAL_SUCCEED;
}


str
JSONjson2str(str *ret, json *j)
{
	char *s = *j, *c;

	if (*s == '"')
		s++;
	if ((s = GDKstrdup(s)) == NULL)
		throw(MAL, "json.str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	c = s + strlen(s) - 1;
	if (*c == '"')
		*c = 0;
	*ret = s;
	return MAL_SUCCEED;
}

str
JSONstr2json(json *ret, str *j)
{
	JSON *jt = JSONparse(*j);

	CHECK_JSON(jt);
	JSONfree(jt);
	if ((*ret = GDKstrdup(*j)) == NULL)
		throw(MAL, "json.new", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

str
JSONisvalid(bit *ret, json *j)
{
	JSON *jt = JSONparse(*j);

	if (jt == NULL)
		throw(MAL, "json.isvalid", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	*ret = jt->error == MAL_SUCCEED;
	JSONfree(jt);
	return MAL_SUCCEED;
}

str
JSONisobject(bit *ret, json *js)
{
	char *j = *js;

	skipblancs(j);
	*ret = *j == '{';
	return MAL_SUCCEED;
}

str
JSONisarray(bit *ret, json *js)
{
	char *j = *js;

	skipblancs(j);
	*ret = *j == '[';
	return MAL_SUCCEED;
}

str
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
#define ROOT_STEP 0
#define CHILD_STEP 1
#define INDEX_STEP 2
#define ANY_STEP 3
#define END_STEP 4

typedef struct {
	int token;
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
				throw(MAL, "json.compile", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
			s++;
			skipblancs(s);
			if (*s != '*') {
				if (*s >= '0' && *s <= '9') {
					terms[t].index = atoi(s);
					terms[t].first = terms[t].last = atoi(s);
				} else
					throw(MAL, "json.path", "'*' or digit expected");
			}
			for (; *s; s++)
				if (*s == ']')
					break;
			if (*s == 0) {
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
	if (strncmp(jt->elm[idx].value, "null", 4) == 0)
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

	if (r == 0 || *r == 0)
		return res;
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
	strcpy(n, res);
	GDKfree(res);
	if (sep) {
		n[l] = ',';
		strncpy(n + l + 1, r, len);
		n[l + 1 + len] = 0;
	} else {
		strncpy(n + l, r, len);
		n[l + len] = 0;
	}
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
			throw(MAL,"JSONfilterInternal", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		msg = createException(MAL,"JSONfilterInternal", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto bailout;
	}
	// process all other PATH expression
	for (tidx++; tidx < MAXTERMS && terms[tidx].token; tidx++)
		if (terms[tidx].token == END_STEP && tidx + 1 < MAXTERMS && terms[tidx + 1].token) {
			s = JSONmatch(jt, 0, terms, ++tidx);
			if (s == (char *) -1) {
				msg = createException(MAL,"JSONfilterInternal", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		throw(MAL,"JSONfilterInternal", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
				continue;
			case 'u':
				j++;
				hex(j);
				hex(j);
				hex(j);
				hex(j);
				break;
			default:
				*next = j;
				throw(MAL, "json.parser", "illegal escape char");
			}
			break;
		case '"':
			j++;
			*next = j;
			return MAL_SUCCEED;
		}
	}
	*next = j;
	throw(MAL, "json.parser", "Nonterminated string");
}

static str
JSONnumberParser(const char *j, const char **next)
{
	const char *backup = j;

	if (*j == '-')
		j++;
	skipblancs(j);
	if (*j < '0' || *j > '9') {
		*next = j;
		throw(MAL, "json.parser", "Number expected");
	}
	for (; *j; j++)
		if (*j < '0' || *j > '9')
			break;
	backup = j;
	skipblancs(j);
	if (*j == '.') {
		j++;
		skipblancs(j);
		for (; *j; j++)
			if (*j < '0' || *j > '9')
				break;
		backup = j;
	} else
		j = backup;
	skipblancs(j);
	if (*j == 'e' || *j == 'E') {
		j++;
		skipblancs(j);
		if (*j == '-')
			j++;
		skipblancs(j);
		for (; *j; j++)
			if (*j < '0' || *j > '9')
				break;
	} else
		j = backup;
	*next = j;
	return MAL_SUCCEED;
}

static int
JSONtoken(JSON *jt, const char *j, const char **next)
{
	str msg;
	int nxt, idx = JSONnew(jt);

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
			if (jt->elm[nxt].kind != JSON_ELEMENT) {
				jt->error = createException(MAL, "json.parser", "Syntax error : element expected");
				return idx;
			}
			JSONappend(jt, idx, nxt);
			if (jt->error)
				return idx;
			j = *next;
			skipblancs(j);
			if (*j == '}')
				break;
			if (*j != '}' && *j != ',') {
				jt->error = createException(MAL, "json.parser", "Syntax error : ','  or '}' expected");
				return idx;
			}
			j++;
		}
		if (*j != '}') {
			jt->error = createException(MAL, "json.parser", "Syntax error : '}' expected");
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
				jt->error = createException(MAL, "json.parser", "Syntax error : Array value expected");
				return idx;
			}
			if (*j != ']' && *j != ',') {
				jt->error = createException(MAL, "json.parser", "Syntax error : ','  or ']' expected");
				return idx;
			}
			j++;
			skipblancs(j);
		}
		if (*j != ']') {
			jt->error = createException(MAL, "json.parser", "Syntax error : ']' expected");
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
		j = *next;
		skipblancs(j);
		if (*j == ':') {
			j++;
			skipblancs(j);
			jt->elm[idx].kind = JSON_ELEMENT;
			nxt = JSONtoken(jt, j, next);
			if (jt->error)
				return idx;
			jt->elm[idx].child = nxt;
			jt->elm[idx].value++;
			jt->elm[idx].valuelen -= 2;
		}
		return idx;
	case 'n':
		if (strncmp("null", j, 4) == 0) {
			*next = j + 4;
			jt->elm[idx].kind = JSON_NULL;
			jt->elm[idx].value = j;
			jt->elm[idx].valuelen = 4;
			return idx;
		}
		jt->error = createException(MAL, "json.parser", "Syntax error: NULL expected");
		return idx;
	case 't':
		if (strncmp("true", j, 4) == 0) {
			*next = j + 4;
			jt->elm[idx].kind = JSON_NUMBER;
			jt->elm[idx].value = j;
			jt->elm[idx].valuelen = 4;
			return idx;
		}
		jt->error = createException(MAL, "json.parser", "Syntax error: True expected");
		return idx;
	case 'f':
		if (strncmp("false", j, 5) == 0) {
			*next = j + 5;
			jt->elm[idx].kind = JSON_NUMBER;
			jt->elm[idx].value = j;
			jt->elm[idx].valuelen = 5;
			return idx;
		}
		jt->error = createException(MAL, "json.parser", "Syntax error: False expected");
		return idx;
	default:
		if (*j == '-' || (*j >= '0' && *j <= '9')) {
			jt->elm[idx].value = j;
			msg = JSONnumberParser(j, next);
			if (msg)
				jt->error = msg;
			jt->elm[idx].kind = JSON_NUMBER;
			jt->elm[idx].valuelen = *next - jt->elm[idx].value;
			return idx;
		}
		jt->error = createException(MAL, "json.parser", "Syntax error: value expected");
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
	if (!*j || !(*j == '{' || *j == '[')) {
		jt->error = createException(MAL, "json.parser", "Syntax error: json parse failed, expecting '{', '['");
		return jt;
	}
	JSONtoken(jt, j, &j);
	if (jt->error)
		return jt;
	skipblancs(j);
	if (*j)
		jt->error = createException(MAL, "json.parser", "Syntax error: json parse failed");
	return jt;
}

str
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

str
JSONfilterArray_bte(json *ret, json *js, bte *index)
{
	return JSONfilterArrayDefault(ret, js, (lng) *index, 0);
}

str
JSONfilterArrayDefault_bte(json *ret, json *js, bte *index, str *other)
{
	return JSONfilterArrayDefault(ret, js, (lng) *index, *other);
}

str
JSONfilterArray_sht(json *ret, json *js, sht *index)
{
	return JSONfilterArrayDefault(ret, js, (lng) *index, 0);
}

str
JSONfilterArrayDefault_sht(json *ret, json *js, sht *index, str *other)
{
	return JSONfilterArrayDefault(ret, js, (lng) *index, *other);
}

str
JSONfilterArray_int(json *ret, json *js, int *index)
{
	return JSONfilterArrayDefault(ret, js, (lng) *index, 0);
}

str
JSONfilterArrayDefault_int(json *ret, json *js, int *index, str *other)
{
	return JSONfilterArrayDefault(ret, js, (lng) *index, *other);
}

str
JSONfilterArray_lng(json *ret, json *js, lng *index)
{
	return JSONfilterArrayDefault(ret, js, (lng) *index, 0);
}

str
JSONfilterArrayDefault_lng(json *ret, json *js, lng *index, str *other)
{
	return JSONfilterArrayDefault(ret, js, (lng) *index, *other);
}

#ifdef HAVE_HGE
str
JSONfilterArray_hge(json *ret, json *js, hge *index)
{
	if (*index < (hge) GDK_lng_min || *index > (hge) GDK_lng_max)
		throw(MAL, "json.filter", "index out of range");
	return JSONfilterArrayDefault(ret, js, (lng) *index, 0);
}

str
JSONfilterArrayDefault_hge(json *ret, json *js, hge *index, str *other)
{
	if (*index < (hge) GDK_lng_min || *index > (hge) GDK_lng_max)
		throw(MAL, "json.filter", "index out of range");
	return JSONfilterArrayDefault(ret, js, (lng) *index, *other);
}
#endif

str
JSONfilter(json *ret, json *js, str *expr)
{
	return JSONfilterInternal(ret, js, expr, 0);
}

// glue all values together with an optional separator
// The json string should be valid

static char *
JSONplaintext(char *r, size_t *l, JSON *jt, int idx, char sep)
{
	int i;
	size_t j;

	switch (jt->elm[idx].kind) {
	case JSON_OBJECT:
		for (i = jt->elm[idx].next; i; i = jt->elm[i].next)
			if (jt->elm[i].child)
				r = JSONplaintext(r, l, jt, jt->elm[i].child, sep);
		break;
	case JSON_ARRAY:
		for (i = jt->elm[idx].next; i; i = jt->elm[i].next)
			r = JSONplaintext(r, l, jt, i, sep);
		break;
	case JSON_ELEMENT:
	case JSON_VALUE:
		if (jt->elm[idx].child)
			r = JSONplaintext(r, l, jt, jt->elm[idx].child, sep);
		break;
	case JSON_STRING:
		for (j = 1; *l > 1 && j < jt->elm[idx].valuelen - 1; j++) {
			if (jt->elm[idx].value[j] == '\\')
				*r = jt->elm[idx].value[++j];
			else
				*r = jt->elm[idx].value[j];
			r++;
			(*l)--;
		}
		if (*l > 1 && sep) {
			*r++ = sep;
			(*l)--;
		}
		break;
	default:
		for (j = 0; *l > 1 && j < jt->elm[idx].valuelen; j++) {
			*r = jt->elm[idx].value[j];
			r++;
			(*l)--;
		}
		if (*l > 1 && sep) {
			*r++ = sep;
			(*l)--;
		}
	}
	assert(*l > 0);
	*r = 0;
	return r;
}

str
JSONjson2text(str *ret, json *js)
{
	JSON *jt;
	size_t l;
	str s;

	jt = JSONparse(*js);

	CHECK_JSON(jt);
	l = strlen(*js) + 1;
	s = GDKmalloc(l);
	if(s == NULL) {
		JSONfree(jt);
		throw(MAL,"json2txt", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	JSONplaintext(s, &l, jt, 0, ' ');
	l = strlen(s);
	if (l)
		s[l - 1] = 0;
	*ret = s;
	JSONfree(jt);
	return MAL_SUCCEED;
}

str
JSONjson2textSeparator(str *ret, json *js, str *sep)
{
	JSON *jt;
	size_t l;
	str s;

	jt = JSONparse(*js);

	CHECK_JSON(jt);
	l = strlen(*js) + 1;
	s = GDKmalloc(l);
	if(s == NULL) {
		JSONfree(jt);
		throw(MAL,"json2txt", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	JSONplaintext(s, &l, jt, 0, **sep);
	l = strlen(s);
	if (l)
		s[l - 1] = 0;
	*ret = s;
	JSONfree(jt);
	return MAL_SUCCEED;
}

str
JSONjson2number(dbl *ret, json *js)
{
	JSON *jt;
	char *rest;

	*ret = dbl_nil;
	jt = JSONparse(*js);
	CHECK_JSON(jt);
	switch (jt->elm[0].kind) {
	case JSON_NUMBER:
		*ret = strtod(jt->elm[0].value, &rest);
		if (rest && (size_t) (rest - jt->elm[0].value) !=jt->elm[0].valuelen)
			*ret = dbl_nil;
		break;
	case JSON_ARRAY:
		if (jt->free == 2) {
			*ret = strtod(jt->elm[1].value, &rest);
			if (rest && (size_t) (rest - jt->elm[1].value) !=jt->elm[1].valuelen)
				*ret = dbl_nil;
		}
		break;
	case JSON_OBJECT:
		if (jt->free == 3) {
			*ret = strtod(jt->elm[2].value, &rest);
			if (rest && (size_t) (rest - jt->elm[2].value) !=jt->elm[2].valuelen)
				*ret = dbl_nil;
		}
	}
	JSONfree(jt);
	return MAL_SUCCEED;
}

str
JSONjson2integer(lng *ret, json *js)
{
	JSON *jt;
	char *rest;

	*ret = lng_nil;
	jt = JSONparse(*js);
	CHECK_JSON(jt);
	switch (jt->elm[0].kind) {
	case JSON_NUMBER:
		*ret = strtol(jt->elm[0].value, &rest, 0);
		if (rest && (size_t) (rest - jt->elm[0].value) !=jt->elm[0].valuelen)
			*ret = lng_nil;
		break;
	case JSON_ARRAY:
		if (jt->free == 2) {
			*ret = strtol(jt->elm[1].value, &rest, 0);
			if (rest && (size_t) (rest - jt->elm[1].value) !=jt->elm[1].valuelen)
				*ret = lng_nil;
		}
		break;
	case JSON_OBJECT:
		if (jt->free == 3) {
			*ret = strtol(jt->elm[2].value, &rest, 0);
			if (rest && (size_t) (rest - jt->elm[2].value) !=jt->elm[2].valuelen)
				*ret = lng_nil;
		}
	}
	JSONfree(jt);
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
			if (BUNappend(bk, r, FALSE) != GDK_SUCCEED) {
				GDKfree(r);
				goto memfail;
			}
			GDKfree(r);
			if ((r = JSONgetValue(jt, jt->elm[i].child)) == NULL)
				goto memfail;
			if (BUNappend(bv, r, FALSE) != GDK_SUCCEED) {
				GDKfree(r);
				goto memfail;
			}
			GDKfree(r);
			if (bo) {
				if (BUNappend(bo, o, FALSE) != GDK_SUCCEED)
					goto memfail;
			}
			(*o)++;
			if (i == last)
				break;
		}
	} else if (jt->elm[idx].kind == JSON_ARRAY) {
		for (i = jt->elm[idx].next; i; i = jt->elm[i].next) {
			if (BUNappend(bk, str_nil, FALSE) != GDK_SUCCEED)
				goto memfail;
			if (jt->elm[i].kind == JSON_VALUE)
				r = JSONgetValue(jt, jt->elm[i].child);
			else
				r = JSONgetValue(jt, i);
			if (r == NULL)
				goto memfail;
			if (BUNappend(bv, r, FALSE) != GDK_SUCCEED) {
				GDKfree(r);
				goto memfail;
			}
			GDKfree(r);
			if (bo) {
				if (BUNappend(bo, o, FALSE) != GDK_SUCCEED)
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
	throw(MAL, "json.unfold", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
		throw(MAL, "json.unfold", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bk->tsorted = 1;
	bk->trevsorted = 0;
	bk->tnonil = 1;

	if (od) {
		bo = COLnew(0, TYPE_oid, 64, TRANSIENT);
		if (bo == NULL) {
			BBPreclaim(bk);
			JSONfree(jt);
			throw(MAL, "json.unfold", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		bo->tsorted = 1;
		bo->trevsorted = 0;
		bo->tnonil = 1;
	}

	bv = COLnew(0, TYPE_json, 64, TRANSIENT);
	if (bv == NULL) {
		JSONfree(jt);
		BBPreclaim(bo);
		BBPreclaim(bk);
		throw(MAL, "json.unfold", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bv->tsorted = 1;
	bv->trevsorted = 0;
	bv->tnonil = 1;

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



str
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
		throw(MAL, "json.keys", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bn->tsorted = 1;
	bn->trevsorted = 0;
	bn->tnonil = 1;

	for (i = jt->elm[0].next; i; i = jt->elm[i].next) {
		r = JSONgetValue(jt, i);
		if (r == NULL ||
			BUNappend(bn, r, FALSE) != GDK_SUCCEED) {
			GDKfree(r);
			JSONfree(jt);
			BBPreclaim(bn);
			throw(MAL, "json.keys", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		GDKfree(r);
	}
	JSONfree(jt);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

str
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
			r = GDKzalloc(jt->elm[i].valuelen + 3);
			if (r == NULL) {
				JSONfree(jt);
				goto memfail;
			}
			if (jt->elm[i].valuelen)
				strncpy(r, jt->elm[i].value - 1, jt->elm[i].valuelen + 2);
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
	throw(MAL, "json.keyarray", SQLSTATE(HY001) MAL_MALLOC_FAIL);
}


str
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
		throw(MAL, "json.values", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bn->tsorted = 1;
	bn->trevsorted = 0;
	bn->tnonil = 1;

	for (i = jt->elm[0].next; i; i = jt->elm[i].next) {
		if (jt->elm[i].kind == JSON_ELEMENT)
			r = JSONgetValue(jt, jt->elm[i].child);
		else
			r = JSONgetValue(jt, i);
		if (r == NULL ||
			BUNappend(bn, r, FALSE) != GDK_SUCCEED) {
			GDKfree(r);
			BBPreclaim(bn);
			JSONfree(jt);
			throw(MAL, "json.values", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		GDKfree(r);
	}
	JSONfree(jt);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

str
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
	throw(MAL, "json.valuearray", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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

str
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
		throw(MAL,"json.renderobject", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
	throw(MAL,"json.renderobject", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
	return NULL;
}

str
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
	throw(MAL,"json.renderArray", SQLSTATE(HY001) MAL_MALLOC_FAIL);
}

static str
JSONfoldKeyValue(str *ret, const bat *id, const bat *key, const bat *values)
{
	BAT *bo = 0, *bk = 0, *bv;
	BATiter boi, bki, bvi;
	int tpe;
	char *row, *val = 0, *nme = 0;
	BUN i, cnt;
	size_t len, lim, l;
	void *p;
	oid o = 0;;

	if (key) {
		bk = BATdescriptor(*key);
		if (bk == NULL) {
			throw(MAL, "json.fold", RUNTIME_OBJECT_MISSING);
		}
	}

	bv = BATdescriptor(*values);
	if (bv == NULL) {
		if (bk)
			BBPunfix(bk->batCacheid);
		throw(MAL, "json.fold", RUNTIME_OBJECT_MISSING);
	}
	tpe = bv->ttype;
	cnt = BATcount(bv);
	if (bk)
		bki = bat_iterator(bk);
	bvi = bat_iterator(bv);
	if (id) {
		bo = BATdescriptor(*id);
		if (bo == NULL) {
			if (bk)
				BBPunfix(bk->batCacheid);
			BBPunfix(bv->batCacheid);
			throw(MAL, "json.nest", RUNTIME_OBJECT_MISSING);
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
		boi = bat_iterator(bo);
		o = *(oid *) BUNtail(boi, 0);
	}

	for (i = 0; i < cnt; i++) {
		if (id && bk) {
			p = BUNtail(boi, i);
			if (*(oid *) p != o) {
				snprintf(row + len, lim - len, ", ");
				len += 2;
				o = *(oid *) p;
			}
		}

		if (bk) {
			nme = (str) BUNtail(bki, i);
			l = strlen(nme);
			while (l + 3 > lim - len)
				lim = (lim / (i + 1)) * cnt + BUFSIZ + l + 3;
			p = GDKrealloc(row, lim);
			if (p == NULL) {
				goto memfail;
			}
			row = p;
			if (strcmp(nme, str_nil)) {
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
	throw(MAL, "json.fold", SQLSTATE(HY001) MAL_MALLOC_FAIL);
}

str
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

str
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

str
JSONtextString(str *ret, bat *bid)
{
	(void) ret;
	(void) bid;
	throw(MAL, "json.text", PROGRAM_NYI);
}


str
JSONtextGrouped(bat *ret, bat *bid, bat *gid, bat *ext, bit *flg)
{
	(void) ret;
	(void) bid;
	(void) gid;
	(void) ext;
	(void) flg;
	throw(MAL, "json.text", PROGRAM_NYI);
}

str
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
		throw(MAL, "json.group", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(buf);
		throw(MAL, "json.agg", RUNTIME_OBJECT_MISSING);
	}
	assert(b->ttype == TYPE_str || b->ttype == TYPE_dbl);

	strcpy(buf, str_nil);
	offset = 0;
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		int n = 0, nil = 0;

		switch (b->ttype) {
		case TYPE_str:
			t = (const char *) BUNtail(bi, p);
			nil = (strNil(t));
			break;
		case TYPE_dbl:
			val = (const double *) BUNtail(bi, p);
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
				err = MAL_MALLOC_FAIL;
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
	BUN ngrp, start, end;
	BUN nils = 0;
	int isnil;
	const oid *cand = NULL, *candend = NULL;
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
	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end, &cand, &candend)) != NULL) {
		return err;
	}
	if (BATcount(b) == 0 || ngrp == 0) {
		bn = BATconstant(ngrp == 0 ? 0 : min, TYPE_str, ATOMnilptr(TYPE_str), ngrp, TRANSIENT);
		if (bn == NULL)
			return MAL_MALLOC_FAIL;
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
		err = MAL_MALLOC_FAIL;
		goto out;
	}
	buflen = 0;
	bn = COLnew(min, TYPE_str, ngrp, TRANSIENT);
	if (bn == NULL) {
		err = MAL_MALLOC_FAIL;
		goto out;
	}
	bi = bat_iterator(b);
	if (g) {
		/* stable sort g */
		if (BATsort(&t1, &t2, NULL, g, NULL, NULL, 0, 1) != GDK_SUCCEED) {
			err = "internal sort failed";
			goto out;
		}
		if (freeg)
			BBPunfix(g->batCacheid);
		g = t1;
		freeg = 1;
		if (t2->ttype == TYPE_void) {
			map = NULL;
			mapoff = t2->tseqbase;
		} else {
			map = (const oid *) Tloc(t2, 0);
		}
		if (g && BATtdense(g)) {
			for (p = 0, q = BATcount(g); p < q; p++) {
				switch (b->ttype) {
				case TYPE_str:
					v = (const char *) BUNtail(bi, (map ? (BUN) map[p] : p + mapoff));
					break;
				case TYPE_dbl:
					val = (const double *) BUNtail(bi, (map ? (BUN) map[p] : p + mapoff));
					if (!is_dbl_nil(*val)) {
						snprintf(temp, sizeof(temp), "%f", *val);
						v = (const char *) temp;
					} else {
						v = NULL;
					}
					break;
				}
				if (!v || strNil(v)) {
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
					if (len >= maxlen) {
						maxlen += len + BUFSIZ;
						buf2 = GDKrealloc(buf, maxlen);
						if (buf2 == NULL) {
							err = MAL_MALLOC_FAIL;
							goto bunins_failed;
						}
						buf = buf2;
					}
					switch (b->ttype) {
					case TYPE_str:
						len = snprintf(buf, maxlen, "[ \"%s\" ]", v);
						break;
					case TYPE_dbl:
						len = snprintf(buf, maxlen, "[ %s ]", v);
						break;
					}
				}
				bunfastapp_nocheck(bn, BUNlast(bn), buf, Tsize(bn));
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
					bunfastapp_nocheck(bn, BUNlast(bn), str_nil, Tsize(bn));
					nils++;
				}
				bunfastapp_nocheck(bn, BUNlast(bn), buf, Tsize(bn));
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
				v = (const char *) BUNtail(bi, (map ? (BUN) map[p] : p + mapoff));
				break;
			case TYPE_dbl:
				val = (const double *) BUNtail(bi, (map ? (BUN) map[p] : p + mapoff));
				if (!is_dbl_nil(*val)) {
					snprintf(temp, sizeof(temp), "%f", *val);
					v = (const char *) temp;
				} else {
					v = NULL;
				}
				break;
			}
			if (!v || strNil(v)) {
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
						err = MAL_MALLOC_FAIL;
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
				v = (const char *) BUNtail(bi, p);
				break;
			case TYPE_dbl:
				val = (const double *) BUNtail(bi, p);
				if (!is_dbl_nil(*val)) {
					snprintf(temp, sizeof(temp), "%f", *val);
					v = (const char *) temp;
				} else {
					v = NULL;
				}
				break;
			}

			if (!v || strNil(v)) {
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
					err = MAL_MALLOC_FAIL;
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
		bunfastapp_nocheck(bn, BUNlast(bn), buf, Tsize(bn));
	}
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tkey = BATcount(bn) <= 1;

  out:
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
		err = MAL_MALLOC_FAIL;	/* insertion into result BAT failed */
	goto out;
}

str
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
		err = RUNTIME_OBJECT_MISSING;
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

str
JSONsubjson(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils)
{
	return JSONsubjsoncand(retval, bid, gid, eid, NULL, skip_nils);
}
