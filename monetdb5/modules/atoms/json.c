/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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

#define hex(J)									\
	do {										\
		if ((*(J) >='0' && *(J) <='9') ||		\
			(*(J) >='a' && *(J) <='f') ||		\
			(*(J) >='A' && *(J) <='F'))			\
			(J)++;								\
	} while (0)

#define CHECK_JSON(jt) 			\
	if (jt && jt->error) { 		\
		char *msg = jt->error;	\
		JSONfree(jt);		\
		return msg;		\
	}

#define SEPARATOR ' '

int TYPE_json;

#define JSONlast(J) ((J)->free-1)

/* Internal constructors. */
static int jsonhint = 8;
static JSON *JSONparse(char *j, int silent);

static JSON *
JSONnewtree(int size)
{
	JSON *js;

	if (size == 0)
		size = jsonhint;
	js = (JSON *) GDKzalloc(sizeof(JSON));
	js->elm = (JSONterm *) GDKzalloc(sizeof(JSONterm) * size);
	js->size = size;
	return js;
}

static int
JSONnew(JSON *js)
{
	JSONterm *term;

	if (js->free == js->size) {
		term = (JSONterm *) GDKrealloc(js->elm, sizeof(JSONterm) * (js->size + 8));
		if (term == NULL) {
			js->error = createException(MAL, "json.new", MAL_MALLOC_FAIL);
			return js->free - 1;
		}
		js->elm = term;
		memset(((char *) term) + sizeof(JSONterm) * js->size, 0, 8 * sizeof(JSONterm));
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
	GDKfree(c->elm);
	GDKfree(c);
}

int
JSONfromString(str src, int *len, json *j)
{
	ssize_t slen = (ssize_t) strlen(src);
	JSON *jt = JSONparse(src, FALSE);

	if (*j)
		GDKfree(*j);

	if (!jt || jt->error) {
		*j = GDKstrdup(str_nil);
		if (jt)
			JSONfree(jt);
		return 0;
	}
	JSONfree(jt);

	*j = GDKstrdup(src);
	*len = (int) slen;
	if (GDKstrFromStr((unsigned char *) *j, (const unsigned char *) src, slen) < 0) {
		GDKfree(*j);
		*j = GDKstrdup(str_nil);
		*len = 2;
		return 0;
	}
	return *len;
}

int
JSONtoString(str *s, int *len, json src)
{
	size_t cnt;
	char *c, *dst;

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
		*s = (str) GDKmalloc(cnt);
		if (*s == NULL)
			return 0;
		*len = (int) cnt;
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
	return (int) (cnt - 1);	/* length without \0 */
}

#define tab(D) { int kk; for(kk=0; kk< D * 4; kk++) mnstr_printf(fd," ");}

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
	JSON *jt = JSONparse(*val, FALSE);

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
	s = GDKstrdup(s);
	c = s + strlen(s) - 1;
	if (*c == '"')
		*c = 0;
	*ret = s;
	return MAL_SUCCEED;
}

str
JSONstr2json(json *ret, str *j)
{
	JSON *jt = JSONparse(*j, FALSE);

	CHECK_JSON(jt);
	if (jt)
		JSONfree(jt);
	*ret = GDKstrdup(*j);
	return MAL_SUCCEED;
}

str
JSONisvalid(bit *ret, json *j)
{
	JSON *jt = JSONparse(*j, FALSE);

	if (jt) {
		*ret = jt->error == MAL_SUCCEED;
		JSONfree(jt);
	}
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
 * The JSON filter operation takes a path expression which is purposely kept simple,
 * It provides step (.), multistep (..) and indexed ([nr]) access to the JSON elements.
 * A wildcard * can be used as placeholder for a step identifier.
 *
 * A path expression is always validated upfront and can only be applied to valid json strings.
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
		res = GDKstrdup(r);
	else {
		l = strlen(res);
		n = GDKzalloc(l + len + 3);
		strcpy(n, res);
		if (sep) {
			n[l] = ',';
			strncpy(n + l + 1, r, len);
			n[l + 1 + len] = 0;
		} else {
			strncpy(n + l, r, len);
			n[l + len] = 0;
		}
		GDKfree(res);
		res = n;
	}
	if (r)
		GDKfree(r);
	return res;
}

static str
JSONmatch(JSON *jt, int ji, pattern * terms, int ti)
{
	str r = NULL, res = NULL;
	int i, match;
	int cnt;

	if (terms[ti].token == ROOT_STEP) {
		if (terms[ti + 1].token == END_STEP)
			return JSONgetValue(jt, 0);
		ti++;
	}

	switch (jt->elm[ji].kind) {
	case JSON_ARRAY:
		if (terms[ti].name != 0 && terms[ti].token != ANY_STEP) {
			if (terms[ti].token == END_STEP)
				res = JSONgetValue(jt, ji);
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
				} else if (terms[ti + 1].token == END_STEP) {
					if (jt->elm[i].kind == JSON_VALUE || jt->elm[i].kind == JSON_VALUE)
						r = JSONgetValue(jt, jt->elm[i].child);
					else
						r = JSONgetValue(jt, i);
				} else
					r = JSONmatch(jt, jt->elm[i].child, terms, ti + 1);
				res = JSONglue(res, r, ',');
			}
		}
		break;
	case JSON_OBJECT:
		cnt = 0;
		for (i = jt->elm[ji].next; i && cnt >= 0; i = jt->elm[i].next) {
			// check the element label
			match = (terms[ti].name && jt->elm[i].valuelen == terms[ti].namelen && strncmp(terms[ti].name, jt->elm[i].value, terms[ti].namelen) == 0) ||terms[ti].name == 0 || terms[ti].name[0] == '*';
			if (match) {
				if (terms[ti].index == INT_MAX || (cnt >= terms[ti].first && cnt <= terms[ti].last)) {
					if (terms[ti + 1].token == END_STEP)
						r = JSONgetValue(jt, jt->elm[i].child);
					else
						r = JSONmatch(jt, jt->elm[i].child, terms, ti + 1);
					res = JSONglue(res, r, ',');
				}
				cnt++;
			} else if (terms[ti].token == ANY_STEP && jt->elm[i].child) {
				r = JSONmatch(jt, jt->elm[i].child, terms, ti);
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
		return MAL_SUCCEED;
	}
	memset((char *) terms, 0, MAXTERMS * sizeof(pattern));
	msg = JSONcompile(*expr, terms);
	if (msg)
		return msg;
	jt = JSONparse(j, FALSE);
	CHECK_JSON(jt);

	result = s = JSONmatch(jt, 0, terms, tidx);
	// process all other PATH expression
	for (tidx++; tidx < MAXTERMS && terms[tidx].token; tidx++)
		if (terms[tidx].token == END_STEP && tidx + 1 < MAXTERMS && terms[tidx + 1].token) {
			s = JSONmatch(jt, 0, terms, ++tidx);
			result = JSONglue(result, s, ',');
		}
	if (result) {
		l = strlen(result);
		if (result[l - 1] == ',')
			result[l - 1] = 0;
	} else
		l = 3;
	s = GDKzalloc(l + 3);
	snprintf(s, l + 3, "[%s]", (result ? result : ""));
	GDKfree(result);

	for (l = 0; terms[l].token; l++)
		if (terms[l].name)
			GDKfree(terms[l].name);
	JSONfree(jt);
	*ret = s;
	return msg;
}


static str
JSONstringParser(char *j, char **next, int silent)
{
	if (*j == '"')
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
				if (silent) {
					return MAL_SUCCEED;
				}
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
	if (!silent)
		throw(MAL, "json.parser", "Nonterminated string");
	return MAL_SUCCEED;
}

static str
JSONnumberParser(char *j, char **next, int silent)
{
	char *backup = j;

	if (*j == '-')
		j++;
	skipblancs(j);
	if (*j < '0' || *j > '9') {
		*next = j;
		if (!silent)
			throw(MAL, "json.parser", "Number expected");
		return MAL_SUCCEED;
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
JSONtoken(JSON *jt, char *j, char **next, int silent)
{
	str msg;
	int nxt, idx = JSONnew(jt);

	assert(silent==0);
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
			nxt = JSONtoken(jt, j, next, silent);
			if (jt->error) 
				return idx;
			if (jt->elm[nxt].kind != JSON_ELEMENT) {
				if (!silent)
					jt->error = createException(MAL, "json.parser", "Syntax error : element expected");
				return idx;
			}
			JSONappend(jt, idx, nxt);
			j = *next;
			skipblancs(j);
			if (*j == '}')
				break;
			if (*j != '}' && *j != ',') {
				if (!silent)
					jt->error = createException(MAL, "json.parser", "Syntax error : ','  or '}' expected");
				return idx;
			}
			j++;
		}
		if (*j != '}') {
			if (!silent)
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
			nxt = JSONtoken(jt, j, next, silent);
			if (jt->error) 
				return idx;
			switch (jt->elm[nxt].kind) {
			case JSON_ELEMENT:{
				int k = JSONnew(jt);
				jt->elm[k].kind = JSON_OBJECT;
				jt->elm[k].child = nxt;
				nxt = k;
			}
				/* fall through */
			case JSON_OBJECT:
			case JSON_ARRAY:
				if (jt->elm[nxt].kind == JSON_OBJECT || jt->elm[nxt].kind == JSON_ARRAY) {
					int k = JSONnew(jt);
					JSONappend(jt, idx, k);
					jt->elm[k].kind = JSON_VALUE;
					jt->elm[k].child = nxt;
				}
				break;
			default:
				JSONappend(jt, idx, nxt);
			}
			j = *next;
			skipblancs(j);
			if (*j == ']')
				break;
			if (jt->elm[nxt].kind == JSON_ELEMENT) {
				if (!silent)
					jt->error = createException(MAL, "json.parser", "Syntax error : Array value expected");
				return idx;
			}
			if (*j != ']' && *j != ',') {
				if (!silent)
					jt->error = createException(MAL, "json.parser", "Syntax error : ','  or ']' expected");
				return idx;
			}
			j++;
			skipblancs(j);
		}
		if (*j != ']') {
			if (!silent)
				jt->error = createException(MAL, "json.parser", "Syntax error : ']' expected");
		} else
			j++;
		*next = j;
		jt->elm[idx].valuelen = *next - jt->elm[idx].value;
		return idx;
	case '"':
		msg = JSONstringParser(j, next, silent);
		if (!silent && msg) {
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
			nxt = JSONtoken(jt, j, next, silent);
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
		if (!silent)
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
		if (!silent) 
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
		if (!silent)
			jt->error = createException(MAL, "json.parser", "Syntax error: False expected");
		return idx;
	default:
		if (*j == '-' || (*j >= '0' && *j <= '9')) {
			jt->elm[idx].value = j;
			msg = JSONnumberParser(j, next, silent);
			if (!silent && msg)
				jt->error = msg;
			jt->elm[idx].kind = JSON_NUMBER;
			jt->elm[idx].valuelen = *next - jt->elm[idx].value;
			return idx;
		}
		if (!silent)
			jt->error = createException(MAL, "json.parser", "Syntax error: value expected");
		return idx;
	}
	return idx;
}


static JSON *
JSONparse(char *j, int silent)
{
	JSON *jt = JSONnewtree(0);

	skipblancs(j);
	if (!*j || !(*j == '{' || *j == '[')) {
		jt->error = createException(MAL, "json.parser", "Syntax error: json parse failed, expecting '{', '['");
		return jt;
	}
	JSONtoken(jt, j, &j, silent);
	if (jt && jt->error)
		return jt;
	skipblancs(j);
	if (*j) {
		if (!silent)
			jt->error = createException(MAL, "json.parser", "Syntax error: json parse failed");
	}
	return jt;
}

str
JSONlength(int *ret, json *j)
{
	int i, cnt = 0;
	JSON *jt = JSONparse(*j, FALSE);

	CHECK_JSON(jt);
	for (i = jt->elm[0].next; i; i = jt->elm[i].next)
		cnt++;
	*ret = cnt;
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

	jt = JSONparse(*js, FALSE);

	CHECK_JSON(jt);
	l = strlen(*js) + 1;
	s = GDKmalloc(l);
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

	jt = JSONparse(*js, FALSE);

	CHECK_JSON(jt);
	l = strlen(*js) + 1;
	s = GDKmalloc(l);
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
	jt = JSONparse(*js, FALSE);
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
	jt = JSONparse(*js, FALSE);
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

static void
JSONunfoldContainer(JSON *jt, int idx, BAT *bo, BAT *bk, BAT *bv, oid *o)
{
	int i, last;
	int cnt = 0;
	char *r;

	last = jt->elm[idx].tail;
	if (jt->elm[idx].kind == JSON_OBJECT)
		for (i = jt->elm[idx].next; i; i = jt->elm[i].next) {
			r = JSONgetValue(jt, i);
			BUNappend(bk, r, FALSE);
			GDKfree(r);
			r = JSONgetValue(jt, jt->elm[i].child);
			BUNappend(bv, r, FALSE);
			if (bo)
				BUNappend(bo, o, FALSE);
			(*o)++;
			GDKfree(r);
			if (i == last)
				break;
	} else if (jt->elm[idx].kind == JSON_ARRAY)
		for (i = jt->elm[idx].next; i; i = jt->elm[i].next) {
			r = GDKstrdup(str_nil);
			BUNappend(bk, r, FALSE);
			if (jt->elm[i].kind == JSON_VALUE)
				r = JSONgetValue(jt, jt->elm[i].child);
			else
				r = JSONgetValue(jt, i);
			BUNappend(bv, r, FALSE);
			if (bo)
				BUNappend(bo, o, FALSE);
			(*o)++;
			cnt++;
			GDKfree(r);
			if (i == last)
				break;
		}
}

static str
JSONunfoldInternal(bat *od, bat *key, bat *val, json *js)
{
	BAT *bo = NULL, *bk, *bv;
	oid o = 0;
	str msg = MAL_SUCCEED;

	JSON *jt = JSONparse(*js, FALSE);

	CHECK_JSON(jt);
	bk = BATnew(TYPE_void, TYPE_str, 64, TRANSIENT);
	if (bk == NULL) {
		JSONfree(jt);
		throw(MAL, "json.unfold", MAL_MALLOC_FAIL);
	}
	BATseqbase(bk, 0);
	bk->hsorted = 1;
	bk->hrevsorted = 0;
	bk->H->nonil = 1;
	bk->tsorted = 1;
	bk->trevsorted = 0;
	bk->T->nonil = 1;

	if (od) {
		bo = BATnew(TYPE_void, TYPE_oid, 64, TRANSIENT);
		if (bo == NULL) {
			BBPunfix(bk->batCacheid);
			JSONfree(jt);
			throw(MAL, "json.unfold", MAL_MALLOC_FAIL);
		}
		BATseqbase(bo, 0);
		bo->hsorted = 1;
		bo->hrevsorted = 0;
		bo->H->nonil = 1;
		bo->tsorted = 1;
		bo->trevsorted = 0;
		bo->T->nonil = 1;
	}

	bv = BATnew(TYPE_void, TYPE_json, 64, TRANSIENT);
	if (bv == NULL) {
		JSONfree(jt);
		if (od)
			BBPunfix(bo->batCacheid);
		BBPunfix(bk->batCacheid);
		throw(MAL, "json.unfold", MAL_MALLOC_FAIL);
	}
	BATseqbase(bv, 0);
	bv->hsorted = 1;
	bv->hrevsorted = 0;
	bv->H->nonil = 1;
	bv->tsorted = 1;
	bv->trevsorted = 0;
	bv->T->nonil = 1;

	if (jt->elm[0].kind == JSON_ARRAY || jt->elm[0].kind == JSON_OBJECT)
		JSONunfoldContainer(jt, 0, (od ? bo : 0), bk, bv, &o);
	else
		msg = createException(MAL, "json.unfold", "JSON object or array expected");
	JSONfree(jt);
	BBPkeepref(*key = bk->batCacheid);
	BBPkeepref(*val = bv->batCacheid);
	if (od)
		BBPkeepref(*od = bo->batCacheid);
	return msg;
}



str
JSONkeyTable(bat *ret, json *js)
{
	BAT *bn;
	char *r;
	int i;
	JSON *jt;

	jt = JSONparse(*js, FALSE);	// already validated
	CHECK_JSON(jt);
	bn = BATnew(TYPE_void, TYPE_str, 64, TRANSIENT);
	if (bn == NULL)
		throw(MAL, "json.keys", MAL_MALLOC_FAIL);
	BATseqbase(bn, 0);
	bn->hsorted = 1;
	bn->hrevsorted = 0;
	bn->H->nonil = 1;
	bn->tsorted = 1;
	bn->trevsorted = 0;
	bn->T->nonil = 1;

	for (i = jt->elm[0].next; i; i = jt->elm[i].next) {
		r = JSONgetValue(jt, i);
		BUNappend(bn, r, FALSE);
		GDKfree(r);
	}
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

	jt = JSONparse(*js, FALSE);	// already validated

	CHECK_JSON(jt);
	if (jt->elm[0].kind == JSON_OBJECT)
		for (i = jt->elm[0].next; i; i = jt->elm[i].next) {
			r = GDKzalloc(jt->elm[i].valuelen + 3);
			if (jt->elm[i].valuelen)
				strncpy(r, jt->elm[i].value - 1, jt->elm[i].valuelen + 2);
			result = JSONglue(result, r, ',');
	} else
		throw(MAL, "json.keyarray", "Object expected");
	r = (char *) GDKstrdup("[");
	result = JSONglue(r, result, 0);
	r = (char *) GDKstrdup("]");
	*ret = JSONglue(result, r, 0);
	return MAL_SUCCEED;
}


str
JSONvalueTable(bat *ret, json *js)
{
	BAT *bn;
	char *r;
	int i;
	JSON *jt;

	jt = JSONparse(*js, FALSE);	// already validated
	CHECK_JSON(jt);
	bn = BATnew(TYPE_void, TYPE_json, 64, TRANSIENT);
	if (bn == NULL)
		throw(MAL, "json.values", MAL_MALLOC_FAIL);
	BATseqbase(bn, 0);
	bn->hsorted = 1;
	bn->hrevsorted = 0;
	bn->H->nonil = 1;
	bn->tsorted = 1;
	bn->trevsorted = 0;
	bn->T->nonil = 1;

	for (i = jt->elm[0].next; i; i = jt->elm[i].next) {
		if (jt->elm[i].kind == JSON_ELEMENT)
			r = JSONgetValue(jt, jt->elm[i].child);
		else
			r = JSONgetValue(jt, i);
		BUNappend(bn, r, FALSE);
		GDKfree(r);
	}
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

	jt = JSONparse(*js, FALSE);	// already validated

	CHECK_JSON(jt);
	if (jt->elm[0].kind == JSON_OBJECT)
		for (i = jt->elm[0].next; i; i = jt->elm[i].next) {
			r = JSONgetValue(jt, jt->elm[i].child);
			result = JSONglue(result, r, ',');
	} else
		throw(MAL, "json.valuearray", "Object expected");
	r = (char *) GDKstrdup("[");
	result = JSONglue(r, result, 0);
	r = (char *) GDKstrdup("]");
	*ret = JSONglue(result, r, 0);
	return MAL_SUCCEED;
}

static BAT **
JSONargumentlist(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, error = 0, error2 = 0, bats = 0;
	BUN cnt = 0;
	BAT **bl;

	bl = (BAT **) GDKzalloc(sizeof(*bl) * pci->argc);
	for (i = pci->retc; i < pci->argc; i++)
		if (isaBatType(getArgType(mb, pci, i))) {
			bats++;
			bl[i] = BATdescriptor(stk->stk[getArg(pci, i)].val.bval);
			if (bl[i] == 0)
				error++;
			error2 |= (cnt > 0 && BATcount(bl[i]) != cnt);
			cnt = BATcount(bl[i]);
		}
	if (error + error2 || bats == 0) {
		GDKfree(bl);
		bl = 0;
	}
	return bl;
}

static str
JSONrenderRowObject(BAT **bl, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, BUN idx)
{
	int i, tpe;
	char *row, *name = 0, *val = 0;
	size_t len, lim, l;
	void *p;
	BATiter bi;

	row = (char *) GDKmalloc(lim = BUFSIZ);
	row[0] = '{';
	row[1] = 0;
	len = 1;
	val = (char *) GDKmalloc(BUFSIZ);
	for (i = pci->retc; i < pci->argc; i += 2) {
		name = stk->stk[getArg(pci, i)].val.sval;
		bi = bat_iterator(bl[i + 1]);
		p = BUNtail(bi, BUNfirst(bl[i + 1]) + idx);
		tpe = getColumnType(getArgType(mb, pci, i + 1));
		ATOMformat(tpe, p, &val);
		if (strncmp(val, "nil", 3) == 0)
			strcpy(val, "null");
		l = strlen(name) + strlen(val);
		while (l > lim - len)
			row = (char *) GDKrealloc(row, lim += BUFSIZ);
		snprintf(row + len, lim - len, "\"%s\":%s,", name, val);
		len += l + 4;
	}
	if (row[1])
		row[len - 1] = '}';
	else {
		row[1] = '}';
		row[2] = 0;
	}
	GDKfree(val);
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
	for (i = pci->retc; i < pci->argc; i += 2)
		if (getArgType(mb, pci, i) != TYPE_str)
			throw(MAL, "json.renderobject", "Keys missing");

	cnt = BATcount(bl[pci->retc + 1]);
	result = (char *) GDKmalloc(lim = BUFSIZ);
	result[0] = '[';
	result[1] = 0;
	len = 1;

	for (j = 0; j < cnt; j++) {
		row = JSONrenderRowObject(bl, mb, stk, pci, j);
		l = strlen(row);
		while (l + 2 > lim - len)
			row = (char *) GDKrealloc(row, lim = cnt * l <= lim ? cnt * l : lim + BUFSIZ);
		strcpy(result + len, row);
		GDKfree(row);
		len += l;
		result[len++] = ',';
		result[len] = 0;
	}
	result[len - 1] = ']';
	ret = getArgReference_TYPE(stk, pci, 0, json);
	*ret = result;
	return MAL_SUCCEED;
}

static str
JSONrenderRowArray(BAT **bl, MalBlkPtr mb, InstrPtr pci, BUN idx)
{
	int i, tpe;
	char *row, *val = 0;
	size_t len, lim, l;
	void *p;
	BATiter bi;

	row = (char *) GDKmalloc(lim = BUFSIZ);
	row[0] = '[';
	row[1] = 0;
	len = 1;
	val = (char *) GDKmalloc(BUFSIZ);
	for (i = pci->retc; i < pci->argc; i++) {
		bi = bat_iterator(bl[i]);
		p = BUNtail(bi, BUNfirst(bl[i]) + idx);
		tpe = getColumnType(getArgType(mb, pci, i));
		ATOMformat(tpe, p, &val);
		if (strncmp(val, "nil", 3) == 0)
			strcpy(val, "null");
		l = strlen(val);
		while (l > lim - len)
			row = (char *) GDKrealloc(row, lim += BUFSIZ);
		snprintf(row + len, lim - len, "%s,", val);
		len += l + 1;
	}
	if (row[1])
		row[len - 1] = ']';
	else {
		row[1] = '}';
		row[2] = 0;
	}
	GDKfree(val);
	return row;
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
	result = (char *) GDKmalloc(lim = BUFSIZ);
	result[0] = '[';
	result[1] = 0;
	len = 1;

	for (j = 0; j < cnt; j++) {
		row = JSONrenderRowArray(bl, mb, pci, j);
		l = strlen(row);
		while (l + 2 > lim - len)
			row = (char *) GDKrealloc(row, lim = cnt * l <= lim ? cnt * l : lim + BUFSIZ);
		strcpy(result + len, row);
		GDKfree(row);
		len += l;
		result[len++] = ',';
		result[len] = 0;
	}
	result[len - 1] = ']';
	ret = getArgReference_TYPE(stk, pci, 0, json);
	*ret = result;
	return MAL_SUCCEED;
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
			*ret = GDKstrdup(str_nil);
			throw(MAL, "json.fold", RUNTIME_OBJECT_MISSING);
		}
	}

	bv = BATdescriptor(*values);
	if (bv == NULL) {
		if (bk)
			BBPunfix(bk->batCacheid);
		*ret = GDKstrdup(str_nil);
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

	row = (char *) GDKmalloc(lim = BUFSIZ);
	row[0] = '[';
	row[1] = 0;
	len = 1;
	val = (char *) GDKmalloc(BUFSIZ);
	if (id) {
		boi = bat_iterator(bo);
		o = *(oid *) BUNtail(boi, BUNfirst(bo));
	}
	if (bk)
		bki = bat_iterator(bk);
	bvi = bat_iterator(bv);

	for (i = 0; i < cnt; i++) {
		if (id &&bk) {
			p = BUNtail(boi, BUNfirst(bo) + i);
			if (*(oid *) p != o) {
				snprintf(row + len, lim - len, ", ");
				len += 2;
				o = *(oid *) p;
			}
		}

		if (bk) {
			nme = (str) BUNtail(bki, BUNfirst(bk) + i);
			l = strlen(nme);
			while (l + 3 > lim - len)
				row = (char *) GDKrealloc(row, lim = (lim / (i + 1)) * cnt + BUFSIZ + l + 3);
			if (row == NULL) {
				*ret = GDKstrdup(str_nil);
				if (bo)
					BBPunfix(bo->batCacheid);
				if (bk)
					BBPunfix(bk->batCacheid);
				BBPunfix(bv->batCacheid);
				throw(MAL, "json.fold", MAL_MALLOC_FAIL);
			}
			if (strcmp(nme, str_nil)) {
				snprintf(row + len, lim - len, "\"%s\":", nme);
				len += l + 3;
			}
		}

		bvi = bat_iterator(bv);
		p = BUNtail(bvi, BUNfirst(bv) + i);
		if (tpe == TYPE_json)
			val = p;
		else {
			ATOMformat(tpe, p, &val);
			if (strncmp(val, "nil", 3) == 0)
				strcpy(val, "null");
		}
		l = strlen(val);
		while (l > lim - len)
			row = (char *) GDKrealloc(row, lim = (lim / (i + 1)) * cnt + BUFSIZ + l + 3);

		if (row == NULL) {
			if (bo)
				BBPunfix(bo->batCacheid);
			if (bk)
				BBPunfix(bk->batCacheid);
			BBPunfix(bv->batCacheid);
			*ret = GDKstrdup(str_nil);
			throw(MAL, "json.fold", MAL_MALLOC_FAIL);
		}
		strncpy(row + len, val, l);
		len += l;
		row[len++] = ',';
		row[len] = 0;
	}
	if (row[1]) {
		row[len - 1] = ']';
		row[len] = 0;
	} else {
		row[1] = ']';
		row[2] = 0;
	}
	if (tpe != TYPE_json)
		GDKfree(val);
	if (bo)
		BBPunfix(bo->batCacheid);
	if (bk)
		BBPunfix(bk->batCacheid);
	BBPunfix(bv->batCacheid);
	*ret = row;
	return MAL_SUCCEED;
}

str
JSONunfold(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *id = 0, *key = 0, *val = 0;
	json *js;

	(void) cntxt;
	(void) mb;

	if (pci->retc == 1) {
		val = getArgReference_bat(stk, pci, 0);
	} else if (pci->retc == 2) {
		id = 0;
		key = getArgReference_bat(stk, pci, 0);
		val = getArgReference_bat(stk, pci, 1);
	} else if (pci->retc == 3) {
		id = getArgReference_bat(stk, pci, 0);
		key = getArgReference_bat(stk, pci, 1);
		val = getArgReference_bat(stk, pci, 2);
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
	if (pci->argc - pci->retc == 1) {
		val = getArgReference_bat(stk, pci, 1);
	} else if (pci->argc - pci->retc == 2) {
		id = 0;
		key = getArgReference_bat(stk, pci, 1);
		val = getArgReference_bat(stk, pci, 2);
	} else {
		assert(pci->argc - pci->retc == 3);
		id = getArgReference_bat(stk, pci, 1);
		key = getArgReference_bat(stk, pci, 2);
		val = getArgReference_bat(stk, pci, 3);
	}
	ret = getArgReference_TYPE(stk, pci, 0, json);
	return JSONfoldKeyValue(ret, id, key, val);
}

str
JSONtextString(str *ret, bat *bid)
{
	(void) ret;
	(void) bid;
	throw(MAL, "json.text", "tobeimplemented");
}


str
JSONtextGrouped(bat *ret, bat *bid, bat *gid, bat *ext, bit *flg)
{
	(void) ret;
	(void) bid;
	(void) gid;
	(void) ext;
	(void) flg;
	throw(MAL, "json.text", "tobeimplemented");
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
		throw(MAL, "json.group", MAL_MALLOC_FAIL);
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
			nil = (*val == dbl_nil);
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
	BUN ngrp, start, end, cnt;
	BUN nils = 0;
	int isnil;
	const oid *cand = NULL, *candend = NULL;
	const char *v = NULL;
	const oid *grps, *map;
	oid mapoff = 0;
	oid prev;
	BUN p, q;
	int freeb = 0, freeg = 0;
	char *buf = NULL;
	size_t buflen, maxlen, len;
	const char *err;
	char temp[128] = "";
	const double *val = NULL;

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end, &cnt, &cand, &candend)) !=NULL) {
		return err;
	}
	assert(b->ttype == TYPE_str || b->ttype == TYPE_dbl);
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
	bn = BATnew(TYPE_void, TYPE_str, ngrp, TRANSIENT);
	if (bn == NULL) {
		err = MAL_MALLOC_FAIL;
		goto out;
	}
	bi = bat_iterator(b);
	if (g) {
		/* stable sort g */
		if (BATsort(&t1, &t2, NULL, g, NULL, NULL, 0, 1) != GDK_SUCCEED) {
			BBPreclaim(bn);
			bn = NULL;
			err = "internal sort failed";
			goto out;
		}
		if (freeg)
			BBPunfix(g->batCacheid);
		g = t1;
		freeg = 1;
		if (t2->ttype == TYPE_void) {
			map = NULL;
			mapoff = b->tseqbase;
		} else {
			map = (const oid *) Tloc(t2, BUNfirst(t2));
		}
		if (g && BATtdense(g)) {
			for (p = 0, q = BATcount(g); p < q; p++) {
				switch (b->ttype) {
				case TYPE_str:
					v = (const char *) BUNtail(bi, BUNfirst(b) + (map ? (BUN) map[p] : p + mapoff));
					break;
				case TYPE_dbl:
					val = (const double *) BUNtail(bi, BUNfirst(b) + (map ? (BUN) map[p] : p + mapoff));
					if (*val != dbl_nil) {
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
							strncpy(buf, "[ null ]", maxlen - buflen);
							buflen += strlen("[ null ]");
							isnil = 1;
						} else {
							continue;
						}
					} else {
						strncpy(buf, str_nil, buflen);
						isnil = 1;
					}
				} else {
					len = strlen(v);
					if (len >= maxlen - buflen) {
						maxlen += len + BUFSIZ;
						buf = GDKrealloc(buf, maxlen);
						if (buf == NULL) {
							err = MAL_MALLOC_FAIL;
							goto bunins_failed;
						}
					}
					switch (b->ttype) {
					case TYPE_str:
						len = snprintf(buf + buflen, maxlen - buflen, "[ \"%s\" ]", v);
						buflen += len;
						break;
					case TYPE_dbl:
						len = snprintf(buf + buflen, maxlen - buflen, "[ %s ]", v);
						buflen += len;
						break;
					}
				}
				bunfastapp_nocheck(bn, BUNlast(bn), buf, Tsize(bn));
				buflen = 0;
			}
			BATseqbase(bn, min);
			bn->T->nil = nils != 0;
			bn->T->nonil = nils == 0;
			bn->T->sorted = BATcount(bn) <= 1;
			bn->T->revsorted = BATcount(bn) <= 1;
			bn->T->key = BATcount(bn) <= 1;
			goto out;
		}
		grps = (const oid *) Tloc(g, BUNfirst(g));
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
				v = (const char *) BUNtail(bi, BUNfirst(b) + (map ? (BUN) map[p] : p + mapoff));
				break;
			case TYPE_dbl:
				val = (const double *) BUNtail(bi, BUNfirst(b) + (map ? (BUN) map[p] : p + mapoff));
				if (*val != dbl_nil) {
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
					buf = GDKrealloc(buf, maxlen);
					if (buf == NULL) {
						err = MAL_MALLOC_FAIL;
						goto bunins_failed;
					}
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
		for (p = BUNfirst(b), q = p + BATcount(b); p < q; p++) {
			switch (b->ttype) {
			case TYPE_str:
				v = (const char *) BUNtail(bi, p);
				break;
			case TYPE_dbl:
				val = (const double *) BUNtail(bi, p);
				if (*val != dbl_nil) {
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
				buf = GDKrealloc(buf, maxlen);
				if (buf == NULL) {
					err = MAL_MALLOC_FAIL;
					goto bunins_failed;
				}
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
	BATseqbase(bn, min);
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;
	bn->T->sorted = BATcount(bn) <= 1;
	bn->T->revsorted = BATcount(bn) <= 1;
	bn->T->key = BATcount(bn) <= 1;

      out:
	if (t2)
		BBPunfix(t2->batCacheid);
	if (freeb)
		BBPunfix(b->batCacheid);
	if (freeg)
		BBPunfix(g->batCacheid);
	if (buf)
		GDKfree(buf);
	*bnp = bn;
	return err;

      bunins_failed:
	BBPreclaim(bn);
	bn = NULL;
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
	if (b == NULL || (gid != NULL && g == NULL) || (eid != NULL && e == NULL)) {

		if (b)
			BBPunfix(b->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		throw(MAL, "aggr.subjson", RUNTIME_OBJECT_MISSING);
	}
	if (sid) {
		s = BATdescriptor(*sid);
		if (s == NULL) {
			BBPunfix(b->batCacheid);
			if (g)
				BBPunfix(g->batCacheid);
			if (e)
				BBPunfix(e->batCacheid);
			throw(MAL, "aggr.subjson", RUNTIME_OBJECT_MISSING);
		}
	} else {
		s = NULL;
	}
	err = JSONjsonaggr(&bn, b, g, e, s, *skip_nils);
	BBPunfix(b->batCacheid);
	if (g)
		BBPunfix(g->batCacheid);
	if (e)
		BBPunfix(e->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (err !=NULL)
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
