/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * Martin Kersten
 */
#include "monetdb_config.h"
#include "json_atom.h"
#include "mal.h"
#include <mal_instruction.h>
#include <mal_interpreter.h>

// just validate the string according to www.json.org
// A straightforward recursive solution
#define skipblancs for(; *j; j++) \
	if( *j != ' ' && *j != '\n' && *j != '\t') break;

#define hex if ( *j >='0' && *j <='7') j++;

int TYPE_json;

static str JSONparse(char *j);

int
JSONfromString(str src, int *len, json *j)
{
	ssize_t slen = (ssize_t) strlen(src);
	if ((ssize_t) *len < slen)
		*j = GDKrealloc(*j, slen + 1);
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
	size_t ll;
	int l, cnt = 0;
	char *c, *dst;

	if (GDK_STRNIL(src)) {
		*s = GDKstrdup("null");
		return 0;
	}
	for (c = src; *c; c++)
		switch (*c) {
		case '"':
		case '\\':
		case '\n':
			cnt++;
		}
	ll = strlen(src);
	assert(ll <= (size_t) INT_MAX);
	l = (int) ll + cnt + 3;

	if (l >= *len) {
		GDKfree(*s);
		*s = (str) GDKmalloc(l);
		if (*s == NULL)
			return 0;
	}
	dst = *s;
	*dst++ = '"';
	for (c = src; *c; c++)
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
	*dst++ = '"';
	*dst = 0;
	*len = l - 1;
	return *len;
}

str
JSONjson2str(str *ret, json *j)
{
	char *s = *j;
	if (*s == '"')
		s++;
	s = GDKstrdup(s);
	if (s[strlen(s) - 1] == '"')
		s[strlen(s) - 1] = 0;
	*ret = s;
	return MAL_SUCCEED;
}

str
JSONstr2json(json *ret, str *j)
{
	str msg = JSONparse(*j);

	if (msg) {
		*ret = 0;
		return msg;
	}
	*ret = GDKstrdup(*j);
	return MAL_SUCCEED;
}

str
JSONisvalid(bit *ret, json *j)
{
	str msg = JSONparse(*j);

	*ret = 1;
	if (msg) {
		*ret = 0;
		GDKfree(msg);
	}
	return MAL_SUCCEED;
}

str
JSONisobject(bit *ret, json *j)
{
	char *s;
	for (s = *j; *s; s++)
		if (*s != ' ' && *s != '\n' && *s != '\t')
			break;

	*ret = *s == '{';
	return MAL_SUCCEED;
}

str
JSONisarray(bit *ret, json *j)
{
	char *s;
	for (s = *j; *s; s++)
		if (*s != ' ' && *s != '\n' && *s != '\t')
			break;

	*ret = *s == '[';
	return MAL_SUCCEED;
}

str
JSONprelude(int *ret)
{
	(void) ret;
	TYPE_json = ATOMindex("json");
	return MAL_SUCCEED;
}

static str JSONobjectParser(char *j, char **next);
static str JSONarrayParser(char *j, char **next);

static str
JSONstringParser(char *j, char **next)
{
	if (*j == '"')
		j++;
	for (; *j; j++)
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
				break;
			case 'u':
				j++;
				hex;
				hex;
				hex;
				hex;
				break;
			default:
				throw(MAL, "json.parser", "illegal escape char");
			}
			break;
		case '"':
			j++;
			*next = j;
			return MAL_SUCCEED;
		}
	throw(MAL, "json.parser", "Nonterminated string");
}

static str
JSONnumberParser(char *j, char **next)
{
	char *backup= j;
	if (*j == '-')
		j++;
	skipblancs;
	if (*j < '0' || *j > '9')
		throw(MAL, "json.parser", "Number expected");
	if (*j == '0' && *(j + 1) != '.')
		throw(MAL, "json.parser", "Decimal expected");
	for (; *j; j++)
		if (*j < '0' || *j > '9')
			break;
	backup = j;
	skipblancs;
	if (*j == '.') {
		j++;
		skipblancs;
		for (; *j; j++)
			if (*j < '0' || *j > '9')
				break;
		backup = j;
	} else j = backup;
	skipblancs;
	if (*j == 'e' || *j == 'E') {
		j++;
		skipblancs;
		if (*j == '-')
			j++;
		skipblancs;
		for (; *j; j++)
			if (*j < '0' || *j > '9')
				break;
	} else j = backup;

	*next = j;
	return MAL_SUCCEED;
}

static str
JSONvalueParser(char *j, char **next)
{
	str msg = MAL_SUCCEED;

	switch (*j) {
	case '{':
		msg = JSONobjectParser(j + 1, &j);
		break;
	case '[':
		msg = JSONarrayParser(j + 1, &j);
		break;
	case '"':
		msg = JSONstringParser(j + 1, &j);
		break;
	case 'n':
		if (strncmp("null", j, 4) == 0) {
			j += 4;
			break;
		}
		throw(MAL, "json.parser", "NULL expected");
		break;
	case 't':
		if (strncmp("true", j, 4) == 0) {
			j += 4;
			break;
		}
		throw(MAL, "json.parser", "True expected");
		break;
	case 'f':
		if (strncmp("false", j, 5) == 0) {
			j += 5;
			break;
		}
		throw(MAL, "json.parser", "False expected");
		break;
	default:
		if (*j == '-' || (*j >= '0' && *j <= '9'))
			msg = JSONnumberParser(j, &j);
		else
			throw(MAL, "json.parser", "Value expected");
	}
	*next = j;
	return msg;
}

str
JSONarrayParser(char *j, char **next)
{
	str msg;
	for (; *j && *j != ']'; j++) {
		skipblancs;
		if (*j == ']')
			break;
		msg = JSONvalueParser(j, &j);
		if (msg)
			return msg;
		skipblancs;
		if (*j == ']')
			break;
		if (*j != ',')
			throw(MAL, "json.parser", "',' expected");
	}

	if (*j == 0)
		throw(MAL, "json.parser", "']' expected");
	*next = j + 1;
	return MAL_SUCCEED;
}

str
JSONobjectParser(char *j, char **next)
{
	str msg;

	for (; *j && *j != '}'; j++) {
		skipblancs;
		if (*j == '}')
			break;
		if (*j != '"')
			throw(MAL, "json.parser", "Name expected");
		msg = JSONstringParser(j, &j);
		if (msg)
			return msg;
		skipblancs;
		if (*j != ':')
			throw(MAL, "json.parser", "Value expected");
		j++;
		skipblancs;
		msg = JSONvalueParser(j, &j);
		if (msg)
			return msg;
		skipblancs;
		if (*j == '}')
			break;
		if (*j != ',')
			throw(MAL, "json.parser", "',' expected");
	}

	if (*j == 0)
		throw(MAL, "json.parser", "'}' expected");
	*next = j + 1;
	return MAL_SUCCEED;
}

static str
JSONparse(char *j)
{
	str msg;

	while (*j) {
		switch (*j) {
		case ' ':
		case '\n':
		case '\t':
			j++;
			break;
		case '{':
			msg = JSONobjectParser(j + 1, &j);
			if (msg)
				return msg;
			break;
		case '[':
			msg = JSONarrayParser(j + 1, &j);
			if (msg)
				return msg;
			break;
		default:
			throw(MAL, "json.parser", "'{' or '[' expected");
		}
	}
	return MAL_SUCCEED;
}

static str
JSONarrayLength(char *j, int *l)
{
	int cnt = 0;
	char *msg = MAL_SUCCEED;

	for (; *j && *j != ']'; j++) {
		skipblancs;
		if (*j == ']')
			break;
		msg = JSONvalueParser(j, &j);
		if (msg)
			return msg;
		cnt++;
		skipblancs;
		if (*j == ']')
			break;
		if (*j != ',')
			throw(MAL, "json.length", "',' expected");
	}
	*l = cnt;
	return msg;
}

static str
JSONobjectLength(char *j, int *l)
{
	int cnt = 0;
	char *msg = MAL_SUCCEED;
	for (; *j && *j != '}'; j++) {
		skipblancs;
		if (*j == '}')
			break;
		if (*j != '"')
			throw(MAL, "json.length", "Name expected");
		msg = JSONstringParser(j + 1, &j);
		if (msg)
			return msg;
		skipblancs;
		if (*j != ':')
			throw(MAL, "json.length", "Value expected");
		j++;
		skipblancs;
		msg = JSONvalueParser(j, &j);
		if (msg)
			return msg;
		cnt++;
		skipblancs;
		if (*j == '}')
			break;
		if (*j != ',')
			throw(MAL, "json.length", "',' expected");
	}
	*l = cnt;
	return msg;
}

str
JSONlength(int *ret, json *js)
{
	char *s;

	for (s = *js; *s; s++)
		if (*s != ' ' && *s != '\n' && *s != '\t')
			break;
	if (*s == '[')
		return JSONarrayLength(s + 1, ret);
	if (*s == '{')
		return JSONobjectLength(s + 1, ret);
	throw(MAL, "json.length", "Invalid JSON");
}

//the access functions assume a valid json object or 
//single nested array of objects ([{object},..]
//any structure violation leads to an early abort
//The keys should be unique in an object
static str
JSONfilterObjectInternal(json *ret, json *js, str *pat, int flag)
{
	char *namebegin, *nameend;
	char *valuebegin, *valueend, *msg = MAL_SUCCEED;
	char *result = NULL;
	size_t l, lim, len;
	int nesting = 0;
	char *j = *js;

	skipblancs;
	while (*j == '[') {
		nesting++;
		j++;
		skipblancs;
	}

	if (*j != '{')
		throw(MAL, "json.filter", "JSON object expected");

	// the result is an array of values
	result = (char *) GDKmalloc(BUFSIZ);
	if (result == 0)
		throw(MAL, "json.filter", MAL_MALLOC_FAIL);
	result[0] = '[';
	result[1] = 0;
	len = 1;
	lim = BUFSIZ;

	for (j++; *j && *j != '}'; j++) {
		skipblancs;
		if (*j == ']' && nesting) {
			nesting--;
			continue;
		}
		if (*j == '}' || *j == 0)
			break;
		if (*j != '"') {
			msg = createException(MAL, "json.filter", "Name expected");
			goto wrapup;
		}
		namebegin = j + 1;
		msg = JSONstringParser(j + 1, &j);
		if (msg)
			goto wrapup;
		nameend = j - 1;
		skipblancs;
		if (*j != ':') {
			msg = createException(MAL, "json.filter", "Value expected");
			goto wrapup;
		}
		j++;
		skipblancs;
		valuebegin = j;
		msg = JSONvalueParser(j, &j);
		if (msg)
			goto wrapup;
		valueend = j;

		// test for candidate member
		if (strncmp(*pat, namebegin, (l = nameend - namebegin)) == 0) {
			if (l + 2 > lim)
				result = GDKrealloc(result, lim += BUFSIZ);
			if (strcmp("null", result) == 0) {
				strncpy(result + len, "nil", 3);
				len += 3;
			} else {
				strncpy(result + len, valuebegin, valueend - valuebegin);
				len += valueend - valuebegin;
			}
			result[len++] = ',';
			result[len] = 0;
			if (flag == 0)
				goto found;
		}
		skipblancs;
		if (*j == '}') {
			if (nesting) {
				while (*j && *j != '{' && *j != ']')
					j++;
				if (*j != '{')
					j--;
			}
			continue;
		}

		if (*j != ',')
			msg = createException(MAL, "json.filter", "',' expected");
	}
      found:
	if (result[1] == 0) {
		result[1] = ']';
		result[2] = 0;
	} else
		result[len - 1] = ']';
      wrapup:;
	*ret = result;
	return msg;
}

str
JSONfilterObject(json *ret, json *js, str *pat)
{
	return JSONfilterObjectInternal(ret, js, pat, 0);
}

str
JSONfilterObjectAll(json *ret, json *js, str *pat)
{
	return JSONfilterObjectInternal(ret, js, pat, 1);
}

str
JSONfilterArray(json *ret, json *js, int *index)
{
	char *valuebegin, *valueend, *msg = MAL_SUCCEED;
	char *result = NULL, *j = *js;
	size_t l, len, lim;
	int idx = *index;

	skipblancs;
	if (*j != '[')
		throw(MAL, "json.filter", "JSON object expected");

	result = (char *) GDKmalloc(BUFSIZ);
	if (result == 0)
		throw(MAL, "json.filter", MAL_MALLOC_FAIL);
	result[0] = '[';
	result[1] = 0;
	len = 1;
	lim = BUFSIZ;

	for (j++; *j && *j != ']'; j++) {
		skipblancs;
		if (*j == ']') {
			break;
		}
		valuebegin = j;
		msg = JSONvalueParser(j, &j);
		if (msg)
			goto wrapup;
		valueend = j;

		// test for candidate member
		if (idx == 0) {
			l = valueend - valuebegin;
			if (l + 2 > lim - len)
				result = GDKrealloc(result, lim += BUFSIZ);
			if (strcmp("null", result) == 0) {
				strncpy(result + len, "nil", 3);
				len += 3;
			} else {
				strncpy(result + len, valuebegin, valueend - valuebegin);
				len += valueend - valuebegin;
			}
			result[len++] = ']';
			result[len] = 0;
			break;
		}
		skipblancs;
		if (*j == ']')
			break;
		if (*j != ',')
			msg = createException(MAL, "json.filter", "',' expected");
		idx--;
	}
	if (result[1] == 0) {
		result[1] = ']';
		result[2] = 0;
	}
      wrapup:;
	*ret = result;
	return msg;
}

str
JSONunnest(int *key, int *val, json *js)
{
	BAT *bk, *bv;
	char *namebegin, *nameend;
	char *valuebegin, *valueend;
	char *msg = MAL_SUCCEED;
	char *result = NULL;
	size_t l, lim;
	int nesting = 0, valuelist = 0;
	char *j = *js;

	bk = BATnew(TYPE_void, TYPE_str, 64);
	if (bk == NULL)
		throw(MAL, "json.unnest", MAL_MALLOC_FAIL);
	BATseqbase(bk, 0);
	bk->hsorted = 1;
	bk->hrevsorted = 0;
	bk->H->nonil = 1;
	bk->tsorted = 1;
	bk->trevsorted = 0;
	bk->T->nonil = 1;

	bv = BATnew(TYPE_void, TYPE_json, 64);
	if (bv == NULL) {
		BBPreleaseref(bk->batCacheid);
		throw(MAL, "json.unnest", MAL_MALLOC_FAIL);
	}
	BATseqbase(bv, 0);
	bv->hsorted = 1;
	bv->hrevsorted = 0;
	bv->H->nonil = 1;
	bv->tsorted = 1;
	bv->trevsorted = 0;
	bv->T->nonil = 1;

	skipblancs;
	// unnest {} or [{},...] or [val,...]
	// in the latter case the name table remains empty
	if (*j == '[') {
		nesting++;
		j++;
		skipblancs;
	}
	valuelist = *j != '{';

	// the result is an array of values
	result = (char *) GDKmalloc(BUFSIZ);
	if (result == 0) {
		BBPreleaseref(bk->batCacheid);
		BBPreleaseref(bv->batCacheid);
		throw(MAL, "json.unnest", MAL_MALLOC_FAIL);
	}
	lim = BUFSIZ;

	for ((valuelist ? j : j++); *j && *j != '}'; j++) {
		skipblancs;
		if (*j == ']') {
			break;
		}
		if (*j == '}')
			break;
		if (!valuelist) {
			if (*j != '"') {
				msg = createException(MAL, "json.unnest", "Name expected");
				goto wrapup;
			}
			namebegin = j + 1;
			msg = JSONstringParser(j + 1, &j);
			if (msg)
				goto wrapup;
			nameend = j - 1;
			l = nameend - namebegin;
			if (l + 2 > lim)
				result = GDKrealloc(result, lim += BUFSIZ);
			strncpy(result, namebegin, nameend - namebegin);
			result[l] = 0;
			BUNappend(bk, result, FALSE);

			skipblancs;
			if (*j != ':') {
				msg = createException(MAL, "json.unnest", "Value expected");
				goto wrapup;
			}
			j++;
			skipblancs;
		}
		valuebegin = j;
		msg = JSONvalueParser(j, &j);
		if (msg)
			goto wrapup;
		valueend = j;
		l = valueend - valuebegin;
		if (l + 2 > lim)
			result = GDKrealloc(result, lim += BUFSIZ);
		strncpy(result, valuebegin, l);
		result[l] = 0;
		BUNappend(bv, result, FALSE);

		skipblancs;
		if (*j == '}') {
			if (!valuelist) {
				while (*j && *j != '{')
					j++;
				if (*j != '{')
					j--;
			}
			continue;
		}
		if (*j != ',' && !(valuelist && *j == ']'))
			msg = createException(MAL, "json.unnest", "',' expected");
	}
      wrapup:;
	BBPkeepref(*key = bk->batCacheid);
	BBPkeepref(*val = bv->batCacheid);
	GDKfree(result);
	return msg;
}

str
JSONunnestOne(int *val, json *js)
{
	BAT *bv;
	char *valuebegin, *valueend;
	char *msg = MAL_SUCCEED;
	char *result = NULL;
	size_t l, lim;
	char *j = *js;

	bv = BATnew(TYPE_void, TYPE_json, 64);
	if (bv == NULL) {
		throw(MAL, "json.unnest", MAL_MALLOC_FAIL);
	}
	BATseqbase(bv, 0);
	bv->hsorted = 1;
	bv->hrevsorted = 0;
	bv->H->nonil = 1;
	bv->tsorted = 1;
	bv->trevsorted = 0;
	bv->T->nonil = 1;

	skipblancs;
	// unnest a list
	if (*j != '[')
		throw(MAL, "json.unnest", "JSON list expected");

	// the result is an array of values
	result = (char *) GDKmalloc(BUFSIZ);
	if (result == 0) {
		BBPreleaseref(bv->batCacheid);
		throw(MAL, "json.unnest", MAL_MALLOC_FAIL);
	}
	lim = BUFSIZ;

	for (j++; *j && *j != ']'; j++) {
		skipblancs;
		if (*j == ']') {
			break;
		}
		valuebegin = j;
		msg = JSONvalueParser(j, &j);
		if (msg)
			goto wrapup;
		valueend = j;
		l = valueend - valuebegin;
		if (l + 2 > lim)
			result = GDKrealloc(result, lim += BUFSIZ);
		strncpy(result, valuebegin, l);
		result[l] = 0;
		BUNappend(bv, result, FALSE);

		skipblancs;
		if (*j == ']')
			continue;
		if (*j && *j != ',')
			msg = createException(MAL, "json.unnest", "',' expected");
	}
      wrapup:;
	BBPkeepref(*val = bv->batCacheid);
	GDKfree(result);
	return msg;
}

str
JSONunnestGrouped(int *grp, int *key, int *val, json *js)
{
	BAT *bk, *bv, *bg;
	char *namebegin, *nameend;
	char *valuebegin, *valueend;
	char *msg = MAL_SUCCEED;
	char *result = NULL;
	size_t l, lim;
	int nesting = 0;
	char *j = *js;
	oid o = 0;

	bk = BATnew(TYPE_void, TYPE_str, 64);
	if (bk == NULL)
		throw(MAL, "json.unnest", MAL_MALLOC_FAIL);
	BATseqbase(bk, 0);
	bk->hsorted = 1;
	bk->hrevsorted = 0;
	bk->H->nonil = 1;
	bk->tsorted = 1;
	bk->trevsorted = 0;
	bk->T->nonil = 1;

	bv = BATnew(TYPE_void, TYPE_json, 64);
	if (bv == NULL) {
		BBPreleaseref(bk->batCacheid);
		throw(MAL, "json.unnest", MAL_MALLOC_FAIL);
	}
	BATseqbase(bv, 0);
	bv->hsorted = 1;
	bv->hrevsorted = 0;
	bv->H->nonil = 1;
	bv->tsorted = 1;
	bv->trevsorted = 0;
	bv->T->nonil = 1;

	bg = BATnew(TYPE_void, TYPE_oid, 64);
	if (bg == NULL) {
		BBPreleaseref(bk->batCacheid);
		BBPreleaseref(bv->batCacheid);
		throw(MAL, "json.unnest", MAL_MALLOC_FAIL);
	}
	BATseqbase(bg, 0);
	bg->hsorted = 1;
	bg->hrevsorted = 0;
	bg->H->nonil = 1;
	bg->tsorted = 1;
	bg->trevsorted = 0;
	bg->T->nonil = 1;

	skipblancs;
	while (*j == '[') {
		nesting++;
		j++;
	}
	if (*j != '{')
		throw(MAL, "json.unnest", "JSON object expected");

	// the result is an array of values
	result = (char *) GDKmalloc(BUFSIZ);
	if (result == 0) {
		BBPreleaseref(bk->batCacheid);
		BBPreleaseref(bv->batCacheid);
		throw(MAL, "json.unnest", MAL_MALLOC_FAIL);
	}
	lim = BUFSIZ;

	for (j++; *j && *j != '}'; j++) {
		skipblancs;
		if (*j == ']') {
			break;
		}
		if (*j == '}')
			break;
		if (*j != '"') {
			msg = createException(MAL, "json.unnest", "Name expected");
			goto wrapup;
		}
		namebegin = j + 1;
		msg = JSONstringParser(j + 1, &j);
		if (msg)
			goto wrapup;
		nameend = j - 1;
		l = nameend - namebegin;
		if (l + 2 > lim)
			result = GDKrealloc(result, lim += BUFSIZ);
		strncpy(result, namebegin, nameend - namebegin);
		result[l] = 0;
		BUNappend(bk, result, FALSE);

		skipblancs;
		if (*j != ':') {
			msg = createException(MAL, "json.unnest", "Value expected");
			goto wrapup;
		}
		j++;
		skipblancs;
		valuebegin = j;
		msg = JSONvalueParser(j, &j);
		if (msg)
			goto wrapup;
		valueend = j;
		l = valueend - valuebegin;
		if (l + 2 > lim)
			result = GDKrealloc(result, lim += BUFSIZ);
		strncpy(result, valuebegin, l);
		result[l] = 0;
		BUNappend(bv, result, FALSE);

		BUNappend(bg, &o, FALSE);

		skipblancs;
		if (*j == '}') {
			if (nesting) {
				while (*j && *j != '{' && *j != ']')
					j++;
				if (*j != '{')
					j--;
				else
					o++;
			}
			continue;
		}
		if (*j != ',')
			msg = createException(MAL, "json.unnest", "',' expected");
	}
      wrapup:;
	BBPkeepref(*key = bk->batCacheid);
	BBPkeepref(*val = bv->batCacheid);
	BBPkeepref(*grp = bg->batCacheid);
	GDKfree(result);
	return msg;
}

str
JSONkeys(int *ret, json *js)
{
	BAT *bn;
	char *namebegin, *nameend;
	char *msg = MAL_SUCCEED;
	char *result = NULL;
	size_t l, lim;
	char *j = *js;

	bn = BATnew(TYPE_void, TYPE_str, 64);
	BATseqbase(bn, 0);
	bn->hsorted = 1;
	bn->hrevsorted = 0;
	bn->H->nonil = 1;
	bn->tsorted = 1;
	bn->trevsorted = 0;
	bn->T->nonil = 1;

	skipblancs;
	if (*j != '{')
		throw(MAL, "json.filter", "JSON object expected");

	// the result is an array of values
	result = (char *) GDKmalloc(BUFSIZ);
	if (result == 0)
		throw(MAL, "json.keys", MAL_MALLOC_FAIL);
	lim = BUFSIZ;

	for (j++; *j && *j != '}'; j++) {
		skipblancs;
		if (*j == '}')
			break;
		if (*j != '"') {
			msg = createException(MAL, "json.keys", "Name expected");
			goto wrapup;
		}
		namebegin = j + 1;
		msg = JSONstringParser(j + 1, &j);
		if (msg)
			goto wrapup;
		nameend = j - 1;
		l = nameend - namebegin;
		if (l + 2 > lim)
			result = GDKrealloc(result, lim += BUFSIZ);
		strncpy(result, namebegin, nameend - namebegin);
		result[l] = 0;
		BUNappend(bn, result, FALSE);

		skipblancs;
		if (*j != ':') {
			msg = createException(MAL, "json.keys", "Value expected");
			goto wrapup;
		}
		j++;
		skipblancs;
		msg = JSONvalueParser(j, &j);
		if (msg)
			goto wrapup;
		skipblancs;
		if (*j == '}')
			break;
		if (*j != ',')
			msg = createException(MAL, "json.keys", "',' expected");
	}
      wrapup:;
	BBPkeepref(*ret = bn->batCacheid);
	GDKfree(result);
	return msg;
}
str
JSONkeyArray(json *ret, json *js)
{
	char *namebegin, *nameend;
	char *msg = MAL_SUCCEED;
	char *result = NULL;
	size_t l = 0, lim;
	char *j = *js;

	skipblancs;
	if (*j != '{')
		throw(MAL, "json.filter", "JSON object expected");

	// the result is an array of values
	result = (char *) GDKmalloc(BUFSIZ);
	if (result == 0)
		throw(MAL, "json.keys", MAL_MALLOC_FAIL);
	lim = BUFSIZ;

	result[0]='[';
	result[1]= 0;
	l=1;
	for (j++; *j && *j != '}'; j++) {
		skipblancs;
		if (*j == '}')
			break;
		if (*j != '"') {
			msg = createException(MAL, "json.keys", "Name expected");
			goto wrapup;
		}
		if( l > 1){
			result[l++] = ',';
			result[l] = 0;
		}
		namebegin = j + 1;
		msg = JSONstringParser(j + 1, &j);
		if (msg)
			goto wrapup;
		nameend = j - 1;
		if (l + (size_t)(nameend-namebegin) + 5 > lim){
			result = GDKrealloc(result, lim += BUFSIZ);
			if ( result == NULL)
				goto wrapup;
		}
		strncpy(result+l, namebegin-1, nameend - namebegin+2);
		l += nameend - namebegin+2;
		result[l] = 0;

		skipblancs;
		if (*j != ':') {
			msg = createException(MAL, "json.keys", "Value expected");
			goto wrapup;
		}
		j++;
		skipblancs;
		msg = JSONvalueParser(j, &j);
		if (msg)
			goto wrapup;
		skipblancs;
		if (*j == '}')
			break;
		if (*j != ',')
			msg = createException(MAL, "json.keys", "',' expected");
	}
	result[l]=']';
	result[l+1]= 0;
	l=1;
	*ret = result;
	return MAL_SUCCEED;
wrapup:
	GDKfree(result);
	return msg;
}

str
JSONvalueArray(json *ret, json *js)
{
	char *namebegin, *nameend;
	char *msg = MAL_SUCCEED;
	char *result = NULL;
	size_t l = 0, lim;
	char *j = *js;

	skipblancs;
	if (*j != '{')
		throw(MAL, "json.filter", "JSON object expected");

	// the result is an array of values
	result = (char *) GDKmalloc(BUFSIZ);
	if (result == 0)
		throw(MAL, "json.keys", MAL_MALLOC_FAIL);
	lim = BUFSIZ;

	result[0]='[';
	result[1]= 0;
	l=1;
	for (j++; *j && *j != '}'; j++) {
		skipblancs;
		if (*j == '}')
			break;
		if (*j != '"') {
			msg = createException(MAL, "json.keys", "Name expected");
			goto wrapup;
		}
		if( l > 1){
			result[l++] = ',';
			result[l] = 0;
		}
		msg = JSONstringParser(j + 1, &j);
		if (msg)
			goto wrapup;

		skipblancs;
		if (*j != ':') {
			msg = createException(MAL, "json.keys", "Value expected");
			goto wrapup;
		}
		j++;
		skipblancs;
		namebegin = j + 1;
		msg = JSONvalueParser(j, &j);
		if (msg)
			goto wrapup;
		nameend = j - 1;
		if (l + (size_t)(nameend-namebegin) + 5 > lim){
			result = GDKrealloc(result, lim += BUFSIZ);
			if ( result == NULL)
				goto wrapup;
		}
		strncpy(result+l, namebegin-1, nameend - namebegin+2);
		l += nameend - namebegin+2;
		result[l] = 0;

		skipblancs;
		if (*j == '}')
			break;
		if (*j != ',')
			msg = createException(MAL, "json.keys", "',' expected");
	}
	result[l]=']';
	result[l+1]= 0;
	l=1;
	*ret = result;
	return MAL_SUCCEED;
wrapup:
	GDKfree(result);
	return msg;
}

static str
JSONarrayvalues(int *ret, BAT *bn, char *j)
{
	char *valuebegin, *valueend, *msg = MAL_SUCCEED;
	char *result = NULL;
	size_t l, lim;

	skipblancs;
	if (*j != '[')
		throw(MAL, "json.value", "JSON object expected");

	result = (char *) GDKmalloc(BUFSIZ);
	if (result == 0)
		throw(MAL, "json.value", MAL_MALLOC_FAIL);
	lim = BUFSIZ;

	for (j++; *j && *j != ']'; j++) {
		skipblancs;
		if (*j == ']') {
			break;
		}
		valuebegin = j;
		msg = JSONvalueParser(j, &j);
		if (msg)
			goto wrapup;
		valueend = j;

		// test for candidate member
		l = valueend - valuebegin;
		if (l + 2 > lim)
			result = GDKrealloc(result, lim += BUFSIZ);
		strncpy(result, valuebegin, l);
		result[l] = 0;
		if (strcmp("null", result) == 0)
			BUNappend(bn, str_nil, FALSE);
		else
			BUNappend(bn, result, FALSE);
		skipblancs;
		if (*j == ']')
			break;
		if (*j != ',')
			msg = createException(MAL, "json.value", "',' expected");
	}
      wrapup:;
	BBPkeepref(*ret = bn->batCacheid);
	GDKfree(result);
	return MAL_SUCCEED;
}

str
JSONvalues(int *ret, json *js)
{
	BAT *bn;
	char *valuebegin, *valueend;
	char *msg = MAL_SUCCEED;
	char *result = NULL;
	size_t l, lim;
	char *j = *js;

	bn = BATnew(TYPE_void, TYPE_str, 64);
	BATseqbase(bn, 0);
	bn->hsorted = 1;
	bn->hrevsorted = 0;
	bn->H->nonil = 1;
	bn->tsorted = 1;
	bn->trevsorted = 0;
	bn->T->nonil = 1;

	skipblancs;
	if (*j == '[')
		return JSONarrayvalues(ret, bn, j);
	if (*j != '{')
		throw(MAL, "json.filter", "JSON object expected");

	// the result is an array of values
	result = (char *) GDKmalloc(BUFSIZ);
	if (result == 0)
		throw(MAL, "json.values", MAL_MALLOC_FAIL);
	lim = BUFSIZ;

	for (j++; *j && *j != '}'; j++) {
		skipblancs;
		if (*j == '}')
			break;
		if (*j != '"') {
			msg = createException(MAL, "json.values", "Name expected");
			goto wrapup;
		}
		msg = JSONstringParser(j + 1, &j);
		if (msg)
			goto wrapup;
		skipblancs;
		if (*j != ':') {
			msg = createException(MAL, "json.values", "Value expected");
			goto wrapup;
		}
		j++;
		skipblancs;
		valuebegin = j;
		msg = JSONvalueParser(j, &j);
		if (msg)
			goto wrapup;
		valueend = j;
		l = valueend - valuebegin;
		if (l + 2 > lim)
			result = GDKrealloc(result, lim += BUFSIZ);
		strncpy(result, valuebegin, valueend - valuebegin);
		result[l] = 0;
		if (strcmp("null", result) == 0)
			BUNappend(bn, str_nil, FALSE);
		else
			BUNappend(bn, result, FALSE);
		skipblancs;
		if (*j == '}')
			break;
		if (*j != ',')
			msg = createException(MAL, "json.values", "',' expected");
	}
      wrapup:;
	BBPkeepref(*ret = bn->batCacheid);
	GDKfree(result);
	return msg;
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
		tpe = getTailType(getArgType(mb, pci, i + 1));
		ATOMformat(tpe, p, &val);
		if (strncmp(val, "nil", 3) == 0)
			strcpy(val, "null");
		l = strlen(name) + strlen(val);
		if (l > lim - len)
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
	str *ret;
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
		if (l + 2 > lim - len)
			row = (char *) GDKrealloc(row, lim = cnt * l <= lim ? cnt * l : lim + BUFSIZ);
		strncpy(result + len, row, l + 1);
		GDKfree(row);
		len += l;
		result[len++] = ',';
		result[len] = 0;
	}
	result[len - 1] = ']';
	ret = (str *) getArgReference(stk, pci, 0);
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
		tpe = getTailType(getArgType(mb, pci, i));
		ATOMformat(tpe, p, &val);
		if (strncmp(val, "nil", 3) == 0)
			strcpy(val, "null");
		l = strlen(val);
		if (l > lim - len)
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
		if (l + 2 > lim - len)
			row = (char *) GDKrealloc(row, lim = cnt * l <= lim ? cnt * l : lim + BUFSIZ);
		strncpy(result + len, row, l + 1);
		GDKfree(row);
		len += l;
		result[len++] = ',';
		result[len] = 0;
	}
	result[len - 1] = ']';
	ret = (str *) getArgReference(stk, pci, 0);
	*ret = result;
	return MAL_SUCCEED;
}

static str
JSONnestKeyValue(str *ret, int *id, int *key, int *values)
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
		if (bk == NULL)
			throw(MAL, "json.nest", RUNTIME_OBJECT_MISSING);
	}

	bv = BATdescriptor(*values);
	if (bv == NULL) {
		if (bk)
			BBPreleaseref(bk->batCacheid);
		throw(MAL, "json.nest", RUNTIME_OBJECT_MISSING);
	}
	tpe = bv->ttype;
	cnt = BATcount(bv);

	if (id) {
		bo = BATdescriptor(*id);
		if (bo == NULL) {
			if (bk)
				BBPreleaseref(bk->batCacheid);
			BBPreleaseref(bv->batCacheid);
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

	if (cnt && bk) {
		row[1] = '{';
		row[2] = 0;
		len = 2;
	}

	for (i = 0; i < cnt; i++) {
		if (id &&bk) {
			p = BUNtail(boi, BUNfirst(bo) + i);
			if (*(oid *) p != o) {
				snprintf(row + len - 1, lim - len, "},{");
				len += 2;
				o = *(oid *) p;
			}
		}

		if (bk) {
			nme = (str) BUNtail(bki, BUNfirst(bk) + i);
			l = strlen(nme);
			if (l + 3 > lim - len)
				row = (char *) GDKrealloc(row, lim = (lim / (i + 1)) * cnt + BUFSIZ + l + 3);
			snprintf(row + len, lim - len, "\"%s\":", nme);
			len += l + 3;
		}

		bvi = bat_iterator(bv);
		p = BUNtail(bvi, BUNfirst(bv) + i);
		ATOMformat(tpe, p, &val);
		if (strncmp(val, "nil", 3) == 0)
			strcpy(val, "null");
		l = strlen(val);
		if (l > lim - len)
			row = (char *) GDKrealloc(row, lim = (lim / (i + 1)) * cnt + BUFSIZ + l + 3);
		if (tpe == TYPE_json && *val == '"') {
			l -= 5;
			strncpy(row + len, val + 2, l);	// remove the json brackets
			row[len + l++] = '"';
			row[len + l] = 0;
			len += l;
			row[len++] = ',';
			row[len] = 0;
		} else {
			strncpy(row + len, val, l);
			len += l;
			row[len++] = ',';
			row[len] = 0;
		}
	}
	if (row[1]) {
		if (bk) {
			row[len - 1] = '}';
			row[len] = ']';
			row[len + 1] = 0;
		} else {
			row[len - 1] = ']';
			row[len] = 0;
		}
	} else {
		row[1] = ']';
		row[2] = 0;
	}
	GDKfree(val);
	if (bo)
		BBPreleaseref(bo->batCacheid);
	if (bk)
		BBPreleaseref(bk->batCacheid);
	BBPreleaseref(bv->batCacheid);
	*ret = row;
	return MAL_SUCCEED;
}

str
JSONnest(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *id = 0, *key = 0, *val = 0;
	str *ret;

	(void) cntxt;
	(void) mb;

	if (pci->argc - pci->retc == 1) {
		val = (int *) getArgReference(stk, pci, 1);
	} else if (pci->argc - pci->retc == 2) {
		id = 0;
		key = (int *) getArgReference(stk, pci, 1);
		val = (int *) getArgReference(stk, pci, 2);
	} else if (pci->argc - pci->retc == 3) {
		id = (int *) getArgReference(stk, pci, 1);
		key = (int *) getArgReference(stk, pci, 2);
		val = (int *) getArgReference(stk, pci, 3);
	}
	ret = (str *) getArgReference(stk, pci, 0);
	return JSONnestKeyValue(ret, id, key, val);
}


/*
 * The JSON path expression is purposely kept simple,
 * Only portions are extracted, leaving their analysis
 * to the SQL runtime environment.
 * A pseudo type component can be appended to a value
 * to force conversion, returning null for all missed ones.
 * samples:
 * .store.book
 * .store.book[0]
 * .store.book.author
 * ..author
 * The path syntax is given by
 * .id[index] or ..id[index]
 * where id may be a wildcard *
 */
#define MAXTERMS 256
#define DONE 0
#define CHILD 1
#define INDEX 2
#define CHILDANY 3

typedef struct {
	int token;
	char *name;
	int index;
} pathterm;

static str
JSONpathValidation(char *expr, pathterm terms[])
{
	int t = 0;
	char *s, *beg;

	terms[0].token = CHILD;
	for (s = expr; *s; s++) {
		terms[t].token = CHILD;
		terms[t].index = INT_MIN;
		if (*s == '.' && *(s + 1) == '.') {
			terms[t].token = CHILDANY;
			s += 2;
		} else if (*s == '.')
			s++;

		if (*s != '[') {
			for (beg = s; *s; s++)
				if (*s == '.' || *s == '[')
					break;
			terms[t].name = GDKzalloc(s - beg + 1);
			strncpy(terms[t].name, beg, s - beg);
			if (*s == 0)
				break;
		}
		if (*s == '[') {
			terms[t].index = atoi(s + 1);
			terms[t].token = INDEX;
			for (s++; *s; s++)
				if (*s == ']')
					break;
			if (*s == 0)
				break;
			if (*s != ']')
				throw(MAL, "json.path", "] expected");
		}
		if (++t == MAXTERMS)
			throw(MAL, "json.path", "too many terms");
	}
	return MAL_SUCCEED;
}

static void
JSONtextInternal(char *result, char *valuebegin, char *valueend)
{
	char *s;
	for ( s= valuebegin; *s && s != valueend; s++){
		if ( *s =='[' || *s ==']' ||  *s == '{' || *s == '}' || *s == ',')
			continue;
		if ( *s == '"'){
			for(s++; *s && *s != '"'; s++)
				if ( *s == '\\')
					switch (*++s){
					case '\\':
						*result++ = '\\';break;
					case 'r':
						*result++ = '\r';break;
					case 'f':
						*result++ = '\f';break;
					case 't':
						*result++ = '\t';break;
					case 'n':
						*result++ = '\n';break;
					default:
						*result++ = *s;
					}
				else
					*result++ = *s;
		} else 
			*result++ = *s;
	} 
	*result = 0;
}

static str
JSONpathInternal(json *ret, json *js, str *expr,int flag)
{
	pathterm terms[MAXTERMS];
	str j, msg = MAL_SUCCEED;
	char *namebegin, *nameend;
	char *valuebegin, *valueend;
	json result = 0, old = 0;
	int copying = -1, t = 0, idx = 0;
	size_t lim, l, len = 0;

	memset((char *) terms, 0, MAXTERMS * sizeof(pathterm));
	msg = JSONpathValidation(*expr, terms);
	if (msg)
		return msg;

	j = *js;
	for (t = 0; terms[t].token != 0; t++) {
		result = (char *) GDKmalloc(BUFSIZ);
		if (result == NULL)
			throw(MAL, "json.path", MAL_MALLOC_FAIL);
		if ( !flag){
			result[0] = '[';
			result[1] = 0;
			len = 1;
		} else {
			result[0] = 0;
			len = 0;
		}
		lim = BUFSIZ;
		idx = INT_MIN;

		skipblancs;
		for (j++; *j; j++) {
			skipblancs;
			if (terms[t].token == CHILDANY) {
				if (*j == '[')
					continue;
			}
			if (*j == '}' || *j == ']' || *j == ',')
				continue;
			if (*j == '{')
				j++;
			skipblancs;
			if (*j == 0)
				break;
			namebegin = j + 1;
			msg = JSONstringParser(j + 1, &j);
			if (msg) {	// json string should be correct, name may be missing
				GDKfree(msg);
				msg = MAL_SUCCEED;
			}
			nameend = j - 1;
			l = nameend - namebegin;

			skipblancs;
			if (*j == ':') {
				j++;
				skipblancs;

				switch (terms[t].token) {
				case INDEX:
					copying = (terms[t].name && l > 0 && strncmp(terms[t].name, namebegin, l) == 0) || terms[t].name == 0 || terms[t].name[0] == '*';
					if (copying) {
						if (idx == INT_MIN)
							idx = terms[t].index;
						else
							idx--;
					}
					if (idx != 0)
						copying = 0;
					break;
				case CHILD:
					copying = terms[t].name && ((l > 0 && strncmp(terms[t].name, namebegin, l) == 0) || terms[t].name[0] == '*');
					break;
				case CHILDANY:
					copying = terms[t].name && ((l > 0 && strncmp(terms[t].name, namebegin, l) == 0) || terms[t].name[0] == '*');
					if (copying == 0) {
						for (; *j; j++)
							if (*j == '{' || *j == ',' || *j == ']')
								break;
						j--;
						continue;
					}
					break;
				default:
					msg = createException(MAL, "json.path", "Unexpected token");
					goto wrapup;
				}
				valuebegin = j;
				msg = JSONvalueParser(j, &j);
				if (msg)
					goto wrapup;
				if (copying == 1) {
					valueend = j;
					l = valueend - valuebegin;
					if (len + l + 3 > lim)
						result = GDKrealloc(result, lim += BUFSIZ);
					if ( !flag ){
						strncpy(result + len, valuebegin, valueend - valuebegin);
						len += l;
						strncpy(result + len, ",", 2);
						len++;
					} else {
						JSONtextInternal(result + len, valuebegin, valueend);
						len += strlen(result+len);
					}
				}
				skipblancs;
			}
		}
		if (result[1]){
			if ( !flag )
				result[len - 1] = ']';
			else result[len] = 0;
		}else {
			if ( flag )
				result[0] = 0;
			else {
				result[1] = ']';
				result[2] = 0;
			}
		}
		if (old)
			GDKfree(old);
		old = result;
		j = result;
	}
      wrapup:;
	for (t = 0; terms[t].token; t++)
		if (terms[t].name)
			GDKfree(terms[t].name);
	*ret = result;
	return msg;
}
str
JSONpath(json *ret, json *js, str *expr)
{
	return JSONpathInternal(ret,js,expr,FALSE);
}
str
JSONtext(json *ret, json *js, str *expr)
{
	return JSONpathInternal(ret,js,expr,TRUE);
}
