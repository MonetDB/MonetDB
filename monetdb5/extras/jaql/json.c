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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * JSON is a lightweight data-interchange format.  It is easy for humans
 * to read and write, and it is easy for machines to parse and generate.
 * It is /the/ data format for Web-2.0 applications.
 */

#include "monetdb_config.h"
#include "json.h"
#include "gdk.h"
#include "mal.h"
#include "mal_client.h"
#include "mal_exception.h"
#include "stream.h"


typedef struct _json_bats {
	BAT *kind;    /* all entities */
	BAT *string;  /* string values */
	BAT *integer; /* integer values */
	BAT *doble;   /* double values */
	BAT *array;   /* array elements */
	BAT *object;  /* object members */
	BAT *name;    /* pair names */
	char *error;  /* set to non-NULL (an explanatory string) on failure */
	stream *is;   /* set to non-NULL when reading directly from stream */
	char *streambuf; /* buffer used for reading from the stream */
	size_t streambuflen;
} jsonbat;

static char *parse_json_value(jsonbat *jb, oid *v, char *p);
static char *parse_json_object(jsonbat *jb, oid *id, char *p);
static char *parse_json_array(jsonbat *jb, oid *id, char *p);

static void json_error(jsonbat *, char *,
	_In_z_ _Printf_format_string_ const char *, ...)
	__attribute__((__format__(__printf__, 3, 4)));
static void
json_error(jsonbat *jb, char *n, const char *format, ...)
{
	va_list ap;
	char message[8096];
	size_t len;
	char around[32];
	char *p, *q, *start = around;
	char hadend = 0;

	va_start(ap, format);
	len = vsnprintf(message, sizeof(message), format, ap);
	va_end(ap);

	p = n;
	q = around + 13 + 1;
	*q = '\0';
	for (; p >= jb->streambuf && q > around; p--) {
		if (isspace(*p)) {
			if (*q != ' ')
				*--q = ' ';
		} else if (*p == '\0') {
			/* artifact from parse_json_string */
			*--q = '"';
		} else {
			*--q = *p;
		}
	}
	start = q;
	p = n + 1;
	q = around + 13;
	for (; *p != '\0' && q - around < (ssize_t)sizeof(around) - 1; p++) {
		if (isspace(*p)) {
			if (*q != ' ')
				*++q = ' ';
		} else if (*p == '\0') {
			/* artifact from parse_json_string */
			*++q = '"';
		} else {
			*++q = *p;
		}
	}
	*++q = '\0';
	if (q - around < (ssize_t)sizeof(around))
		hadend = 1;

	snprintf(message + len, sizeof(message) - len, " at or around '%s%s%s'",
			start == around ? "..." : "", start, hadend == 0 ? "..." : "");

	if (jb->error != NULL)
		GDKfree(jb->error);
	jb->error = GDKstrdup(message);
}

static size_t
read_from_stream(jsonbat *jb, char **pos, char **start, char **recall)
{
	size_t shift = 0;
	ssize_t sret = 0;
	
	assert(*start - jb->streambuf >= 0);
	shift = *start - jb->streambuf;

	if (shift > 0) {
		memmove(jb->streambuf, *start, jb->streambuflen - shift);
		if (pos != start)
			*pos -= shift;
		*start -= shift;
		if (recall != NULL)
			*recall -= shift;
	}

	shift = *pos - jb->streambuf;
	if (*pos == jb->streambuf + jb->streambuflen - 1) {
		size_t rshift = recall != NULL ? *recall - jb->streambuf : 0;
		char *newbuf = realloc(jb->streambuf, jb->streambuflen += 8096);
		if (newbuf == NULL)
			return 0;
		jb->streambuf = newbuf;
		*pos = jb->streambuf + shift;
		if (pos != start)
			*start = jb->streambuf;
		if (recall != NULL)
			*recall = jb->streambuf + rshift;
	}

	sret = mnstr_read(jb->is, *pos, 1, jb->streambuflen - shift - 1);
	if (sret <= 0)
		return 0;
	jb->streambuf[shift + sret] = '\0';
	assert(**pos != '\0');

	return (size_t) sret;
}

static char *
parse_json_string(jsonbat *jb, oid *v, char pair, char *p, char **recall)
{
	char escape = 0;
	char *r = p;
	char *n = p;
	char *w = p;

	for (; ; p++, w++) {
		if (*p == '\0' &&
				(jb->is == NULL || read_from_stream(jb, &p, &n, &w) == 0))
		{
			json_error(jb, p, "unexpected end of stream while reading string");
			return NULL;
		}
		switch (*p) {
			case '\\':
				if (escape) {
					*w = '\\';
				} else if (w != p) {
					w--;
				}
				escape = !escape;
				break;
			case 'b':
				if (escape) {
					*w = '\b';
				} else if (w != p) {
					*w = *p;
				}
				escape = 0;
				break;
			case 'f':
				if (escape) {
					*w = '\f';
				} else if (w != p) {
					*w = *p;
				}
				escape = 0;
				break;
			case 'n':
				if (escape) {
					*w = '\n';
				} else if (w != p) {
					*w = *p;
				}
				escape = 0;
				break;
			case 'r':
				if (escape) {
					*w = '\r';
				} else if (w != p) {
					*w = *p;
				}
				escape = 0;
				break;
			case 't':
				if (escape) {
					*w = '\t';
				} else if (w != p) {
					*w = *p;
				}
				escape = 0;
				break;
			case '"':
				if (escape) {
					*w = '"';
				} else {
					*w = '\0';
				}
				escape = 0;
				break;
			/* TODO: unicode escapes \u0xxxx */
			default:
				if (w != p)
					*w = *p;
				escape = 0;
				break;
		}
		if (*w == '\0')
			break;
	}
	if (pair == 0) {
		BUNappend(jb->kind, "s", FALSE);
		*v = BUNlast(jb->kind) - 1;
		BUNins(jb->string, v, n, FALSE);
	}
	if (recall != NULL)
		*recall = *recall + (n - r);
	return p + 1;
}

static char *
parse_json_number(jsonbat *jb, oid *v, char *p)
{
	char *n = p;
	char x;
	char isDouble = 0;

	/* skip leading -, so we can easily find integers */
	if (*p == '-')
		p++;

	for (; ; p++) {
		if (*p == '\0' &&
				(jb->is == NULL || read_from_stream(jb, &p, &n, NULL) == 0))
			break;
		if (strchr("0123456789", *p) != NULL) {
			/* valid for ints */
		} else  if (strchr("eE.-+", *p) != NULL) {
			/* valid for doubles */
			isDouble = 1;
		} else {
			break;
		}
	}
	x = *p;
	*p = '\0';
	if (isDouble == 1) {
		double d = atof(n);
		BUNappend(jb->kind, "d", FALSE);
		*v = BUNlast(jb->kind) - 1;
		BUNins(jb->doble, v, &d, FALSE);
	} else {
		long long int i = strtoll(n, NULL, 10);
		BUNappend(jb->kind, "i", FALSE);
		*v = BUNlast(jb->kind) - 1;
		BUNins(jb->integer, v, &i, FALSE);
	}
	*p = x;
	return p;
}

static char *
parse_json_truefalsenull(jsonbat *jb, oid *v, char *p)
{
	char *n = p;
	char *which = *p == 't' ? "true" : *p == 'f' ? "false" : "null";
	char *whichp = which;

	for (; *whichp != '\0'; p++, whichp++) {
		if (*p == '\0' &&
				(jb->is == NULL || read_from_stream(jb, &p, &n, NULL) == 0))
			break;
		if (*p != *whichp)
			break;
	}
	if (*whichp != '\0') {
		json_error(jb, n, "expected '%s'", which);
		return NULL;
	}

	if (*n == 't') {
		BUNappend(jb->kind, "t", FALSE);
		*v = BUNlast(jb->kind) - 1;
	} else if (*n == 'f') {
		BUNappend(jb->kind, "f", FALSE);
		*v = BUNlast(jb->kind) - 1;
	} else {
		BUNappend(jb->kind, "n", FALSE);
		*v = BUNlast(jb->kind) - 1;
	}

	return p;
}

static char *
parse_json_value(jsonbat *jb, oid *v, char *p)
{
	switch (*p) {
		case '"':
			p = parse_json_string(jb, v, 0, p + 1, NULL);
			break;
		case '-':
		case '0':
		case '1':
		case '2':
		case '3':
		case '4':
		case '5':
		case '6':
		case '7':
		case '8':
		case '9':
			p = parse_json_number(jb, v, p);
			break;
		case '{':
			p = parse_json_object(jb, v, p + 1);
			break;
		case '[':
			p = parse_json_array(jb, v, p + 1);
			break;
		case 't':
		case 'f':
		case 'n':
			p = parse_json_truefalsenull(jb, v, p);
			break;
		default:
			json_error(jb, p, "unexpected character '%c' for value", *p);
			return NULL;
	}
	return p;
}

static char *
parse_json_pair(jsonbat *jb, oid *v, char *p)
{
	oid n;
	char *x;

	if (*p != '"') {
		json_error(jb, p, "expected string for pair");
		return NULL;
	}

	x = p + 1;
	if ((p = parse_json_string(jb, NULL, 1, x, &x)) == NULL)
		return NULL;
	/* the pair name string is not kept/retained, because it can
	 * potentially be the key for the rest of the entire document
	 * to avoid that we have to copy it here, we insert it with a
	 * slight hack, anticipating on what parse_json_value will do */
	n = BUNlast(jb->kind);
	BUNins(jb->name, &n, x, FALSE);

	for (; ; p++) {
		if (*p == '\0' &&
				(jb->is == NULL || read_from_stream(jb, &p, &p, NULL) == 0))
			break;
		if (!isspace(*p))
			break;
	}
	if (*p != ':') {
		json_error(jb, p, "exected ':' for pair");
		return NULL;
	}
	p++;
	for (; ; p++) {
		if (*p == '\0' &&
				(jb->is == NULL || read_from_stream(jb, &p, &p, NULL) == 0))
			break;
		if (!isspace(*p))
			break;
	}

	if ((p = parse_json_value(jb, v, p)) == NULL)
		return NULL;

	return p;
}

static char *
parse_json_object(jsonbat *jb, oid *id, char *p)
{
	oid v = (oid)0;

	BUNappend(jb->kind, "o", FALSE);
	*id = BUNlast(jb->kind) - 1;

	for (; ; p++) {
		if (*p == '\0' &&
				(jb->is == NULL || read_from_stream(jb, &p, &p, NULL) == 0))
			break;
		if (isspace(*p))
			continue;
		switch (*p) {
			case ',':
				/* next value */
				break;
			case '}':
				/* end of object */
				return p + 1;
			default:
				/* expect pair */
				if ((p = parse_json_pair(jb, &v, p)) == NULL)
					return NULL;
				
				BUNins(jb->object, id, &v, FALSE);
				p--;
				break;
		}
	}

	return p;
}

static char *
parse_json_array(jsonbat *jb, oid *id, char *p)
{
	oid v;

	BUNappend(jb->kind, "a", FALSE);
	*id = BUNlast(jb->kind) - 1;

	for (; ; p++) {
		if (*p == '\0' &&
				(jb->is == NULL || read_from_stream(jb, &p, &p, NULL) == 0))
			break;
		if (isspace(*p))
			continue;
		switch (*p) {
			case ']':
				/* end of array */
				return p + 1;
			case ',':
				/* next value */
				break;
			default:
				/* expect value */
				if ((p = parse_json_value(jb, &v, p)) == NULL)
					return NULL;
				
				BUNins(jb->array, id, &v, FALSE);
				p--;
				break;
		}
	}

	return p;
}

#define loadbat(name) \
	jb.name = BATdescriptor(ABS(*name)); \
	if (*name < 0) \
		jb.name = BATmirror(jb.name);
#define loadbats() \
	loadbat(kind); \
	loadbat(string); \
	loadbat(integer); \
	loadbat(doble); \
	loadbat(array); \
	loadbat(object); \
	loadbat(name);

#define unloadbat(name) \
	BBPunfix(jb.name->batCacheid);
#define unloadbats() \
	unloadbat(kind); \
	unloadbat(string); \
	unloadbat(integer); \
	unloadbat(doble); \
	unloadbat(array); \
	unloadbat(object); \
	unloadbat(name);

static str
shred_json(jsonbat *jb, int *kind, int *string, int *integer, int *doble, int *array, int *object, int *name, str *json)
{
	char *p = NULL;
	oid v = (oid)0; 

	/* initialise all bats */
	jb->kind = BATnew(TYPE_void, TYPE_bte, BATTINY);
	jb->kind = BATseqbase(jb->kind, (oid)0);
	jb->string = BATnew(TYPE_oid, TYPE_str, BATTINY);
	jb->doble = BATnew(TYPE_oid, TYPE_dbl, BATTINY);
	jb->integer = BATnew(TYPE_oid, TYPE_lng, BATTINY);
	jb->name = BATnew(TYPE_oid, TYPE_str, BATTINY);
	jb->object = BATnew(TYPE_oid, TYPE_oid, BATTINY);
	jb->array = BATnew(TYPE_oid, TYPE_oid, BATTINY);

	if (json == NULL) {
		p = jb->streambuf;
	} else {
		p = *json;
	}

	for (; ; p++) {
		if (*p == '\0' &&
				(jb->is == NULL || read_from_stream(jb, &p, &p, NULL) == 0))
			break;
		if (!isspace(*p))
			break;
	}
	if (*p == '\0') {
		jb->error = GDKstrdup("expected data");
		p = NULL;
	} else {
		p = parse_json_value(jb, &v, p);
	}

	for (; p != NULL; p++) {
		if (*p == '\0' &&
				(jb->is == NULL || read_from_stream(jb, &p, &p, NULL) == 0))
			break;
		if (!isspace(*p))
			break;
	}
	if (p == NULL || *p != '\0') {
		str e;
		if (p == NULL) {
			/* parsing failed */
			e = createException(MAL, "json.shred", "%s", jb->error);
		} else {
			e = createException(MAL, "json.shred", "invalid JSON data, "
					"trailing characters: %s", p);
		}
		BBPunfix(jb->kind->batCacheid);
		BBPunfix(jb->string->batCacheid);
		BBPunfix(jb->integer->batCacheid);
		BBPunfix(jb->doble->batCacheid);
		BBPunfix(jb->array->batCacheid);
		BBPunfix(jb->object->batCacheid);
		BBPunfix(jb->name->batCacheid);
		GDKfree(jb->error);
		return e;
	}
	BBPkeepref(jb->kind->batCacheid);
	*kind = jb->kind->batCacheid;
	BBPkeepref(jb->string->batCacheid);
	*string = jb->string->batCacheid;
	BBPkeepref(jb->integer->batCacheid);
	*integer = jb->integer->batCacheid;
	BBPkeepref(jb->doble->batCacheid);
	*doble = jb->doble->batCacheid;
	BBPkeepref(jb->array->batCacheid);
	*array = jb->array->batCacheid;
	BBPkeepref(jb->object->batCacheid);
	*object = jb->object->batCacheid;
	BBPkeepref(jb->name->batCacheid);
	*name = jb->name->batCacheid;
	return MAL_SUCCEED;
}

str
JSONshred(int *kind, int *string, int *integer, int *doble, int *array, int *object, int *name, str *json)
{
	jsonbat jb;

	memset(&jb, 0, sizeof(jsonbat));

	return shred_json(&jb, kind, string, integer, doble, array, object, name, json);
}

str
JSONshredstream(int *kind, int *string, int *integer, int *doble, int *array, int *object, int *name, str *uri)
{
	jsonbat jb;
	str ret;

	memset(&jb, 0, sizeof(jsonbat));
	
	if ((jb.is = open_urlstream(*uri)) == NULL)
		throw(MAL, "json.shreduri", "failed to open urlstream");

	if (mnstr_errnr(jb.is) != 0) {
		str err = createException(MAL, "json.shreduri",
				"opening stream failed: %s", mnstr_error(jb.is));
		mnstr_destroy(jb.is);
		return err;
	}

	jb.streambuflen = 8096;
	jb.streambuf = malloc(jb.streambuflen);
	jb.streambuf[0] = '\0';

	ret = shred_json(&jb, kind, string, integer, doble, array, object, name, NULL);

	free(jb.streambuf);

	return ret;
}

static size_t
strlen_json_value(jsonbat *jb, oid id)
{
	BATiter bi;
	oid v;
	size_t ret = 0;

	bi = bat_iterator(jb->kind);

	BUNfndOID(v, bi, &id);
	switch (*(char *)BUNtail(bi, v)) {
		case 'i':
			bi = bat_iterator(jb->integer);
			BUNfndOID(v, bi, &id);
			ret += snprintf(NULL, 0, "%lld", *(lng *)BUNtail(bi, v));
			break;
		case 'd':
			bi = bat_iterator(jb->doble);
			BUNfndOID(v, bi, &id);
			ret += snprintf(NULL, 0, "%f", *(dbl *)BUNtail(bi, v));
			break;
		case 's':
			bi = bat_iterator(jb->string);
			BUNfndOID(v, bi, &id);
			ret += 2 + strlen(BUNtail(bi, v));
			break;
		case 't':
			ret += 4;
			break;
		case 'f':
			ret += 5;
			break;
		case 'n':
			ret += 4;
			break;
		case 'a':
		{
			BAT *elems;
			BUN p, q;
			elems = BATmirror(BATselect(BATmirror(jb->array), &id, &id));
			bi = bat_iterator(elems);
			ret += 2;
			BATloop (elems, p, q) {
				ret += strlen_json_value(jb, *(oid *)BUNtail(bi, p));
				if (p < q - 1)
					ret += 2;
			}
			ret += 2;
			BBPunfix(elems->batCacheid);
			break;
		}
		case 'o':
		{
			BATiter ni;
			BAT *objects = BATmirror(BATselect(BATmirror(jb->object), &id, &id));
			BUN p, q;
			oid n;
			ret += 2;
			bi = bat_iterator(objects);
			ni = bat_iterator(jb->name);
			BATloop (objects, p, q) {
				BUNfndOID(n, ni, BUNtail(bi, p));
				ret += 2 + strlen(BUNtail(ni, n)) + 2;
				ret += strlen_json_value(jb, *(oid *)BUNtail(bi, p));
				if (p < q - 1)
					ret += 2;
			}
			ret += 2;
			BBPunfix(objects->batCacheid);
			break;
		}
	}

	return ret;
}

static void
print_json_value(jsonbat *jb, stream *s, oid id, int indent)
{
	BATiter bi;
	oid v;
	bi = bat_iterator(jb->kind);

	BUNfndOID(v, bi, &id);
	switch (*(char *)BUNtail(bi, v)) {
		case 'i':
			bi = bat_iterator(jb->integer);
			BUNfndOID(v, bi, &id);
			mnstr_printf(s, "%*s%lld",
					indent >= 0 ? indent : 0, "",
					*(lng *)BUNtail(bi, v));
			break;
		case 'd':
			bi = bat_iterator(jb->doble);
			BUNfndOID(v, bi, &id);
			mnstr_printf(s, "%*s%f",
					indent >= 0 ? indent : 0, "",
					*(dbl *)BUNtail(bi, v));
			break;
		case 's':
			bi = bat_iterator(jb->string);
			BUNfndOID(v, bi, &id);
			mnstr_printf(s, "%*s\"%s\"",
					indent >= 0 ? indent : 0, "",
					BUNtail(bi, v));
			break;
		case 't':
			mnstr_printf(s, "%*strue", indent >= 0 ? indent : 0, "");
			break;
		case 'f':
			mnstr_printf(s, "%*sfalse", indent >= 0 ? indent : 0, "");
			break;
		case 'n':
			mnstr_printf(s, "%*snull", indent >= 0 ? indent : 0, "");
			break;
		case 'a':
		{
			BUN p, q;
			char first = 1;
			if (indent >= 0) {
				mnstr_printf(s, "%*s[\n", indent, "");
			} else {
				mnstr_printf(s, "[ ");
			}
			bi = bat_iterator(jb->array);
			BATloop (jb->array, p, q) {
				if (*(oid *)BUNhead(bi, p) != id)
					continue;
				if (!first)
					mnstr_printf(s, ",%c", indent >= 0 ? '\n' : ' ');
				print_json_value(jb, s, *(oid *)BUNtail(bi, p),
						indent >= 0 ? indent + 2 : indent);
				first = 0;
			}
			mnstr_printf(s, "%c%*s]", indent >= 0 ? '\n' : ' ',
					indent >= 0 ? indent : 0, "");
			break;
		}
		case 'o':
		{
			BATiter oi, ni;
			BUN p, q;
			oid n;
			char first = 1;
			if (indent >= 0) {
				mnstr_printf(s, "%*s{\n", indent, "");
			} else {
				mnstr_printf(s, "{ ");
			}
			oi = bat_iterator(jb->object);
			ni = bat_iterator(jb->name);
			BATloop (jb->object, p, q) {
				if (*(oid *)BUNhead(oi, p) != id)
					continue;
				if (!first)
					mnstr_printf(s, ",%c", indent >= 0 ? '\n' : ' ');
				BUNfndOID(n, ni, BUNtail(oi, p));
				if (indent >= 0) {
					mnstr_printf(s, "%*s\"%s\":",
							indent + 2, "", BUNtail(ni, n));
					BUNfndOID(v, bi, BUNtail(oi, p));
					switch (*(char *)BUNtail(bi, v)) {
						case 'o':
						case 'a':
							mnstr_printf(s, "\n");
							print_json_value(jb, s, *(oid *)BUNtail(oi, p),
									indent + 2);
							break;
						default:
							mnstr_printf(s, " ");
							print_json_value(jb, s, *(oid *)BUNtail(oi, p), -1);
							break;
					}
				} else {
					mnstr_printf(s, "\"%s\": ", BUNtail(ni, n));
					print_json_value(jb, s, *(oid *)BUNtail(oi, p), indent);
				}
				first = 0;
			}
			mnstr_printf(s, "%c%*s}", indent >= 0 ? '\n' : ' ',
					indent >= 0 ? indent : 0, "");
			break;
		}
	}
}

str
JSONprint(int *ret, stream **s, int *kind, int *string, int *integer, int *doble, int *array, int *object, int *name, oid *start, bit *pretty)
{
	jsonbat jb;
	BATiter bi;
	BUN startoid;

	loadbats();

	bi = bat_iterator(jb.kind);
	BUNfndOID(startoid, bi, start);
	if (startoid == BUN_NONE)
		throw(ILLARG, "json.print", "start must be a valid oid from kind");

	if (*pretty == TRUE && *(char *)BUNtail(bi, startoid) == 'a') {
		BAT *elems;
		BUN p, q;
		size_t esize = 0, fsize = 0;
		int indent;
		char first = 1;
		oid id = *(oid *)BUNhead(bi, startoid);

		/* look into the first array's members to see if breaking up is
		 * going to be necessary */
		elems = BATmirror(BATselect(BATmirror(jb.array), &id, NULL));
		bi = bat_iterator(elems);
		fsize = 4;
		BATloop (elems, p, q) {
			esize = strlen_json_value(&jb, *(oid *)BUNtail(bi, p));
			if (esize > 80)
				break;
			fsize += esize;
			if (p < q - 1)
				fsize += 2;
			if (fsize > 80)
				break;
		}
		if (esize > 80 || fsize > 80) {
			mnstr_printf(*s, "[\n");
			indent = 2;
		} else {
			mnstr_printf(*s, "[ ");
			indent = -1;
		}
		BBPunfix(elems->batCacheid);

		/* now print it, by manually doing the outer loop to allow only
		 * the inners not being broken up */
		bi = bat_iterator(jb.array);
		BATloop (jb.array, p, q) {
			if (*(oid *)BUNhead(bi, p) != id)
				continue;
			if (!first)
				mnstr_printf(*s, ",%c", indent >= 0 ? '\n' : ' ');
			if (indent >= 0 && esize <= 80) {
				mnstr_printf(*s, "%*s", indent, "");
				print_json_value(&jb, *s, *(oid *)BUNtail(bi, p), -1);
			} else {
				print_json_value(&jb, *s, *(oid *)BUNtail(bi, p), indent);
			}
			first = 0;
		}
		mnstr_printf(*s, "%c]\n", indent >= 0 ? '\n' : ' ');
	} else {
		print_json_value(&jb, *s, *(oid *)BUNhead(bi, startoid), -1);
		mnstr_printf(*s, "\n");
	}

	unloadbats();

	*ret = 0;
	return MAL_SUCCEED;
}

str
JSONexportResult(int *ret, stream **s, int *kind, int *string, int *integer, int *doble, int *array, int *object, int *name, oid *start)
{
	stream *f;
	buffer *bufstr = NULL;
	bit pretty = TRUE;
	size_t maxlen = 0;
	size_t rows = 0;
	str buf;
	str line, oline;

	bufstr = buffer_create(8096);
	f = buffer_wastream(bufstr, "bufstr_write");
	JSONprint(ret, &f, kind, string, integer, doble, array, object, name, start, &pretty);

	/* calculate width of column, and the number of tuples */
	buf = buffer_get_buf(bufstr);
	oline = buf;
	while ((line = strchr(oline, '\n')) != NULL) {
		if ((size_t) (line - oline) > maxlen)
			maxlen = line - oline;
		rows++;
		oline = line + 1;
	} /* last line ends with \n */

	mnstr_printf(*s, "&1 0 " SZFMT " 1 " SZFMT "\n",
			/* type id rows columns tuples */ rows, rows);
	mnstr_printf(*s, "%% .json # table_name\n");
	mnstr_printf(*s, "%% json # name\n");
	mnstr_printf(*s, "%% clob # type\n");
	mnstr_printf(*s, "%% " SZFMT " # length\n", maxlen);
	oline = buf;
	while ((line = strchr(oline, '\n')) != NULL) {
		*line++ = '\0';
		mnstr_printf(*s, "=%s\n", oline);
		oline = line;
	}
	free(buf);
	mnstr_close(f);
	mnstr_destroy(f);
	buffer_destroy(bufstr);

	return MAL_SUCCEED;
}

str
JSONstore(int *ret, str *nme, int *kind, int *string, int *integer, int *doble, int *array, int *object, int *name)
{
	char buf[256];
	int bid;
	jsonbat jb;
	bat blist[8] = { 0, 0, 0, 0, 0, 0, 0, 0 };
	int bcnt = 1;

	snprintf(buf, sizeof(buf), "json_%s_kind", *nme);
	bid = BBPindex(buf);
	if (bid)
		throw(MAL, "json.store",
				"JSON object with the same name already exists: %s", *nme);

	loadbats();

	BBPrename(jb.kind->batCacheid, buf);
	BATmode(jb.kind, PERSISTENT);
	blist[bcnt++] = ABS(jb.kind->batCacheid);
	if (jb.string != NULL) {
		snprintf(buf, sizeof(buf), "json_%s_string", *nme);
		BBPrename(jb.string->batCacheid, buf);
		BATmode(jb.string, PERSISTENT);
		blist[bcnt++] = ABS(jb.string->batCacheid);
	}
	if (jb.integer != NULL) {
		snprintf(buf, sizeof(buf), "json_%s_integer", *nme);
		BBPrename(jb.integer->batCacheid, buf);
		BATmode(jb.integer, PERSISTENT);
		blist[bcnt++] = ABS(jb.integer->batCacheid);
	}
	if (jb.doble != NULL) {
		snprintf(buf, sizeof(buf), "json_%s_doble", *nme);
		BBPrename(jb.doble->batCacheid, buf);
		BATmode(jb.doble, PERSISTENT);
		blist[bcnt++] = ABS(jb.doble->batCacheid);
	}
	if (jb.array != NULL) {
		snprintf(buf, sizeof(buf), "json_%s_array", *nme);
		BBPrename(jb.array->batCacheid, buf);
		BATmode(jb.array, PERSISTENT);
		blist[bcnt++] = ABS(jb.array->batCacheid);
	}
	if (jb.object != NULL) {
		snprintf(buf, sizeof(buf), "json_%s_object", *nme);
		BBPrename(jb.object->batCacheid, buf);
		BATmode(jb.object, PERSISTENT);
		blist[bcnt++] = ABS(jb.object->batCacheid);
	}
	if (jb.name != NULL) {
		snprintf(buf, sizeof(buf), "json_%s_name", *nme);
		BBPrename(jb.name->batCacheid, buf);
		BATmode(jb.name, PERSISTENT);
		blist[bcnt++] = ABS(jb.name->batCacheid);
	}

	TMsubcommit_list(blist, bcnt);

	unloadbats();

	*ret = 0;
	return MAL_SUCCEED;
}

str
JSONload(int *kind, int *string, int *integer, int *doble, int *array, int *object, int *name, str *nme)
{
	char buf[256];
	int bid;

	snprintf(buf, sizeof(buf), "json_%s_kind", *nme);
	bid = BBPindex(buf);
	if (!bid)
		throw(MAL, "json.load",
				"no such JSON object with name: %s", *nme);

	*kind = bid;
	snprintf(buf, sizeof(buf), "json_%s_string", *nme);
	bid = BBPindex(buf);
	if (bid) {
		*string = bid;
	} else {
		*string = 0;
	}
	snprintf(buf, sizeof(buf), "json_%s_integer", *nme);
	bid = BBPindex(buf);
	if (bid) {
		*integer = bid;
	} else {
		*integer = 0;
	}
	snprintf(buf, sizeof(buf), "json_%s_doble", *nme);
	bid = BBPindex(buf);
	if (bid) {
		*doble = bid;
	} else {
		*doble = 0;
	}
	snprintf(buf, sizeof(buf), "json_%s_array", *nme);
	bid = BBPindex(buf);
	if (bid) {
		*array = bid;
	} else {
		*array = 0;
	}
	snprintf(buf, sizeof(buf), "json_%s_object", *nme);
	bid = BBPindex(buf);
	if (bid) {
		*object = bid;
	} else {
		*object = 0;
	}
	snprintf(buf, sizeof(buf), "json_%s_name", *nme);
	bid = BBPindex(buf);
	if (bid) {
		*name = bid;
	} else {
		*name = 0;
	}

	/* incref for MAL interpreter ref */
	BBPincref(*kind, TRUE);
	BBPincref(*string, TRUE);
	BBPincref(*integer, TRUE);
	BBPincref(*doble, TRUE);
	BBPincref(*array, TRUE);
	BBPincref(*object, TRUE);
	BBPincref(*name, TRUE);
	return MAL_SUCCEED;
}

str
JSONdrop(int *ret, str *name)
{
	char buf[256];
	int bid;
	bat blist[8] = { 0, 0, 0, 0, 0, 0, 0, 0 };
	int bcnt = 1;

	snprintf(buf, sizeof(buf), "json_%s_kind", *name);
	bid = BBPindex(buf);
	if (!bid)
		throw(MAL, "json.drop",
				"no such JSON object with name: %s", *name);

	BBPclear(bid);
	blist[bcnt++] = ABS(bid);

	snprintf(buf, sizeof(buf), "json_%s_string", *name);
	bid = BBPindex(buf);
	if (bid) {
		BBPclear(bid);
		blist[bcnt++] = ABS(bid);
	}
	snprintf(buf, sizeof(buf), "json_%s_integer", *name);
	bid = BBPindex(buf);
	if (bid) {
		BBPclear(bid);
		blist[bcnt++] = ABS(bid);
	}
	snprintf(buf, sizeof(buf), "json_%s_doble", *name);
	bid = BBPindex(buf);
	if (bid) {
		BBPclear(bid);
		blist[bcnt++] = ABS(bid);
	}
	snprintf(buf, sizeof(buf), "json_%s_array", *name);
	bid = BBPindex(buf);
	if (bid) {
		BBPclear(bid);
		blist[bcnt++] = ABS(bid);
	}
	snprintf(buf, sizeof(buf), "json_%s_object", *name);
	bid = BBPindex(buf);
	if (bid) {
		BBPclear(bid);
		blist[bcnt++] = ABS(bid);
	}
	snprintf(buf, sizeof(buf), "json_%s_name", *name);
	bid = BBPindex(buf);
	if (bid) {
		BBPclear(bid);
		blist[bcnt++] = ABS(bid);
	}

	TMsubcommit_list(blist, bcnt);

	*ret = 0;
	return MAL_SUCCEED;
}

static oid
json_copy_entry(BATiter bik, BATiter bis, BATiter bii, BATiter bid, BATiter bia, BATiter bio, BATiter bin, oid start, oid v, jsonbat *jb, jsonbat *jbr)
{
	BUN w, x;
	bte k;

	BUNfndOID(w, bik, &v);
	k = *(bte *)BUNtail(bik, w);
	BUNappend(jbr->kind, &k, FALSE);

	w = BUNlast(jbr->kind) - 1 + start;
	switch (k) {
		case 'i':
			BUNfndOID(x, bii, &v);
			BUNins(jbr->integer, &w, BUNtail(bii, x), FALSE);
			break;
		case 'd':
			BUNfndOID(x, bid, &v);
			BUNins(jbr->doble, &w, BUNtail(bid, x), FALSE);
			break;
		case 's':
			BUNfndOID(x, bis, &v);
			BUNins(jbr->string, &w, BUNtail(bis, x), FALSE);
			break;
		case 'n':
		case 't':
		case 'f':
			/* nothing to do here */
			break;
		case 'o': {
			BUN p, q, y;
			oid z;
			BATloop (jb->object, p, q) {
				if (*(oid *)BUNhead(bio, p) != v)
					continue;
				x = *(oid *)BUNtail(bio, p);
				z = json_copy_entry(bik, bis, bii, bid, bia, bio, bin,
						start, x, jb, jbr);
				BUNins(jbr->object, &w, &z, FALSE);
				BUNfndOID(y, bin, &x);
				BUNins(jbr->name, &z, BUNtail(bin, y), FALSE);
			}
			break;
		}
		case 'a': {
			BUN p, q;
			oid z;
			BATloop (jb->array, p, q) {
				if (*(oid *)BUNhead(bia, p) != v)
					continue;
				z = json_copy_entry(bik, bis, bii, bid, bia, bio, bin,
						start, *(oid *)BUNtail(bia, p), jb, jbr);
				BUNins(jbr->array, &w, &z, FALSE);
			}
			break;
		}
	}

	return w;
}

str
JSONextract(int *rkind, int *rstring, int *rinteger, int *rdoble, int *rarray, int *robject, int *rname, int *kind, int *string, int *integer, int *doble, int *array, int *object, int *name, int *elems, oid *startoid)
{
	jsonbat jb, jbr;
	BAT *e;
	BATiter bie, bik, bis, bii, bid, bia, bio, bin;
	BUN p, q;
	oid v, w, z;

	loadbats();
	bik = bat_iterator(jb.kind);
	bis = bat_iterator(jb.string);
	bii = bat_iterator(jb.integer);
	bid = bat_iterator(jb.doble);
	bia = bat_iterator(jb.array);
	bio = bat_iterator(jb.object);
	bin = bat_iterator(jb.name);
	e = BBPquickdesc(ABS(*elems), FALSE);
	if (*elems < 0)
		e = BATmirror(e);
	BBPfix(*elems);
	bie = bat_iterator(e);

	memset(&jbr, 0, sizeof(jsonbat));

	/* initialise all bats */
	jbr.kind = BATnew(TYPE_void, TYPE_bte, BATTINY);
	jbr.kind = BATseqbase(jbr.kind, *startoid);
	jbr.string = BATnew(TYPE_oid, TYPE_str, BATTINY);
	jbr.doble = BATnew(TYPE_oid, TYPE_dbl, BATTINY);
	jbr.integer = BATnew(TYPE_oid, TYPE_lng, BATTINY);
	jbr.name = BATnew(TYPE_oid, TYPE_str, BATTINY);
	jbr.object = BATnew(TYPE_oid, TYPE_oid, BATTINY);
	jbr.array = BATnew(TYPE_oid, TYPE_oid, BATTINY);

	/* return all elems as the outermost array */
	BUNappend(jbr.kind, "a", FALSE);
	w = BUNlast(jbr.kind) - 1 + *startoid;
	BATloop(e, p, q) {
		v = *(oid *)BUNtail(bie, p);
		if (v != oid_nil) {
			z = json_copy_entry(bik, bis, bii, bid, bia, bio, bin,
					*startoid, v, &jb, &jbr);
			BUNins(jbr.array, &w, &z, FALSE);
		}
	}

	unloadbats();
	BBPunfix(*elems);

	BBPkeepref(jbr.kind->batCacheid);
	*rkind = jbr.kind->batCacheid;
	BBPkeepref(jbr.string->batCacheid);
	*rstring = jbr.string->batCacheid;
	BBPkeepref(jbr.integer->batCacheid);
	*rinteger = jbr.integer->batCacheid;
	BBPkeepref(jbr.doble->batCacheid);
	*rdoble = jbr.doble->batCacheid;
	BBPkeepref(jbr.array->batCacheid);
	*rarray = jbr.array->batCacheid;
	BBPkeepref(jbr.object->batCacheid);
	*robject = jbr.object->batCacheid;
	BBPkeepref(jbr.name->batCacheid);
	*rname = jbr.name->batCacheid;
	return MAL_SUCCEED;
}

str
JSONwrap(int *rkind, int *rstring, int *rinteger, int *rdoble, int *rarray, int *robject, int *rname, int *elems)
{
	jsonbat jbr;
	BAT *e;
	BATiter bie;
	BUN p, q;
	oid v, w;
	bte k;
	str s;
	lng i;
	dbl d;
	bit b;

	e = BBPquickdesc(ABS(*elems), FALSE);
	if (*elems < 0)
		e = BATmirror(e);

	/* figure out what type this is */
	switch (e->ttype) {
		case TYPE_str:
		case TYPE_lng:
		case TYPE_dbl:
		case TYPE_bit:
			break;
		default:
			throw(MAL, "json.wrap", "unsupported tail type");
	}

	BBPfix(*elems);
	bie = bat_iterator(e);

	memset(&jbr, 0, sizeof(jsonbat));

	/* initialise all bats */
	jbr.kind = BATnew(TYPE_void, TYPE_bte, BATTINY);
	jbr.kind = BATseqbase(jbr.kind, (oid)0);
	jbr.string = BATnew(TYPE_oid, TYPE_str, BATTINY);
	jbr.doble = BATnew(TYPE_oid, TYPE_dbl, BATTINY);
	jbr.integer = BATnew(TYPE_oid, TYPE_lng, BATTINY);
	jbr.name = BATnew(TYPE_oid, TYPE_str, BATTINY);
	jbr.object = BATnew(TYPE_oid, TYPE_oid, BATTINY);
	jbr.array = BATnew(TYPE_oid, TYPE_oid, BATTINY);

	/* return all elems as the outermost array */
	v = BUNlast(jbr.kind);
	BUNappend(jbr.kind, "a", FALSE);
	w = BUNlast(jbr.kind);

	BATloop(e, p, q) {
		switch (e->ttype) {
			case TYPE_str:
				s = (str)BUNtail(bie, p);
				k = 's';
				if (strcmp(s, str_nil) == 0) {
					k = 'n';
				} else {
					BUNins(jbr.string, &w, s, FALSE);
				}
				break;
			case TYPE_lng:
				i = *(lng *)BUNtail(bie, p);
				k = 'i';
				if (i == lng_nil) {
					k = 'n';
				} else {
					BUNins(jbr.integer, &w, &i, FALSE);
				}
				break;
			case TYPE_dbl:
				d = *(dbl *)BUNtail(bie, p);
				k = 'd';
				if (d == dbl_nil) {
					k = 'n';
				} else {
					BUNins(jbr.doble, &w, &d, FALSE);
				}
				break;
			case TYPE_bit:
				b = *(bit *)BUNtail(bie, p);
				if (b == bit_nil) {
					k = 'n';
				} else if (b != 0) {
					k = 't';
				} else {
					k = 'f';
				}
				break;
			default:
				assert(0);
		}
		BUNins(jbr.array, &v, &w, FALSE);
		BUNappend(jbr.kind, &k, FALSE);
		w = BUNlast(jbr.kind);
	}

	BBPunfix(*elems);

	BBPkeepref(jbr.kind->batCacheid);
	*rkind = jbr.kind->batCacheid;
	BBPkeepref(jbr.string->batCacheid);
	*rstring = jbr.string->batCacheid;
	BBPkeepref(jbr.integer->batCacheid);
	*rinteger = jbr.integer->batCacheid;
	BBPkeepref(jbr.doble->batCacheid);
	*rdoble = jbr.doble->batCacheid;
	BBPkeepref(jbr.array->batCacheid);
	*rarray = jbr.array->batCacheid;
	BBPkeepref(jbr.object->batCacheid);
	*robject = jbr.object->batCacheid;
	BBPkeepref(jbr.name->batCacheid);
	*rname = jbr.name->batCacheid;
	return MAL_SUCCEED;
}

str
JSONunwrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int *)getArgReference(stk, pci, 0);
	str *rets = (str *)getArgReference(stk, pci, 0);
	int *kind = (int *)getArgReference(stk, pci, 1);
	int *string = (int *)getArgReference(stk, pci, 2);
	int *integer = (int *)getArgReference(stk, pci, 3);
	int *doble = (int *)getArgReference(stk, pci, 4);
	int *array = (int *)getArgReference(stk, pci, 5);
	int *object = (int *)getArgReference(stk, pci, 6);
	int *name = (int *)getArgReference(stk, pci, 7);
	oid *arrid = (oid *)getArgReference(stk, pci, 8);
	ValPtr tpe = NULL;
	jsonbat jb;
	BATiter bi, bis, bii, bid, ci;
	BAT *b, *c, *r = NULL;
	BUN p, q, t, u, x;
	oid v = 0;
	lng l;
	dbl d;
	str s;
	char buf[24];
	char isUnwrap = strcmp(pci->fcnname, "unwrap") == 0;
	enum typeorder {tlng, tdbl, tstr} totype = tlng;

	(void)mb;
	(void)cntxt;

	if (isUnwrap) {
		tpe = getArgReference(stk, pci, 9);
		switch (tpe->vtype) {
			case TYPE_str:
			case TYPE_dbl:
			case TYPE_lng:
				/* ok, can do these */
				break;
			default:
				throw(MAL, "json.unwrap", "can only unwrap to "
						"str, dbl and lng values");
		}
	}

	loadbats();

	/* find types of referenced array */
	bi = bat_iterator(jb.kind);
	BUNfndOID(p, bi, arrid);
	if (*(bte *)BUNtail(bi, p) != 'a') {
		unloadbats();
		throw(MAL, isUnwrap ? "json.unwrap" : "json.unwraptype",
				"JSON value must be an array");
	}
	b = BATselect(BATmirror(jb.array), BUNhead(bi, p), NULL);
	b = BATsemijoin(jb.kind, b);
	bi = bat_iterator(b);

	if (BATcount(b) == 0) {
		/* input empty, return empty result */
		unloadbats();
		switch (tpe->vtype) {
			case TYPE_str:
				r = BATnew(TYPE_oid, TYPE_str, 0);
				break;
			case TYPE_dbl:
				r = BATnew(TYPE_oid, TYPE_dbl, 0);
				break;
			case TYPE_lng:
				r = BATnew(TYPE_oid, TYPE_lng, 0);
				break;
			default:
				assert(0); /* should not happen, checked above */
		}
		BBPkeepref(r->batCacheid);
		*ret = r->batCacheid;
		return MAL_SUCCEED;
	}

	/* special case for when the argument is a single array */
	c = BATantiuselect_(b, "a", NULL, TRUE, TRUE);
	if (BATcount(c) != 0) {
		c = BATmirror(BATselect(BATmirror(jb.kind), arrid, NULL));
	} else {
		c = b;
	}
	ci = bat_iterator(c);

	BATloop(c, t, u) {
		v = *(oid *)BUNhead(ci, t);
		b = BATselect(BATmirror(jb.array), &v, NULL);
		b = BATsemijoin(jb.kind, b);
		bi = bat_iterator(b);

		if (!isUnwrap) {
			BATloop(b, p, q) {
				switch (*(bte *)BUNtail(bi, p)) {
					case 't':
					case 'f':
					case 'n':
					case 'i':
						/* lower than or equal to the minimum type (lng) */
						break;
					case 'd':
						if (totype < tdbl)
							totype = tdbl;
						break;
					default:
						totype = tstr;
						break;
				}
			}
		} else {
			bis = bat_iterator(jb.string);
			bii = bat_iterator(jb.integer);
			bid = bat_iterator(jb.doble);
			switch (tpe->vtype) {
				case TYPE_str:
					if (r == NULL)
						r = BATnew(TYPE_oid, TYPE_str, BATcount(b));
					BATloop(b, p, q) {
						switch (*(bte *)BUNtail(bi, p)) {
							case 's':
								BUNfndOID(x, bis, BUNhead(bi, p));
								BUNins(r, &v, BUNtail(bis, x), FALSE);
								break;
							case 'i':
								BUNfndOID(x, bii, BUNhead(bi, p));
								snprintf(buf, sizeof(buf), "long('%lld')",
										*(lng *)BUNtail(bii, x));
								BUNins(r, &v, buf, FALSE);
								break;
							case 'd':
								BUNfndOID(x, bid, BUNhead(bi, p));
								snprintf(buf, sizeof(buf), "double('%f')",
										*(dbl *)BUNtail(bid, x));
								BUNins(r, &v, buf, FALSE);
								break;
							case 't':
								snprintf(buf, sizeof(buf), "bool('true')");
								BUNins(r, &v, buf, FALSE);
								break;
							case 'f':
								snprintf(buf, sizeof(buf), "bool('false')");
								BUNins(r, &v, buf, FALSE);
								break;
							case 'n':
								snprintf(buf, sizeof(buf), "(null)");
								BUNins(r, &v, buf, FALSE);
								break;
							default:
								/* JSON piece (object/array), serialise */
								(void)s;
								/* TODO: implement right call */;
								snprintf(buf, sizeof(buf),
										"FIXME: not yet implemented");
								BUNins(r, &v, buf, FALSE);
								break;
						}
					}
					if (BATcount(b) == 0)
						BUNins(r, &v, str_nil, FALSE);
					break;
				case TYPE_dbl:
					if (r == NULL)
						r = BATnew(TYPE_oid, TYPE_dbl, BATcount(b));
					BATloop(b, p, q) {
						switch (*(bte *)BUNtail(bi, p)) {
							case 's':
								BUNfndOID(x, bis, BUNhead(bi, p));
								d = atof((str)BUNtail(bis, x));
								BUNins(r, &v, &d, FALSE);
								break;
							case 'i':
								BUNfndOID(x, bii, BUNhead(bi, p));
								d = (dbl)*(lng *)BUNtail(bii, x);
								BUNins(r, &v, &d, FALSE);
								break;
							case 'd':
								BUNfndOID(x, bid, BUNhead(bi, p));
								BUNins(r, &v, BUNtail(bid, x), FALSE);
								break;
							case 't':
								d = 1.0;
								BUNins(r, &v, &d, FALSE);
								break;
							case 'f':
								d = 0.0;
								BUNins(r, &v, &d, FALSE);
								break;
							case 'n':
							default:
								d = 0.0;
								BUNins(r, &v, &d, FALSE);
								break;
						}
					}
					if (BATcount(b) == 0) {
						d = dbl_nil;
						BUNins(r, &v, &d, FALSE);
					}
					break;
				case TYPE_lng:
					if (r == NULL)
						r = BATnew(TYPE_oid, TYPE_lng, BATcount(b));
					BATloop(b, p, q) {
						switch (*(bte *)BUNtail(bi, p)) {
							case 's':
								BUNfndOID(x, bis, BUNhead(bi, p));
								l = atoi((str)BUNtail(bis, x));
								BUNins(r, &v, &l, FALSE);
								break;
							case 'd':
								BUNfndOID(x, bid, BUNhead(bi, p));
								l = (lng)*(dbl *)BUNtail(bid, x);
								BUNins(r, &v, &l, FALSE);
								break;
							case 'i':
								BUNfndOID(x, bii, BUNhead(bi, p));
								BUNins(r, &v, BUNtail(bii, x), FALSE);
								break;
							case 't':
								l = 1;
								BUNins(r, &v, &l, FALSE);
								break;
							case 'f':
								l = 0;
								BUNins(r, &v, &l, FALSE);
								break;
							case 'n':
							default:
								l = 0;
								BUNins(r, &v, &l, FALSE);
								break;
						}
					}
					if (BATcount(b) == 0) {
						l = lng_nil;
						BUNins(r, &v, &l, FALSE);
					}
					break;
			}
		}
	}

	unloadbats();

	if (!isUnwrap) {
		if (totype == tlng) {
			*rets = GDKstrdup("lng");
		} else if (totype == tdbl) {
			*rets = GDKstrdup("dbl");
		} else {
			*rets = GDKstrdup("str");
		}
	} else {
		assert(r != NULL);
		BBPkeepref(r->batCacheid);
		*ret = r->batCacheid;
	}

	return MAL_SUCCEED;
}

str
JSONnextid(oid *ret, int *kind)
{
	jsonbat jb;
	BATiter bi;
	oid lastid = 0;

	loadbat(kind);

	bi = bat_iterator(jb.kind);
	if (BAThordered(jb.kind)) {
		lastid = *(oid *)BUNhead(bi, BUNlast(jb.kind) - 1);
	} else {
		BUN p, q;
		oid h;
		BATloop(jb.kind, p, q) {
			h = *(oid *)BUNhead(bi, p);
			if (h > lastid)
				lastid = h;
		}
	}

	unloadbat(kind);

	*ret = lastid + 1;
	return MAL_SUCCEED;
}
