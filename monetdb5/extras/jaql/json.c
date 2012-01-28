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
 * Copyright August 2008-2012 MonetDB B.V.
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
} jsonbat;

static char *parse_json_value(jsonbat *jb, oid *v, char *p);
static char *parse_json_object(jsonbat *jb, oid *id, char *p);
static char *parse_json_array(jsonbat *jb, oid *id, char *p);

static char *
parse_json_string(jsonbat *jb, oid *v, char pair, char *p)
{
	char escape = 0;
	char *n = p;
	char *w = p;

	for (; *p != '\0'; p++) {
		switch (*p) {
			case '\\':
				if (escape) {
					*w = '\\';
				} else if (w != p) {
					*w = *p;
				}
				w++;
				escape = !escape;
				break;
			case 'b':
				if (escape) {
					*w = '\b';
				} else if (w != p) {
					*w = *p;
				}
				w++;
				escape = 0;
				break;
			case 'f':
				if (escape) {
					*w = '\f';
				} else if (w != p) {
					*w = *p;
				}
				w++;
				escape = 0;
				break;
			case 'n':
				if (escape) {
					*w = '\n';
				} else if (w != p) {
					*w = *p;
				}
				w++;
				escape = 0;
				break;
			case 'r':
				if (escape) {
					*w = '\r';
				} else if (w != p) {
					*w = *p;
				}
				w++;
				escape = 0;
				break;
			case 't':
				if (escape) {
					*w = '\t';
				} else if (w != p) {
					*w = *p;
				}
				w++;
				escape = 0;
				break;
			case '"':
				if (escape) {
					*w++ = '"';
					escape = 0;
				} else {
					*w = '\0';
					break;
				}
			/* TODO: unicode escapes \u0xxxx */
			default:
				if (w != p)
					*w = *p;
				w++;
				escape = 0;
				break;
		}
		if (*w == '\0')
			break;
	}
	if (*w != *p && *p == '\0') {
		jb->error = GDKstrdup("unexpected end of stream while reading string");
		return NULL;
	}
	if (pair == 0) {
		BUNappend(jb->kind, "s", FALSE);
		*v = BUNlast(jb->kind) - 1;
		BUNins(jb->string, v, n, FALSE);
	}
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

	for (; *p != '\0'; p++) {
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
		long long int i = atoll(n);
		BUNappend(jb->kind, "i", FALSE);
		*v = BUNlast(jb->kind) - 1;
		BUNins(jb->integer, v, &i, FALSE);
	}
	*p = x;
	return p;
}

static char *
parse_json_boolean(jsonbat *jb, oid *v, char *p)
{
	if (*p == 't') {
		if (strncmp(p, "true", strlen("true")) != 0) {
			jb->error = GDKstrdup("expected 'true'");
			return NULL;
		}
		p += strlen("true");
		BUNappend(jb->kind, "t", FALSE);
		*v = BUNlast(jb->kind) - 1;
	} else if (*p == 'f') {
		if (strncmp(p, "false", strlen("false")) != 0) {
			jb->error = GDKstrdup("expected 'false'");
			return NULL;
		}
		p += strlen("false");
		BUNappend(jb->kind, "f", FALSE);
		*v = BUNlast(jb->kind) - 1;
	}
	return p;
}

static char *
parse_json_null(jsonbat *jb, oid *v, char *p)
{
	if (*p == 'n') {
		if (strncmp(p, "null", strlen("null")) != 0) {
			jb->error = GDKstrdup("expected 'null'");
			return NULL;
		}
		p += strlen("null");
	}
	BUNappend(jb->kind, "n", FALSE);
	*v = BUNlast(jb->kind) - 1;
	return p;
}

static char *
parse_json_value(jsonbat *jb, oid *v, char *p)
{
	switch (*p) {
		case '"':
			p = parse_json_string(jb, v, 0, p + 1);
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
			p = parse_json_boolean(jb, v, p);
			break;
		case 'n':
			p = parse_json_null(jb, v, p);
			break;
		default:
			jb->error = GDKstrdup("unexpected character 'X' for value");
			jb->error[22] = *p;
			return NULL;
	}
	return p;
}

static char *
parse_json_pair(jsonbat *jb, oid *v, char *p)
{
	oid n = (oid)0;
	char *x;

	if (*p != '"') {
		jb->error = GDKstrdup("expected string for pair");
		return NULL;
	}

	x = p + 1;
	if ((p = parse_json_string(jb, &n, 1, x)) == NULL)
		return NULL;
	for (; *p != '\0' && isspace(*p); p++)
		;
	if (*p != ':') {
		jb->error = GDKstrdup("exected ':' for pair");
		return NULL;
	}
	p++;
	for (; *p != '\0' && isspace(*p); p++)
		;
	if ((p = parse_json_value(jb, v, p)) == NULL)
		return NULL;

	BUNins(jb->name, v, x, FALSE);

	return p;
}

static char *
parse_json_object(jsonbat *jb, oid *id, char *p)
{
	oid v = (oid)0;

	BUNappend(jb->kind, "o", FALSE);
	*id = BUNlast(jb->kind) - 1;

	for (; *p != '\0'; p++) {
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

	for (; *p != '\0'; p++) {
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
	jb.name = BBPquickdesc(ABS(*name), FALSE); \
	if (*name < 0) \
		jb.name = BATmirror(jb.name); \
	BBPfix(*name);
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

str
JSONshred(int *kind, int *string, int *integer, int *doble, int *array, int *object, int *name, str *json)
{
	char *p = *json;
	jsonbat jb;
	oid v = (oid)0; 

	memset(&jb, 0, sizeof(jsonbat));

	/* initialise all bats */
	jb.kind = BATnew(TYPE_void, TYPE_chr, BATTINY);
	jb.kind = BATseqbase(jb.kind, (oid)0);
	jb.string = BATnew(TYPE_oid, TYPE_str, BATTINY);
	jb.doble = BATnew(TYPE_oid, TYPE_dbl, BATTINY);
	jb.integer = BATnew(TYPE_oid, TYPE_lng, BATTINY);
	jb.name = BATnew(TYPE_oid, TYPE_str, BATTINY);
	jb.object = BATnew(TYPE_oid, TYPE_oid, BATTINY);
	jb.array = BATnew(TYPE_oid, TYPE_oid, BATTINY);

	for (; *p != '\0' && isspace(*p); p++)
		;
	if (*p == '\0') {
		jb.error = GDKstrdup("expected data");
		p = NULL;
	} else {
		p = parse_json_value(&jb, &v, p);
	}

	for (; p != NULL && *p != '\0' && isspace(*p); p++)
		;
	if (p == NULL || *p != '\0') {
		str e;
		if (p == NULL) {
			/* parsing failed */
			e = createException(MAL, "json.shred", "%s", jb.error);
		} else {
			e = createException(MAL, "json.shred", "invalid JSON data, "
					"trailing characters: %s", p);
		}
		BBPunfix(jb.kind->batCacheid);
		BBPunfix(jb.string->batCacheid);
		BBPunfix(jb.integer->batCacheid);
		BBPunfix(jb.doble->batCacheid);
		BBPunfix(jb.array->batCacheid);
		BBPunfix(jb.object->batCacheid);
		BBPunfix(jb.name->batCacheid);
		GDKfree(jb.error);
		return e;
	}
	BBPkeepref(jb.kind->batCacheid);
	*kind = jb.kind->batCacheid;
	BBPkeepref(jb.string->batCacheid);
	*string = jb.string->batCacheid;
	BBPkeepref(jb.integer->batCacheid);
	*integer = jb.integer->batCacheid;
	BBPkeepref(jb.doble->batCacheid);
	*doble = jb.doble->batCacheid;
	BBPkeepref(jb.array->batCacheid);
	*array = jb.array->batCacheid;
	BBPkeepref(jb.object->batCacheid);
	*object = jb.object->batCacheid;
	BBPkeepref(jb.name->batCacheid);
	*name = jb.name->batCacheid;
	return MAL_SUCCEED;
}

static void
print_json_value(jsonbat *jb, stream *s, oid id)
{
	BATiter bi;
	oid v;
	bi = bat_iterator(jb->kind);

	BUNfndOID(v, bi, &id);
	switch (*(char *)BUNtail(bi, v)) {
		case 'i':
			bi = bat_iterator(jb->integer);
			BUNfndOID(v, bi, &id);
			mnstr_printf(s, "%lld", *(lng *)BUNtail(bi, v));
			break;
		case 'd':
			bi = bat_iterator(jb->doble);
			BUNfndOID(v, bi, &id);
			mnstr_printf(s, "%f", *(dbl *)BUNtail(bi, v));
			break;
		case 's':
			bi = bat_iterator(jb->string);
			BUNfndOID(v, bi, &id);
			mnstr_printf(s, "\"%s\"", BUNtail(bi, v));
			break;
		case 't':
			mnstr_printf(s, "true");
			break;
		case 'f':
			mnstr_printf(s, "false");
			break;
		case 'n':
			mnstr_printf(s, "null");
			break;
		case 'a':
		{
			BAT *elems;
			BUN p, q;
			elems = BATmirror(BATselect(BATmirror(jb->array), &id, &id));
			bi = bat_iterator(elems);
			mnstr_printf(s, "[ ");
			BATloop (elems, p, q) {
				print_json_value(jb, s, *(oid *)BUNtail(bi, p));
				if (p < q - 1)
					mnstr_printf(s, ", ");
			}
			mnstr_printf(s, " ]");
			BBPunfix(elems->batCacheid);
			break;
		}
		case 'o':
		{
			BATiter ni;
			BAT *objects = BATmirror(BATselect(BATmirror(jb->object), &id, &id));
			BUN p, q;
			oid n;
			mnstr_printf(s, "{ ");
			bi = bat_iterator(objects);
			ni = bat_iterator(jb->name);
			BATloop (objects, p, q) {
				BUNfndOID(n, ni, BUNtail(bi, p));
				mnstr_printf(s, "\"%s\": ", BUNtail(ni, n));
				print_json_value(jb, s, *(oid *)BUNtail(bi, p));
				if (p < q - 1)
					mnstr_printf(s, ", ");
			}
			mnstr_printf(s, " }");
			BBPunfix(objects->batCacheid);
			break;
		}
	}
}

str
JSONprint(int *ret, stream **s, int *kind, int *string, int *integer, int *doble, int *array, int *object, int *name)
{
	jsonbat jb;
	BATiter bi;

	loadbats();

	bi = bat_iterator(jb.kind);

	print_json_value(&jb, *s, *(oid *)BUNhead(bi, BUNfirst(jb.kind)));
	mnstr_printf(*s, "\n");

	unloadbats();

	*ret = 0;
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
		throw(MAL, "json.store",
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

	return MAL_SUCCEED;
}

str
JSONdrop(int *ret, str *name)
{
	char buf[256];
	int bid;
	BAT *t;
	bat blist[8] = { 0, 0, 0, 0, 0, 0, 0, 0 };
	int bcnt = 1;

	snprintf(buf, sizeof(buf), "json_%s_kind", *name);
	bid = BBPindex(buf);
	if (!bid)
		throw(MAL, "json.store",
				"no such JSON object with name: %s", *name);

	t = BBPquickdesc(ABS(bid), FALSE);
	BATmode(t, TRANSIENT);
	blist[bcnt++] = ABS(bid);

	snprintf(buf, sizeof(buf), "json_%s_string", *name);
	bid = BBPindex(buf);
	if (bid) {
		t = BBPquickdesc(ABS(bid), FALSE);
		BATmode(t, TRANSIENT);
		blist[bcnt++] = ABS(bid);
	}
	snprintf(buf, sizeof(buf), "json_%s_integer", *name);
	bid = BBPindex(buf);
	if (bid) {
		t = BBPquickdesc(ABS(bid), FALSE);
		BATmode(t, TRANSIENT);
		blist[bcnt++] = ABS(bid);
	}
	snprintf(buf, sizeof(buf), "json_%s_doble", *name);
	bid = BBPindex(buf);
	if (bid) {
		t = BBPquickdesc(ABS(bid), FALSE);
		BATmode(t, TRANSIENT);
		blist[bcnt++] = ABS(bid);
	}
	snprintf(buf, sizeof(buf), "json_%s_array", *name);
	bid = BBPindex(buf);
	if (bid) {
		t = BBPquickdesc(ABS(bid), FALSE);
		BATmode(t, TRANSIENT);
		blist[bcnt++] = ABS(bid);
	}
	snprintf(buf, sizeof(buf), "json_%s_object", *name);
	bid = BBPindex(buf);
	if (bid) {
		t = BBPquickdesc(ABS(bid), FALSE);
		BATmode(t, TRANSIENT);
		blist[bcnt++] = ABS(bid);
	}
	snprintf(buf, sizeof(buf), "json_%s_name", *name);
	bid = BBPindex(buf);
	if (bid) {
		t = BBPquickdesc(ABS(bid), FALSE);
		BATmode(t, TRANSIENT);
		blist[bcnt++] = ABS(bid);
	}

	TMsubcommit_list(blist, bcnt);

	*ret = 0;
	return MAL_SUCCEED;
}

static oid
json_copy_entry(BATiter bik, BATiter bis, BATiter bii, BATiter bid, BATiter bia, BATiter bio, BATiter bin, oid start, oid v, jsonbat *jb, jsonbat *jbr)
{
	oid w, x;
	chr k;

	BUNfndOID(w, bik, &v);
	k = *(chr *)BUNtail(bik, w);
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
			BAT *elems;
			BUN p, q;
			BATiter bi;
			oid y, z;
			elems = BATmirror(BATselect(BATmirror(jb->object), &v, &v));
			bi = bat_iterator(elems);
			BATloop (elems, p, q) {
				x = *(oid *)BUNtail(bi, p);
				z = json_copy_entry(bik, bis, bii, bid, bia, bio, bin,
						start, x, jb, jbr);
				BUNins(jbr->object, &w, &z, FALSE);
				BUNfndOID(y, bin, &x);
				BUNins(jbr->name, &z, BUNtail(bin, y), FALSE);
			}
			BBPunfix(elems->batCacheid);
			break;
		}
		case 'a': {
			BAT *elems;
			BUN p, q;
			BATiter bi;
			oid z;
			elems = BATmirror(BATselect(BATmirror(jb->array), &v, &v));
			bi = bat_iterator(elems);
			BATloop (elems, p, q) {
				z = json_copy_entry(bik, bis, bii, bid, bia, bio, bin,
						start, *(oid *)BUNtail(bi, p), jb, jbr);
				BUNins(jbr->array, &w, &z, FALSE);
			}
			BBPunfix(elems->batCacheid);
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
	jbr.kind = BATnew(TYPE_void, TYPE_chr, BATTINY);
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
		z = json_copy_entry(bik, bis, bii, bid, bia, bio, bin,
				*startoid, v, &jb, &jbr);
		BUNins(jbr.array, &w, &z, FALSE);
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
