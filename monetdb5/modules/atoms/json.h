/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#ifndef JSON_H
#define JSON_H

#include "gdk.h"
#include "mal.h"
#include "mal_client.h"
#include "mal_instruction.h"
#include "mal_exception.h"

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

mal_export int TYPE_json;

mal_export ssize_t JSONfromString(const char *src, size_t *len, json *x, bool external);
mal_export ssize_t JSONtoString(str *s, size_t *len, const char *src, bool external);


mal_export str JSONstr2json(json *ret, str *j);
mal_export str JSONjson2str(str *ret, json *j);
mal_export str JSONjson2text(str *ret, json *arg);
mal_export str JSONjson2textSeparator(str *ret, json *arg, str *sep);
mal_export str JSONjson2number(dbl *ret, json *arg);
mal_export str JSONjson2integer(lng *ret, json *arg);

mal_export str JSONfilter( json *ret, json *js, str *expr);
mal_export str JSONfilterArray_bte(json *ret, json *j, bte *index);
mal_export str JSONfilterArrayDefault_bte(json *ret, json *j, bte *index, str *other);
mal_export str JSONfilterArray_sht(json *ret, json *j, sht *index);
mal_export str JSONfilterArrayDefault_sht(json *ret, json *j, sht *index, str *other);
mal_export str JSONfilterArray_int(json *ret, json *j, int *index);
mal_export str JSONfilterArrayDefault_int(json *ret, json *j, int *index, str *other);
mal_export str JSONfilterArray_lng(json *ret, json *j, lng *index);
mal_export str JSONfilterArrayDefault_lng(json *ret, json *j, lng *index, str *other);
#ifdef HAVE_HGE
mal_export str JSONfilterArray_hge(json *ret, json *j, hge *index);
mal_export str JSONfilterArrayDefault_hge(json *ret, json *j, hge *index, str *other);
#endif

mal_export str JSONisvalid(bit *ret, json *j);
mal_export str JSONisobject(bit *ret, json *j);
mal_export str JSONisarray(bit *ret, json *j);

mal_export str JSONlength(int *ret, json *j);
mal_export str JSONunfold(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str JSONfold(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str JSONkeyTable(bat *ret, json *j);
mal_export str JSONvalueTable(bat *ret, json *j);
mal_export str JSONkeyArray(json *ret, json *arg);
mal_export str JSONvalueArray(json *ret, json *arg);

mal_export str JSONtextString(str *ret, bat *bid);
mal_export str JSONtextGrouped(bat *ret, bat *bid, bat *gid, bat *ext, bit *flg);
mal_export str JSONdump(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str JSONprelude(void *ret);

mal_export str JSONrenderobject(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str JSONrenderarray(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str JSONgroupStr(str *ret, const bat *bid);
mal_export str JSONsubjsoncand(bat *retval, bat *bid, bat *gid, bat *eid, bat *id, bit *skip_nils);
mal_export str JSONsubjson(bat *retval, bat *bid, bat *gid, bat *eid, bit *skipnils);
#endif /* JSON_H */
