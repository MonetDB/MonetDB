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

#ifndef JSON_H
#define JSON_H

#include <gdk.h>
#include "mal.h"
#include "mal_client.h"
#include "mal_instruction.h"
#include "mal_exception.h"

#define JSON_OBJECT 1
#define JSON_ARRAY 2
#define JSON_ELEMENT 3
#define JSON_VALUE 4
#define JSON_STRING 5
#define JSON_NUMBER 6
#define JSON_BOOL 7
#define JSON_NULL 8

/* The JSON index structure is meant for short lived versions */
typedef struct JSONterm {
    short kind;
    char *name; /* exclude the quotes */
    int namelen;
    char *value; /* start of string rep */
    int valuelen;
    int child, next, tail; /* next offsets allow you to walk array/object chains and append quickly */
    /* An array or object item has a number of components */
} JSONterm; 

typedef struct JSON{
    JSONterm *elm;
    str error;
    int size;
    int free;
} JSON;

typedef str json;

#ifdef WIN32
#ifndef LIBATOMS
#define json_export extern __declspec(dllimport)
#else
#define json_export extern __declspec(dllexport)
#endif
#else
#define json_export extern
#endif

json_export int TYPE_json;

json_export int JSONfromString(str src, int *len, json *x);
json_export int JSONtoString(str *s, int *len, json src);


json_export str JSONstr2json(json *ret, str *j);
json_export str JSONjson2str(str *ret, json *j);
json_export str JSONjson2text(str *ret, json *arg);
json_export str JSONjson2textSeparator(str *ret, json *arg, str *sep);

json_export str JSONfilter( json *ret, json *js, str *expr);
json_export str JSONfilterArray(json *ret, json *j, int *index);
json_export str JSONfilterArrayDefault(json *ret, json *j, int *index, str *other);

json_export str JSONisvalid(bit *ret, json *j);
json_export str JSONisobject(bit *ret, json *j);
json_export str JSONisarray(bit *ret, json *j);

json_export str JSONlength(int *ret, json *j);
json_export str JSONunfold(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
json_export str JSONfold(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
json_export str JSONkeyTable(int *ret, json *j);
json_export str JSONvalueTable(int *ret, json *j);
json_export str JSONkeyArray(json *ret, json *arg);
json_export str JSONvalueArray(json *ret, json *arg);

json_export str JSONdump(int *ret, json *val);
json_export str JSONprelude(int *ret);

json_export str JSONrenderobject(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
json_export str JSONrenderarray(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* JSON_H */
