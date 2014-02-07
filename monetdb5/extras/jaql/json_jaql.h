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
#define JSON_H 1

#include "stream.h"
#include "gdk.h"
#include "mal_client.h"
#include "mal_interpreter.h"

#ifdef WIN32
#ifndef LIBJSON_JAQL
#define json_export extern __declspec(dllimport)
#else
#define json_export extern __declspec(dllexport)
#endif
#else
#define json_export extern
#endif

json_export str JSONshred(int *kind, int *string, int *integer, int *doble, int *array, int *object, int *name, str *json);
json_export str JSONshredstream(int *kind, int *string, int *integer, int *doble, int *array, int *object, int *name, str *uri);
json_export str JSONprint(int *ret, stream **s, int *kind, int *string, int *integer, int *doble, int *array, int *object, int *name, oid *start, bit *pretty);
json_export str JSONexportResult(int *ret, stream **s, int *kind, int *string, int *integer, int *doble, int *array, int *object, int *name, oid *start);
json_export str JSONstore(int *ret, str *nme, int *kind, int *string, int *integer, int *doble, int *array, int *object, int *name);
json_export str JSONload(int *kind, int *string, int *integer, int *doble, int *array, int *object, int *name, str *nme);
json_export str JSONdrop(int *ret, str *name);
json_export str JSONextract(int *rkind, int *rstring, int *rinteger, int *rdoble, int *rarray, int *robject, int *rname, int *kind, int *string, int *integer, int *doble, int *array, int *object, int *name, int *elems, oid *startoid);
json_export str JSONwrap(int *rkind, int *rstring, int *rinteger, int *rdoble, int *rarray, int *robject, int *rname, int *elems);
json_export str JSONunwraptype(str *ret, int *kind, int *string, int *integer, int *doble, int *array, int *object, int *name, oid *arrid);
json_export str JSONunwrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
json_export str JSONnextid(oid *ret, int *kind);

#endif

