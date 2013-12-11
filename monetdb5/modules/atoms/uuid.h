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
 * @-
 * Wrapper around uuid library
 */
#ifndef AUUID_H
#define AUUID_H

#include <gdk.h>
#include <ctype.h>

typedef str uuid;

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define uuid_export extern __declspec(dllimport)
#else
#define uuid_export extern __declspec(dllexport)
#endif
#else
#define uuid_export extern
#endif

uuid_export int UUIDtoString(str *retval, int *len, str handle);
uuid_export int UUIDfromString(char *svalue, int *len, str *retval);
uuid_export str UUIDgenerateUuid(str *retval);
uuid_export str UUIDstr2uuid(str *retval, str *s);
uuid_export str UUIDuuid2str(str *retval, str *s);
uuid_export str UUIDisaUUID(bit *retval, str *s);
uuid_export str UUIDequal(bit *retval, str *l, str *r);

#endif /* AUUID_H */
