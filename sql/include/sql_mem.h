/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef _SQL_MEM_H_
#define _SQL_MEM_H_

#include "gdk.h"

#define SQL_OK 	1
#define SQL_ERR 0
#define SQL_CONFLICT 2

#ifdef WIN32
#ifdef LIBSQL
#define sql_export extern __declspec(dllexport)
#else
#define sql_export extern __declspec(dllimport)
#endif
#else
#define sql_export extern
#endif

#define MNEW( type ) (type*)GDKmalloc(sizeof(type) )
#define ZNEW( type ) (type*)GDKzalloc(sizeof(type) )
#define NEW_ARRAY( type, size ) (type*)GDKmalloc((size)*sizeof(type))
#define ZNEW_ARRAY( type, size ) (type*)GDKzalloc((size)*sizeof(type))
#define RENEW_ARRAY( type,ptr,size) (type*)GDKrealloc((void*)ptr,(size)*sizeof(type))

#define _DELETE( ptr )	do { GDKfree(ptr); ptr = NULL; } while (0)
#define _STRDUP( ptr )	GDKstrdup((char*)ptr)

#define SA_NEW( sa, type ) (sa?((type*)sa_alloc( sa, sizeof(type))):MNEW(type))
#define SA_ZNEW( sa, type ) (sa?((type*)sa_zalloc( sa, sizeof(type))):ZNEW(type))
#define SA_NEW_ARRAY( sa, type, size ) (sa?(type*)sa_alloc( sa, ((size)*sizeof(type))):NEW_ARRAY(type,size))
#define SA_ZNEW_ARRAY( sa, type, size ) (type*)sa_zalloc( sa, ((size)*sizeof(type)))
#define SA_RENEW_ARRAY( sa, type, ptr, sz, osz ) (sa?(type*)sa_realloc( sa, ptr, ((sz)*sizeof(type)), ((osz)*sizeof(type))):RENEW_ARRAY(type,ptr,sz))
#define SA_STRDUP( sa, s) (sa?sa_strdup(sa, s):_STRDUP(s))

#define _strlen(s) (int)strlen(s)

#endif /*_SQL_MEM_H_*/
