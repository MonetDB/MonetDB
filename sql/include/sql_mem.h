/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _SQL_MEM_H_
#define _SQL_MEM_H_

#include <gdk.h>

#define SQL_OK 	1
#define SQL_ERR 0

#ifdef WIN32
#if defined(LIBSQLSERVER) || defined(LIBSQLCOMMON) || defined(LIBBATSTORE) || defined(LIBSTORE)
#define sql_export extern __declspec(dllexport)
#define sqlcommon_export extern __declspec(dllexport)
#define sqlbat_export extern __declspec(dllexport)
#define sqlstore_export extern __declspec(dllexport)
#else
#define sql_export extern __declspec(dllimport)
#define sqlcommon_export extern __declspec(dllimport)
#define sqlbat_export extern __declspec(dllimport)
#define sqlstore_export extern __declspec(dllimport)
#endif
#else
#define sql_export extern
#define sqlcommon_export extern
#define sqlbat_export extern
#define sqlstore_export extern
#endif

#define MNEW( type ) (type*)GDKmalloc(sizeof(type) )
#define ZNEW( type ) (type*)GDKzalloc(sizeof(type) )
#define NEW_ARRAY( type, size ) (type*)GDKmalloc((size)*sizeof(type))
#define RENEW_ARRAY( type,ptr,size) (type*)GDKrealloc((void*)ptr,(size)*sizeof(type))

#define NEWADT( size ) (adt*)GDKmalloc(size)
#define _DELETE( ptr )	do { GDKfree(ptr); ptr = NULL; } while (0)
#define _STRDUP( ptr )	GDKstrdup((char*)ptr)

typedef struct sql_ref {
	int refcnt;
} sql_ref;

extern sql_ref *sql_ref_init(sql_ref *r);
extern int sql_ref_inc(sql_ref *r);
extern int sql_ref_dec(sql_ref *r);

typedef struct sql_allocator {
	size_t size;
	size_t nr;
	char **blks;
	size_t used; 	/* memory used in last block */
	size_t usedmem;	/* used memory */
} sql_allocator;

extern sql_allocator *sa_create(void);
extern sql_allocator *sa_reset( sql_allocator *sa );
extern char *sa_alloc( sql_allocator *sa,  size_t sz );
extern char *sa_zalloc( sql_allocator *sa,  size_t sz );
extern char *sa_realloc( sql_allocator *sa,  void *ptr, size_t sz, size_t osz );
extern void sa_destroy( sql_allocator *sa );
extern char *sa_strndup( sql_allocator *sa, const char *s, size_t l);
extern char *sa_strdup( sql_allocator *sa, const char *s);
extern char *sa_strconcat( sql_allocator *sa, const char *s1, const char *s2);
extern size_t sa_size( sql_allocator *sa );

#define SA_NEW( sa, type ) ((type*)sa_alloc( sa, sizeof(type)) )
#define SA_ZNEW( sa, type ) ((type*)sa_zalloc( sa, sizeof(type)) )
#define SA_NEW_ARRAY( sa, type, size ) (type*)sa_alloc( sa, ((size)*sizeof(type)))
#define SA_RENEW_ARRAY( sa, type, ptr, sz, osz ) (type*)sa_realloc( sa, ptr, ((sz)*sizeof(type)), ((osz)*sizeof(type)))

#define _strlen(s) (int)strlen(s)

#endif /*_SQL_MEM_H_*/
