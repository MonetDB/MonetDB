#ifndef _MEM_H_
#define _MEM_H_

/*#ifdef _MSC_VER*/
#include <sql_config.h>
/*#endif*/

#include <gdk.h>
#include <stdio.h>
#include <assert.h>

#define SQL_OK 	GDK_SUCCEED
#define SQL_ERR GDK_FAIL

#ifdef _MSC_VER
#ifndef LIBSQLSERVER
#define sql_export extern __declspec(dllimport)
#else
#define sql_export extern __declspec(dllexport)
#endif
#ifndef LIBSQLCOMMON
#define sqlcommon_export extern __declspec(dllimport)
#else
#define sqlcommon_export extern __declspec(dllexport)
#endif
#ifndef LIBBATSTORE
#define sqlbat_export extern __declspec(dllimport)
#else
#define sqlbat_export extern __declspec(dllexport)
#endif
#else
#define sql_export extern
#define sqlcommon_export extern
#define sqlbat_export extern
#endif

#define NEW( type ) (type*)GDKmalloc(sizeof(type) )
#define NEW_ARRAY( type, size ) (type*)GDKmalloc((size)*sizeof(type))
#define RENEW_ARRAY( type,ptr,size) (type*)GDKrealloc((void*)ptr,(size)*sizeof(type))

#define NEWADT( size ) (adt*)GDKmalloc(size)
#define _DELETE( ptr )	{ GDKfree(ptr); ptr = NULL; }
#define _strdup( ptr )	GDKstrdup((char*)ptr)

typedef struct sql_ref {
	int refcnt;
} sql_ref;

sqlcommon_export sql_ref *sql_ref_init(sql_ref *r);
sqlcommon_export int sql_ref_inc(sql_ref *r);
sqlcommon_export int sql_ref_dec(sql_ref *r);

#endif /*_MEM_H_*/
