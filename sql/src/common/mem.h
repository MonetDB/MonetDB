#ifndef _MEM_H_ 
#define _MEM_H_ 

#ifdef _MSC_VER
#include <sql_config.h>
#endif

#include <stdio.h>
#include <assert.h>

#ifdef HAVE_MALLOC_H 
#include <malloc.h>
#endif

#ifdef _MSC_VER
#define gdk_export extern __declspec(dllimport)   
#else
#define gdk_export extern
#endif

gdk_export void* 	GDKmalloc  (size_t size);
gdk_export void* 	GDKrealloc (void* pold, size_t size);
gdk_export void	GDKfree    (void* blk); 
gdk_export char*	GDKstrdup  (char *s);

#define NEW( type ) (type*)GDKmalloc(sizeof(type) )
#define NEW_ARRAY( type, size ) (type*)GDKmalloc((size)*sizeof(type))
#define RENEW_ARRAY( type,ptr,size) (type*)GDKrealloc((void*)ptr,(size)*sizeof(type))
#define NEWADT( size ) (adt*)GDKmalloc(size)
#define _DELETE( ptr )	GDKfree(ptr)
#define _strdup( ptr )	GDKstrdup((char*)ptr)

#endif /*_MEM_H_*/
