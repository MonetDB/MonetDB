#ifndef _MEM_H_ 
#define _MEM_H_ 

#include <sql_config.h>

#include <stdio.h>
#include <assert.h>

#ifdef HAVE_MALLOC_H 
#include <malloc.h>
#endif

#define NEW( type ) (type*)malloc(sizeof(type) )
#define NEW_ARRAY( type, size ) (type*)malloc((size)*sizeof(type))
#define RENEW_ARRAY( type,ptr,size) (type*)realloc((void*)ptr,(size)*sizeof(type))
#define NEWADT( size ) (adt*)malloc(size)
#define _DELETE( ptr )	free(ptr)
#define _strdup( ptr )	strdup((char*)ptr)

#endif /*_MEM_H_*/
