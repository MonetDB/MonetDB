
#include <sql_config.h>

#include <stdlib.h>
#include <string.h>

#ifdef HAVE_MALLOC_H
#include <malloc.h>
#endif

void* 	GDKmalloc  (size_t size){
	return (void*)malloc(size);
}
void* 	GDKrealloc (void* pold, size_t size){
	return (void*)realloc((char*)pold, size);
}
void	GDKfree    (void* blk){ 
	free((char*)blk);
}
char*	GDKstrdup  (char *s){
	return strdup(s);
}
