#include <stdlib.h>
#include <string.h>

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
