#ifndef _GDK_H_
#define _GDK_H_

#include <unistd.h>

extern void* 	GDKmalloc  (size_t size); 
extern void* 	GDKrealloc (void* pold, size_t size); 
extern void	GDKfree    (void* blk); 
extern char*	GDKstrdup  (char* s);

#endif
