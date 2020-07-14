#ifndef EXCEPTION_BUFFER_H
#define EXCEPTION_BUFFER_H

#include <setjmp.h>

typedef struct exception_buffer {
	jmp_buf state;
	int code;
	char *msg;
	int enabled;
} exception_buffer;

extern exception_buffer *eb_init( exception_buffer *eb );

/* != 0 on when we return to the savepoint */
#define eb_savepoint(eb) ((eb)->enabled=1,setjmp((eb)->state))
extern void eb_error( exception_buffer *eb, char *msg, int val );

#endif /* EXCEPTION_BUFFER_H */
