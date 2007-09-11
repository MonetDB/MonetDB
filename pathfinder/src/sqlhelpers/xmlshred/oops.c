#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "oops.h"
#include <assert.h>

#define PUFFER_SIZE 1024

/* worker for SHoops */
static void
SHoops_ (int err, const char *fmt, va_list az) 
{
    assert (fmt);

    /* error msg from errno */
    int errmsg = errno;
        char *emsg;
    char puffer[PUFFER_SIZE+1];

    vsnprintf (puffer, PUFFER_SIZE, fmt, az);
    if (err) {
        emsg = strerror (errmsg);
        snprintf (puffer+strlen(puffer), 
                  PUFFER_SIZE - strlen (emsg), 
                  ": %s", emsg); 
    }
    fflush(stdout); /* if stdout and stderr are equal */
    /* write everything in the puffer to stderr */
    fprintf (stderr, "%s\n", puffer);
    fflush (NULL);
    return;
}

/* Global error-message routine */
void
SHoops (err_t err, const char *fmt, ...)
{
    assert (fmt);

    va_list az;
        
    va_start (az, fmt);
    switch (err) {
        case SH_WARNING:
        case SH_FATAL:
            SHoops_ (0, fmt, az);
            break;
        case SH_DUMP:
            SHoops_ (1, fmt, az);
            break;
        default:
            SHoops_ (1, "This err_msg is unknown", az);
            exit (3);
    }
    va_end (az);

    if (err == SH_WARNING)
        return;
    else if (err == SH_DUMP)
        abort ();
    exit (1);
}
