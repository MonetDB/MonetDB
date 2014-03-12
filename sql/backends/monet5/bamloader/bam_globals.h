#ifndef _BAM_LOG_H
#define _BAM_LOG_H

#define BAM_DEBUG /* TODO Use global macro */

/* Macro that enables writing to a log. If the debug flag is not set, it does not do anything */
#ifdef BAM_DEBUG
#define TO_LOG(...) { \
    fprintf(stderr, __VA_ARGS__); \
    fprintf(stderr, "\n"); \
}
#else
#define TO_LOG(...) (void)0
#endif

/* Macro to create an exception that uses a previous allocated exception as format parameter. Makes sure that the old one is freed and that
 * msg points to the new exception afterwards.
 * I had a look at rethrow but this does not achieve the right thing.
 */
#define REUSE_EXCEPTION(msg, type, fnc, ...) { \
    str msg_tmp = createException(type, fnc, __VA_ARGS__); \
    GDKfree(msg); \
    msg = msg_tmp; \
}

#endif
