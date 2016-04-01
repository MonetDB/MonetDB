

#ifdef WIN32

#undef HAVE_IO_H
#undef HAVE_SYS_UTIME_H
#undef HAVE_STRFTIME
#undef PREFIX
#undef EXEC_PREFIX
#undef SIZEOF_VOID_P
#undef SIZEOF_SIZE_T
#undef HAVE_PUTENV
#undef HAVE_FTIME
#undef snprintf
#undef vsnprintf

#undef ssize_t

#ifndef bool
#define bool unsigned char
#endif

#endif
