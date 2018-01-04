/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * M. Raasveldt
 * On Windows, we have to #undef a number of variables to prevent #define
 * conflicts between MonetDB and Python.
 * This file should be included after MonetDB headers are included and before
 * Python headers are included to clean up conflicting #defines.
 */

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
