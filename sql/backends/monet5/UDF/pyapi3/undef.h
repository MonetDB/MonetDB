/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/*
 * M. Raasveldt
 * On Windows, we have to #undef a number of variables to prevent #define
 * conflicts between MonetDB and Python.
 * This file should be included after MonetDB headers are included and before
 * Python headers are included to clean up conflicting #defines.
 */

#ifdef WIN32

#undef PREFIX
#undef EXEC_PREFIX
#undef SIZEOF_VOID_P
#undef SIZEOF_SIZE_T
#undef HAVE_FTIME
#undef snprintf
#undef vsnprintf

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION < 10
#undef ssize_t
#endif

#ifndef bool
#define bool unsigned char
#endif

#endif
