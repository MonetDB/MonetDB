/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/* In your own module, replace "UDF" & "udf" by your module's name */

#ifndef _SQL_UDF_H_
#define _SQL_UDF_H_
#include "sql.h"
#include <string.h>

/* This is required as-is (except from renaming "UDF" & "udf" as suggested
 * above) for all modules for correctly exporting function on Unix-like and
 * Windows systems. */

#ifdef WIN32
#ifndef LIBUDF
#define udf_export extern __declspec(dllimport)
#else
#define udf_export extern __declspec(dllexport)
#endif
#else
#define udf_export extern
#endif

/* export MAL wrapper functions */

udf_export char * UDFreverse(char **ret, const char **arg);
udf_export char * UDFBATreverse(bat *ret, const bat *arg);

/* using C macro for convenient type-expansion */
#define UDFfuse_scalar_decl(in,out) \
        udf_export char * UDFfuse_##in##_##out(out *ret, const in *one, const in *two)
UDFfuse_scalar_decl(bte, sht);
UDFfuse_scalar_decl(sht, int);
UDFfuse_scalar_decl(int, lng);
#ifdef HAVE_HGE
UDFfuse_scalar_decl(lng, hge);
#endif

udf_export char * UDFBATfuse(bat *ret, const bat *one, const bat *two);

#endif /* _SQL_UDF_H_ */
