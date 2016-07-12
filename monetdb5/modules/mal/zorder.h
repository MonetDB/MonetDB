/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _ZORDER_H
#define _ZORDER_H


mal_export str ZORDencode_int_oid(oid *z, int *x, int *y);
mal_export str ZORDbatencode_int_oid(bat *z, bat *x, bat *y);
mal_export str ZORDdecode_int_oid(int *x, int *y, oid *z);
mal_export str ZORDdecode_int_oid_x(int *x, oid *z);
mal_export str ZORDdecode_int_oid_y(int *y, oid *z);
mal_export str ZORDbatdecode_int_oid(bat *x, bat *y, bat *z);
mal_export str ZORDbatdecode_int_oid_x(bat *x, bat *z);
mal_export str ZORDbatdecode_int_oid_y(bat *y, bat *z);
mal_export str ZORDslice_int(bat *r, int *xb, int *yb, int *xt, int *yt);

#endif /* _ZORDER_H */
