/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _MAL_NAMESPACE_H
#define _MAL_NAMESPACE_H

mal_export void initNamespace(void);
mal_export void finishNamespace(void);
mal_export str putName(const char *nme);
mal_export str putNameLen(const char *nme, size_t len);
mal_export str getName(const char *nme);
mal_export str getNameLen(const char *nme, size_t len);
mal_export void delName(const char *nme, size_t len);

#define MAXIDENTLEN    1024

#endif /* _MAL_NAMESPACE_H */
