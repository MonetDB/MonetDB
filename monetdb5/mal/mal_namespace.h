/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _MAL_NAMESPACE_H
#define _MAL_NAMESPACE_H

mal_export void initNamespace(void);
mal_export void finishNamespace(void);
mal_export str putName(str nme, size_t len);
mal_export str getName(str nme, size_t len);
mal_export void delName(str nme, size_t len);
mal_export void dumpNamespaceStatistics(stream *f, int details);

#define MAXIDENTLEN    1024

#endif /* _MAL_NAMESPACE_H */
