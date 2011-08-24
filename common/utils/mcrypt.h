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

#ifndef _SEEN_MCRYPT_H
#define _SEEN_MCRYPT_H 1

char *mcrypt_getHashAlgorithms(void);
char *mcrypt_MD5Sum(const char *string, const int len);
char *mcrypt_SHA1Sum(const char *string, const int len);
char *mcrypt_SHA224Sum(const char *string, const int len);
char *mcrypt_SHA256Sum(const char *string, const int len);
char *mcrypt_SHA384Sum(const char *string, const int len);
char *mcrypt_SHA512Sum(const char *string, const int len);
char *mcrypt_RIPEMD160Sum(const char *string, const int len);
char *mcrypt_BackendSum(const char *string, const int len);
char *mcrypt_hashPassword(const char *algo, const char *password, const char *challenge);

#endif
