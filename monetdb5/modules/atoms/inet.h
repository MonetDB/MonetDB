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

/*
 * @* Implementation Code
 * The first 4 bytes of the used lng are in use by the four quads of the
 * IPv4 address, stored in network order.  In the four bytes left,
 * additional information is stored.
 * Currently the fifth byte holds the number of bits from the IPv4 address
 * that should match (ie. /8, /16, /24, /32) also known as subnet mask.
 * The last byte holds whether inet atom represents the value nil or not.
 * The value nil is represented as (per byte) 0000 0001.
 *
 */
typedef struct _inet {
	unsigned char q1;
	unsigned char q2;
	unsigned char q3;
	unsigned char q4;
	unsigned char mask;
	unsigned char filler1;
	unsigned char filler2;
	unsigned char isnil;
} inet;
#define in_isnil(i) ((i)->q1 == 0 && (i)->q2 == 0 && (i)->q3 == 0 && (i)->q4 == 0 && (i)->mask == 0 && (i)->isnil != 0)
#define in_setnil(i) (i)->q1 = (i)->q2 = (i)->q3 = (i)->q4 = (i)->mask = (i)->filler1 = (i)->filler2 = 0; (i)->isnil = 1

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define inet_export extern __declspec(dllimport)
#else
#define inet_export extern __declspec(dllexport)
#endif
#else
#define inet_export extern
#endif

inet_export int INETfromString(str src, int *len, inet **retval);
inet_export int INETtoString(str *retval, int *len, inet *handle);
inet_export str INETnew(inet * retval, str *in);
inet_export str INET_isnil(bit *retval, inet * val);
inet_export str INET_comp_EQ(bit *retval, inet * val1, inet *val2);
inet_export str INET_comp_NEQ(bit *retval, inet * val1, inet *val2);
inet_export str INET_comp_LT(bit *retval, inet * val1, inet *val2);
inet_export str INET_comp_GT(bit *retval, inet * val1, inet *val2);
inet_export str INET_comp_LE(bit *retval, inet * val1, inet *val2);
inet_export str INET_comp_GE(bit *retval, inet * val1, inet *val2);
inet_export str INET_comp_CW(bit *retval, inet * val1, inet *val2);
inet_export str INET_comp_CWE(bit *retval, inet * val1, inet *val2);
inet_export str INET_comp_CS(bit *retval, inet * val1, inet *val2);
inet_export str INET_comp_CSE(bit *retval, inet * val1, inet *val2);
inet_export str INETbroadcast(inet * retval, inet *val);
inet_export str INEThost(str *retval, inet *val);
inet_export str INETmasklen(int *retval, inet *val);
inet_export str INETsetmasklen(inet *retval, inet *val, int *mask);
inet_export str INETnetmask(inet *retval, inet *val);
inet_export str INEThostmask(inet *retval, inet *val);
inet_export str INETnetwork(inet *retval, inet *val);
inet_export str INETtext(str *retval, inet *val);
inet_export str INETabbrev(str *retval, inet *val);
inet_export str INET_inet(inet *d, inet *s);
inet_export str INET_fromstr(inet *ret, str *s);

