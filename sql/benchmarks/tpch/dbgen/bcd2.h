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
 *  Sccsid:     @(#)bcd2.h	2.1.8.1
 */
int bin_bcd2(long binary, long *low_res, long *high_res);
int bcd2_bin(long *dest, long bcd);
int bcd2_add(long *bcd_low, long *bcd_high, long addend);
int bcd2_sub(long *bcd_low, long *bcd_high, long subend);
int bcd2_mul(long *bcd_low, long *bcd_high, long multiplier);
int bcd2_div(long *bcd_low, long *bcd_high, long divisor);
long bcd2_mod(long *bcd_low, long *bcd_high, long modulo);
long bcd2_cmp(long *bcd_low, long *bcd_high, long compare);
