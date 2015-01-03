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
 * Copyright August 2008-2015 MonetDB B.V.
 * All Rights Reserved.
 */

#define CANDINIT(b, s, start, end, cnt, cand, candend)			\
	do {								\
		start = 0;						\
		end = cnt = BATcount(b);				\
		cand = candend = NULL;					\
		if (s) {						\
			assert(BATttype(s) == TYPE_oid);		\
			if (BATcount(s) == 0) {				\
				start = end = 0;			\
			} else {					\
				if (BATtdense(s)) {			\
					start = (s)->T->seq;		\
					end = start + BATcount(s);	\
				} else {				\
					oid x = (b)->H->seq;		\
					start = SORTfndfirst((s), &x);	\
					x += BATcount(b);		\
					end = SORTfndfirst((s), &x);	\
					cand = (const oid *) Tloc((s), start); \
					candend = (const oid *) Tloc((s), end); \
					if (cand == candend) {		\
						start = end = 0;	\
					} else {			\
						assert(cand < candend);	\
						start = *cand;		\
						end = candend[-1] + 1;	\
					}				\
				}					\
				assert(start <= end);			\
				if (start <= (b)->H->seq)		\
					start = 0;			\
				else if (start >= (b)->H->seq + cnt)	\
					start = cnt;			\
				else					\
					start -= (b)->H->seq;		\
				if (end >= (b)->H->seq + cnt)		\
					end = cnt;			\
				else if (end <= (b)->H->seq)		\
					end = 0;			\
				else					\
					end -= (b)->H->seq;		\
			}						\
		}							\
	} while (0)
