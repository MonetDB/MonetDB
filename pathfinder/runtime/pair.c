/**
 * @file
 *
 *
 * Copyright Notice:
 * -----------------
 *
 *  The contents of this file are subject to the MonetDB Public
 *  License Version 1.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 *
 *  The Original Code is the ``Pathfinder'' system. The Initial
 *  Developer of the Original Code is the Database & Information
 *  Systems Group at the University of Konstanz, Germany. Portions
 *  created by U Konstanz are Copyright (C) 2000-2003 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

#include "pair.h"

pair PAIRnil = { NULL, NULL };

pair* PAIRnull()
{
	return &PAIRnil;
}

int PAIRto_str(str *tostr, int* len, pair* p)
{
	int maxlen=32;

	if (*len < maxlen)
	{
		if (*tostr) GDKfree(*tostr);
		*tostr = GDKmalloc(*len=maxlen);
	}
	sprintf(*tostr,"pair(%p,%p)",p->fst,p->snd);
	return strlen(*tostr);
}

int PAIRnew(pair *ret, BAT* fst, BAT* snd)
{
  	if (ret->fst != NULL) { BBPunfix(BBPcacheid(ret->fst)); }
	if (ret->snd != NULL) { BBPunfix(BBPcacheid(ret->snd)); }
	if (fst != NULL) BBPfix(BBPcacheid(fst));
	if (snd != NULL) BBPfix(BBPcacheid(snd));

	ret->fst=fst;
	ret->snd=snd;

	return GDK_SUCCEED;
}

int PAIRclr(pair* p)
{
   if (p->fst != NULL) BBPunfix(BBPcacheid(p->fst));
   if (p->snd != NULL) BBPunfix(BBPcacheid(p->snd));
	p->fst=p->snd=NULL;
	return GDK_SUCCEED;
}

int PAIRfst(BAT **ret, pair* p)
{
	*ret=p->fst;
   if (p->fst != NULL) BBPfix(BBPcacheid(p->fst));
	return GDK_SUCCEED;
}

int PAIRsnd(BAT **ret, pair* p)
{
	*ret=p->snd;
  	if (p->snd != NULL) BBPfix(BBPcacheid(p->snd));
	return GDK_SUCCEED;
}

/* vim:set shiftwidth=4 expandtab: */
