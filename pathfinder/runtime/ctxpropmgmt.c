/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * C-functions for context node sequence property management
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

#include "ctxpropmgmt.h"

int
PFbatcacheid (oid *res, BAT *b)
{
    *res=b->batCacheid;
    return GDK_SUCCEED;
}

ctxprop CTXPROPnil = { 0 };

ctxprop*
CTXPROPnull(void)
{
    return &CTXPROPnil;
}

int
CTXPROPto_str (str *tostr, int *len, ctxprop *p)
{
    int maxlen = 12 + 1 * 12;

    if (*len < maxlen)
    {
        if (*tostr) GDKfree(*tostr);
        *tostr = GDKmalloc(*len=maxlen);
    }
    sprintf (*tostr,"ctxprop(withpost=%d)", (int)p->withpost);
    return strlen (*tostr);
}

int
CTXPROPfrom_str (str fromstr, int *len, ctxprop **p)
{
    int a, nchars;

    if (*len < sizeof (ctxprop)) {
        if (*p) GDKfree (*p);
        *p=GDKmalloc (*len = sizeof (ctxprop));
    }
    if (sscanf (fromstr, "ctxprop(withpost=%d)%n", &a, &nchars) != 2)
        return GDK_FAIL;
    else
    {
        a = (a > 0 ? 1 : 0);
        (*p)->withpost = a;
    }
    printf ("CTXPROPfrom_str: I read %n args (%d)\n", nchars, a);
    return nchars;
}

int
CTXPROPget_withpost(bit *ret, ctxprop* p)
{
    *ret = p->withpost;
    return GDK_SUCCEED;
}

int
CTXPROPset_withpost(ctxprop *p, bit *wp)
{
    p->withpost = *wp;
    return GDK_SUCCEED;
}

/* vim:set shiftwidth=4 expandtab: */
