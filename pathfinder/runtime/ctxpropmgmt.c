/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/*****************************************************************
 * ctxpropmgmt.c : C-functions for context node sequence property
 *                 management
 */

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
