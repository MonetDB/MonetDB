
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

