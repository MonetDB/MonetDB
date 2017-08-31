/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (c) Martin Kersten, Sjoerd Mullender
 * Series generating module for integer, decimal, real, double and timestamps.
 */

#include "monetdb_config.h"
#include "opt_prelude.h"
#include "algebra.h"
#include "generator.h"
#include "mtime.h"
#include "math.h"


#define IDENTITY(x)	(x)

/*
 * The noop simply means that we keep the properties for the generator object.
 */
#define VLTnoop(TPE)\
		{	TPE s;\
			s = pci->argc == 3 ? 1: *getArgReference_##TPE(stk,pci, 3);\
			zeroerror = (s == 0);\
			nullerr = (s == TPE##_nil);\
		}
str
VLTgenerator_noop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int nullerr= 0, zeroerror=0, tpe;
	(void) cntxt;

	switch( tpe = getArgType(mb,pci,1)){
	case TYPE_bte: VLTnoop(bte); break;
	case TYPE_sht: VLTnoop(sht); break;
	case TYPE_int: VLTnoop(int); break;
	case TYPE_lng: VLTnoop(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: VLTnoop(hge); break;
#endif
	case TYPE_flt: VLTnoop(flt); break;
	case TYPE_dbl: VLTnoop(dbl); break;
	default:
		if (tpe == TYPE_timestamp){
			/* with timestamp, step is of SQL type "interval seconds",
			 * i.e., MAL / C type "lng" */
			 VLTnoop(lng);
		} else throw(MAL,"generator.noop","unknown data type %d", getArgType(mb,pci,1));
	}
	if( zeroerror)
		throw(MAL,"generator.noop","zero step size not allowed");
	if( nullerr)
		throw(MAL,"generator.noop","null step size not allowed");
	return MAL_SUCCEED;
}

/*
 * The base line consists of materializing the generator iterator value
 */
#define VLTmaterialize(TPE)						\
	do {								\
		TPE *v, f, l, s;					\
		f = *getArgReference_##TPE(stk, pci, 1);		\
		l = *getArgReference_##TPE(stk, pci, 2);		\
		if ( pci->argc == 3)					\
			s = f<l? (TPE) 1: (TPE)-1;			\
		else s =  *getArgReference_##TPE(stk,pci, 3);		\
		if (s == 0 || (s > 0 && f > l) || (s < 0 && f < l) || f == TPE##_nil || l == TPE##_nil)	\
			throw(MAL, "generator.table",			\
			      "Illegal generator range");		\
		n = (BUN) ((l - f) / s);				\
		if ((TPE) (n * s + f) != l)				\
			n++;						\
		bn = COLnew(0, TYPE_##TPE, n, TRANSIENT);		\
		if (bn == NULL)						\
			throw(MAL, "generator.table", MAL_MALLOC_FAIL);	\
		v = (TPE*) Tloc(bn, 0);					\
		for (c = 0; c < n; c++)					\
			*v++ = (TPE) (f + c * s);			\
		bn->tsorted = s > 0 || n <= 1;				\
		bn->trevsorted = s < 0 || n <= 1;			\
	} while (0)

static str
VLTgenerator_table_(BAT **result, Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BUN c, n;
	BAT *bn;
	str msg;
	int tpe;
	(void) cntxt;

	*result = NULL;
	tpe = getArgType(mb, pci, 1);
	switch (tpe) {
	case TYPE_bte:
		VLTmaterialize(bte);
		break;
	case TYPE_sht:
		VLTmaterialize(sht);
		break;
	case TYPE_int:
		VLTmaterialize(int);
		break;
	case TYPE_lng:
		VLTmaterialize(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		VLTmaterialize(hge);
		break;
#endif
	case TYPE_flt:
		VLTmaterialize(flt);
		break;
	case TYPE_dbl:
		VLTmaterialize(dbl);
		break;
	default:
		if (tpe == TYPE_timestamp) {
			timestamp *v,f,l;
			lng s;
			ValRecord ret;
			if (VARcalccmp(&ret, &stk->stk[pci->argv[1]],
				       &stk->stk[pci->argv[2]]) != GDK_SUCCEED)
				throw(MAL, "generator.table",
				      "Illegal generator expression range");
			f = *getArgReference_TYPE(stk, pci, 1, timestamp);
			l = *getArgReference_TYPE(stk, pci, 2, timestamp);
			if ( pci->argc == 3) 
					throw(MAL,"generator.table","Timestamp step missing");
			s = *getArgReference_lng(stk, pci, 3);
			if (s == 0 ||
			    (s > 0 && ret.val.btval > 0) ||
			    (s < 0 && ret.val.btval < 0) ||
				timestamp_isnil(f) || timestamp_isnil(l))
				throw(MAL, "generator.table",
				      "Illegal generator range");
			/* casting one value to lng causes the whole
			 * computation to be done as lng, reducing the
			 * risk of overflow */
			n = (BUN) ((((lng) l.days - f.days) * 24*60*60*1000 + l.msecs - f.msecs) / s);
			bn = COLnew(0, TYPE_timestamp, n + 1, TRANSIENT);
			if (bn == NULL)
				throw(MAL, "generator.table", MAL_MALLOC_FAIL);
			v = (timestamp *) Tloc(bn, 0);
			for (c = 0; c < n; c++) {
				*v++ = f;
				msg = MTIMEtimestamp_add(&f, &f, &s);
				if (msg != MAL_SUCCEED) {
					BBPreclaim(bn);
					return msg;
				}
			}
			if (f.days != l.days || f.msecs != l.msecs) {
				*v++ = f;
				n++;
			}
			bn->tsorted = s > 0 || n <= 1;
			bn->trevsorted = s < 0 || n <= 1;
		} else {
			throw(MAL, "generator.table", "unsupported type");
		}
		break;
	}
	BATsetcount(bn, c);
	bn->tkey = 1;
	bn->tnil = 0;
	bn->tnonil = 1;
	*result = bn;
	return MAL_SUCCEED;
}

str
VLTgenerator_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg;
	BAT *bn = NULL;

	if ((msg = VLTgenerator_noop(cntxt, mb, stk, pci)) != MAL_SUCCEED)
		return msg;

	msg =  VLTgenerator_table_(&bn, cntxt, mb, stk, pci);
	if( msg == MAL_SUCCEED){
		*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
		BBPkeepref(bn->batCacheid);
	}
	return msg;
}

/*
 * Selection over the generator table does not require a materialization of the table
 * An optimizer can replace the subselect directly into a generator specific one.
 * The target to look for is generator.series(A1,A2,A3)
 * We need the generator parameters, which are injected to replace the target column
 */
static InstrPtr
findGeneratorDefinition(MalBlkPtr mb, InstrPtr pci, int target)
{
	InstrPtr q, p = NULL;
	int i;

	for (i = 1; i < mb->stop; i++) {
		q = getInstrPtr(mb, i);
		if (q->argv[0] == target && getModuleId(q) == generatorRef && (getFunctionId(q) == parametersRef || getFunctionId(q) == seriesRef))
			p = q;
		if (q == pci)
			return p;
	}
	return p;
}

#define calculate_range(TPE, TPE2)					\
	do {								\
		TPE f, l, s, low, hgh;					\
									\
		f = * getArgReference_##TPE(stk, p, 1);		\
		l = * getArgReference_##TPE(stk, p, 2);		\
		if ( p->argc == 3) \
			s = f<l? (TPE) 1: (TPE)-1;\
		else s = * getArgReference_##TPE(stk, p, 3); \
		if (s == 0 || (s > 0 && f > l) || (s < 0 && f < l) || f == TPE##_nil || l == TPE##_nil)	\
			throw(MAL, "generator.subselect",		\
			      "Illegal generator range");		\
		n = (BUN) (((TPE2) l - (TPE2) f) / (TPE2) s);		\
		if ((TPE)(n * s + f) != l)				\
			n++;						\
									\
		low = * getArgReference_##TPE(stk, pci, i);		\
		hgh = * getArgReference_##TPE(stk, pci, i + 1);	\
									\
		if (low == hgh && low != TPE##_nil) 			\
			hi = li;					\
		if (low == TPE##_nil && hgh == TPE##_nil) {		\
			if (li && hi && !anti) {			\
				/* match NILs (of which there aren't */	\
				/* any) */				\
				o1 = o2 = 0;				\
			} else {					\
				/* match all non-NIL values, */		\
				/* i.e. everything */			\
				o1 = 0;					\
				o2 = (oid) n;				\
			}						\
		} else if (s > 0) {					\
			if (low == TPE##_nil || low < f)		\
				o1 = 0;					\
			else {						\
				o1 = (oid) (((TPE2) low - (TPE2) f) / (TPE2) s); \
				if ((TPE) (f + o1 * s) < low ||			\
				    (!li && (TPE) (f + o1 * s) == low))		\
					o1++;				\
			}						\
			if (hgh == TPE##_nil)				\
				o2 = (oid) n;				\
			else if (hgh < f)				\
				o2 = 0;					\
			else {						\
				o2 = (oid) (((TPE2) hgh - (TPE2) f) / (TPE2) s); \
				if ((hi && (TPE) (f + o2 * s) == hgh) ||	\
				    (TPE) (f + o2 * s) < hgh)			\
					o2++;				\
			}						\
		} else {						\
			if (low == TPE##_nil)				\
				o2 = (oid) n;				\
			else if (low > f)				\
				o2 = 0;					\
			else {						\
				o2 = (oid) (((TPE2) low - (TPE2) f) / (TPE2) s); \
				if ((li && (TPE) (f + o2 * s) == low) ||	\
				    (TPE) (f + o2 * s) > low)			\
					o2++;				\
			}						\
			if (hgh == TPE##_nil || hgh > f)		\
				o1 = 0;					\
			else {						\
				o1 = (oid) (((TPE2) hgh - (TPE2) f) / (TPE2) s); \
				if ((!hi && (TPE) (f + o1 * s) == hgh) ||	\
				    (TPE) (f + o1 * s) > hgh)			\
					o1++;				\
			}						\
		}							\
	} while (0)

str
VLTgenerator_subselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit li, hi, anti;
	int i;
	oid o1, o2;
	BUN n = 0;
	oid *cl = 0;
	BUN c;
	BAT *bn, *cand = NULL;
	InstrPtr p;
	str msg = MAL_SUCCEED;
	int tpe;

	(void) cntxt;
	p = findGeneratorDefinition(mb, pci, pci->argv[1]);
	if (p == NULL)
		throw(MAL, "generator.subselect",
		      "Could not locate definition for object");

	if (pci->argc == 8) {	/* candidate list included */
		bat candid = *getArgReference_bat(stk, pci, 2);
		if (candid) {
			cand = BATdescriptor(candid);
			if (cand == NULL)
				throw(MAL, "generator.subselect",
				      RUNTIME_OBJECT_MISSING);
			cl = (oid *) Tloc(cand, 0);
		}
		i = 3;
	} else
		i = 2;

	li = * getArgReference_bit(stk, pci, i + 2);
	hi = * getArgReference_bit(stk, pci, i + 3);
	anti = * getArgReference_bit(stk, pci, i + 4);

	switch ( tpe = getArgType(mb, pci, i)) {
	case TYPE_bte: calculate_range(bte, sht); break;
	case TYPE_sht: calculate_range(sht, int); break;
	case TYPE_int: calculate_range(int, lng); break;
#ifndef HAVE_HGE
	case TYPE_lng: calculate_range(lng, lng); break;
#else
	case TYPE_lng: calculate_range(lng, hge); break;
	case TYPE_hge: calculate_range(hge, hge); break;
#endif
	case TYPE_flt: calculate_range(flt, dbl); break;
	case TYPE_dbl: calculate_range(dbl, dbl); break;
	default:
		if(  tpe == TYPE_timestamp){
			timestamp tsf,tsl;
			timestamp tlow,thgh;
			lng tss;
			lng one = 1;
			oid *ol;

			tsf = *getArgReference_TYPE(stk, p, 1, timestamp);
			tsl = *getArgReference_TYPE(stk, p, 2, timestamp);
			if ( p->argc == 3) 
					throw(MAL,"generator.table","Timestamp step missing");
			tss = *getArgReference_lng(stk, p, 3);
			if ( tss == 0 || 
				timestamp_isnil(tsf) || timestamp_isnil(tsl) ||
				 (tss > 0 && (tsf.days > tsl.days || (tsf.days == tsl.days && tsf.msecs > tsl.msecs) )) ||
				 (tss < 0 && (tsf.days < tsl.days || (tsf.days == tsl.days && tsf.msecs < tsl.msecs) )) 
				)
				throw(MAL, "generator.subselect", "Illegal generator range");

			tlow = *getArgReference_TYPE(stk,pci,i, timestamp);
			thgh = *getArgReference_TYPE(stk,pci,i+1, timestamp);

			if (tlow.msecs == thgh.msecs &&
			    tlow.days == thgh.days &&
			    !timestamp_isnil(tlow))
				hi = li;
			if( hi && !timestamp_isnil(thgh) &&
			    (msg = MTIMEtimestamp_add(&thgh, &thgh, &one)) != MAL_SUCCEED)
				return msg;
			if( !li && !timestamp_isnil(tlow) &&
			    (msg = MTIMEtimestamp_add(&tlow, &tlow, &one)) != MAL_SUCCEED)
				return msg;

			/* casting one value to lng causes the whole
			 * computation to be done as lng, reducing the
			 * risk of overflow */
			o2 = (BUN) ((((lng) tsl.days - tsf.days) * 24*60*60*1000 + tsl.msecs - tsf.msecs) / tss);
			bn = COLnew(0, TYPE_oid, o2 + 1, TRANSIENT);
			if (bn == NULL)
				throw(MAL, "generator.subselect", MAL_MALLOC_FAIL);

			// simply enumerate the sequence and filter it by predicate and candidate list
			ol = (oid *) Tloc(bn, 0);
			for (c=0, o1=0; o1 <= o2; o1++) {
				if( (((tsf.days>tlow.days || (tsf.days== tlow.days && tsf.msecs >= tlow.msecs) || timestamp_isnil(tlow))) &&
				    ((tsf.days<thgh.days || (tsf.days== thgh.days && tsf.msecs < thgh.msecs))  || timestamp_isnil(thgh)) ) != anti ){
					/* could be improved when no candidate list is available into a void/void BAT */
					if( cl){
						while ( c < BATcount(cand) && *cl < o1 ) {cl++; c++;}
						if( *cl == o1){
							*ol++ = o1;
							cl++;
							n++;
							c++;
						}
					} else{
						*ol++ = o1;
						n++;
					}
				}
				msg = MTIMEtimestamp_add(&tsf, &tsf, &tss);
				if (msg != MAL_SUCCEED) {
					BBPreclaim(bn);
					return msg;
				}
			}
			BATsetcount(bn, (BUN) n);
			bn->tsorted = 1;
			bn->trevsorted = BATcount(bn) <= 1;
			bn->tkey = 1;
			bn->tnil = 0;
			bn->tnonil = 1;
			* getArgReference_bat(stk, pci, 0) = bn->batCacheid;
			BBPkeepref(bn->batCacheid);
			return MAL_SUCCEED;
		} else
			throw(MAL, "generator.subselect", "Unsupported type in subselect");
	}
	if (o1 > (oid) n)
		o1 = (oid) n;
	if (o2 > (oid) n)
		o2 = (oid) n;
	assert(o1 <= o2);
	assert(o2 - o1 <= (oid) n);
	if (anti && o1 == o2) {
		o1 = 0;
		o2 = (oid) n;
		anti = 0;
	}
	if (cand) {
		oid o;
		o = o1;
		o1 = SORTfndfirst(cand, &o);
		o = o2;
		o2 = SORTfndfirst(cand, &o);
		n = BATcount(cand);
		if (anti && o1 < o2) {
			bn = COLnew(0, TYPE_oid, n - (o2 - o1), TRANSIENT);
			if (bn) {
				oid *op = (oid *) Tloc(bn, 0);
				const oid *cp = (const oid *) Tloc(cand, 0);
				BATsetcount(bn, n - (o2 - o1));
				bn->tnil = 0;
				bn->tnonil = 1;
				bn->tsorted = 1;
				bn->trevsorted = BATcount(bn) <= 1;
				bn->tkey = 1;
				for (o = 0; o < o1; o++)
					*op++ = cp[o];
				for (o = o2; o < (oid) n; o++)
					*op++ = cp[o];
			}
		} else {
			if (anti) {
				o1 = 0;
				o2 = (oid) n;
			}
			bn = BATslice(cand, (BUN) o1, (BUN) o2);
		}
		BBPunfix(cand->batCacheid);
		if (bn == NULL)
			throw(MAL, "generator.subselect",
			      MAL_MALLOC_FAIL);
	} else {
		if (anti) {
			oid o;
			oid *op;

			bn = COLnew(0, TYPE_oid, n - (o2 - o1), TRANSIENT);
			if (bn == NULL)
				throw(MAL, "generator.subselect",
				      MAL_MALLOC_FAIL);
			BATsetcount(bn, n - (o2 - o1));
			op = (oid *) Tloc(bn, 0);
			for (o = 0; o < o1; o++)
				*op++ = o;
			for (o = o2; o < (oid) n; o++)
				*op++ = o;
			bn->tnil = 0;
			bn->tnonil = 1;
			bn->tsorted = 1;
			bn->trevsorted = BATcount(bn) <= 1;
			bn->tkey = 1;
		} else {
			bn = BATdense(0, o1, (BUN) (o2 - o1));
			if (bn == NULL)
				throw(MAL, "generator.subselect",
				      MAL_MALLOC_FAIL);
		}
	}
	* getArgReference_bat(stk, pci, 0) = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}
#ifndef HAVE_NEXTAFTERF
#define nextafter   _nextafter
#include "mutils.h"		/* nextafterf */
#endif

#define PREVVALUEbte(x) ((x) - 1)
#define PREVVALUEsht(x) ((x) - 1)
#define PREVVALUEint(x) ((x) - 1)
#define PREVVALUElng(x) ((x) - 1)
#ifdef HAVE_HGE
#define PREVVALUEhge(x) ((x) - 1)
#endif
#define PREVVALUEoid(x) ((x) - 1)
#define PREVVALUEflt(x) nextafterf((x), -GDK_flt_max)
#define PREVVALUEdbl(x) nextafter((x), -GDK_dbl_max)

#define NEXTVALUEbte(x) ((x) + 1)
#define NEXTVALUEsht(x) ((x) + 1)
#define NEXTVALUEint(x) ((x) + 1)
#define NEXTVALUElng(x) ((x) + 1)
#ifdef HAVE_HGE
#define NEXTVALUEhge(x) ((x) + 1)
#endif
#define NEXTVALUEoid(x) ((x) + 1)
#define NEXTVALUEflt(x) nextafterf((x), GDK_flt_max)
#define NEXTVALUEdbl(x) nextafter((x), GDK_dbl_max)

#define HGE_ABS(a) (((a) < 0) ? -(a) : (a))

#define VLTthetasubselect(TPE,ABS) {\
	TPE f,l,s, low, hgh;\
	BUN j; oid *v;\
	f = *getArgReference_##TPE(stk,p, 1);\
	l = *getArgReference_##TPE(stk,p, 2);\
	if ( p->argc == 3) \
		s = f<l? (TPE) 1: (TPE)-1;\
	else s =  *getArgReference_##TPE(stk,p, 3);\
	if( s == 0 || (f<l && s < 0) || (f>l && s> 0)) \
		throw(MAL,"generator.thetasubselect","Illegal range");\
	cap = (BUN)(ABS(l-f)/ABS(s));\
	bn = COLnew(0, TYPE_oid, cap, TRANSIENT);\
	if( bn == NULL)\
		throw(MAL,"generator.thetasubselect",MAL_MALLOC_FAIL);\
	low= hgh = TPE##_nil;\
	v = (oid*) Tloc(bn,0);\
	if ( strcmp(oper,"<") == 0){\
		hgh= *getArgReference_##TPE(stk,pci,idx);\
		hgh = PREVVALUE##TPE(hgh);\
	} else\
	if ( strcmp(oper,"<=") == 0){\
		hgh= *getArgReference_##TPE(stk,pci,idx);\
	} else\
	if ( strcmp(oper,">") == 0){\
		low= *getArgReference_##TPE(stk,pci,idx);\
		low = NEXTVALUE##TPE(low);\
	} else\
	if ( strcmp(oper,">=") == 0){\
		low= *getArgReference_##TPE(stk,pci,idx);\
	} else\
	if ( strcmp(oper,"!=") == 0 || strcmp(oper, "<>") == 0){\
		hgh= low= *getArgReference_##TPE(stk,pci,idx);\
		anti = 1;\
	} else\
	if ( strcmp(oper,"==") == 0 || strcmp(oper, "=") == 0){\
		hgh= low= *getArgReference_##TPE(stk,pci,idx);\
	} else\
		throw(MAL,"generator.thetasubselect","Unknown operator");\
	if(cand){ cn = BATcount(cand); if( cl == 0) oc = cand->tseqbase; }\
	for(j=0;j<cap;j++, f+=s, o++)\
		if( ((low == TPE##_nil || f >= low) && (f <= hgh || hgh == TPE##_nil)) != anti){\
			if(cand){ \
				if( cl){ while(cn-- >= 0 && *cl < o) cl++; if ( *cl == o){ *v++= o; c++;}} \
				else { while(cn-- >= 0 && oc < o) oc++; if ( oc == o){ *v++= o; c++;} }\
			} else {*v++ = o; c++;}\
		} \
}


str VLTgenerator_thetasubselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx, c= 0, anti =0,tpe;
	bat cndid =0;
	BAT *cand = 0, *bn = NULL;
	BUN cap,j;
	lng cn= 0;
	oid o = 0, oc = 0,  *cl = 0;
	InstrPtr p;
	str oper, msg= MAL_SUCCEED;

	(void) cntxt;
	p = findGeneratorDefinition(mb,pci,pci->argv[1]);
	if( p == NULL)
		throw(MAL,"generator.thetasubselect","Could not locate definition for object");

	if( pci->argc == 5){ // candidate list included
		cndid = *getArgReference_bat(stk,pci, 2);
		if( cndid != bat_nil){
			cand = BATdescriptor(cndid);
			if( cand == NULL)
				throw(MAL,"generator.subselect",RUNTIME_OBJECT_MISSING);
			cl = (oid*) Tloc(cand,0);
		} 
		idx = 3;
	} else idx = 2;
	oper= *getArgReference_str(stk,pci,idx+1);

	// check the step direction
	
	switch( tpe =getArgType(mb,pci,idx)){
	case TYPE_bte: VLTthetasubselect(bte,abs);break;
	case TYPE_sht: VLTthetasubselect(sht,abs);break;
	case TYPE_int: VLTthetasubselect(int,abs);break;
	case TYPE_lng: VLTthetasubselect(lng,llabs);break;
#ifdef HAVE_HGE
	case TYPE_hge: VLTthetasubselect(hge,HGE_ABS);break;
#endif
	case TYPE_flt: VLTthetasubselect(flt,fabsf);break;
	case TYPE_dbl: VLTthetasubselect(dbl,fabs);break;
	break;
	default:
		if ( tpe == TYPE_timestamp){
			timestamp f,l, val, low, hgh;
			lng  s;
			oid *v;

			f = *getArgReference_TYPE(stk,p, 1, timestamp);
			l = *getArgReference_TYPE(stk,p, 2, timestamp);
			if ( p->argc == 3) {
				if (cand)
					BBPunfix(cand->batCacheid);
				throw(MAL,"generator.table","Timestamp step missing");
			}
			s = *getArgReference_lng(stk,p, 3);
			if ( s == 0 || 
				 (s > 0 && (f.days > l.days || (f.days == l.days && f.msecs > l.msecs) )) ||
				 (s < 0 && (f.days < l.days || (f.days == l.days && f.msecs < l.msecs) )) 
				) {
				if (cand)
					BBPunfix(cand->batCacheid);
				throw(MAL, "generator.subselect", "Illegal generator range");
			}

			hgh = low = *timestamp_nil;
			if ( strcmp(oper,"<") == 0){
				lng minone = -1;
				hgh= *getArgReference_TYPE(stk,pci,idx, timestamp);
				if ((msg = MTIMEtimestamp_add(&hgh, &hgh, &minone)) != MAL_SUCCEED) {
					if (cand)
						BBPunfix(cand->batCacheid);
					return msg;
				}
			} else
			if ( strcmp(oper,"<=") == 0){
				hgh= *getArgReference_TYPE(stk,pci,idx, timestamp) ;
			} else
			if ( strcmp(oper,">") == 0){
				lng one = 1;
				low= *getArgReference_TYPE(stk,pci,idx, timestamp);
				if ((msg = MTIMEtimestamp_add(&hgh, &hgh, &one)) != MAL_SUCCEED) {
					if (cand)
						BBPunfix(cand->batCacheid);
					return msg;
				}
			} else
			if ( strcmp(oper,">=") == 0){
				low= *getArgReference_TYPE(stk,pci,idx, timestamp);
			} else
			if ( strcmp(oper,"!=") == 0 || strcmp(oper, "<>") == 0){
				hgh= low= *getArgReference_TYPE(stk,pci,idx, timestamp);
				anti = 1;
			} else
			if ( strcmp(oper,"==") == 0 || strcmp(oper, "=") == 0){
				hgh= low= *getArgReference_TYPE(stk,pci,idx, timestamp);
			} else {
				if (cand)
					BBPunfix(cand->batCacheid);
				throw(MAL,"generator.thetasubselect","Unknown operator");
			}

			cap = (BUN) ((((lng) l.days - f.days) * 24*60*60*1000 + l.msecs - f.msecs) / s);
			bn = COLnew(0, TYPE_oid, cap, TRANSIENT);
			if( bn == NULL) {
				if (cand)
					BBPunfix(cand->batCacheid);
				throw(MAL,"generator.thetasubselect",MAL_MALLOC_FAIL);
			}
			v = (oid*) Tloc(bn,0);

			if(cand){ cn = BATcount(cand); if( cl == 0) oc = cand->tseqbase; }
			val = f;
			for(j = 0; j< cap; j++,  o++){
				if( (( timestamp_isnil(low) || (val.days > low.days || (val.days == low.days && val.msecs >=low.msecs))) && 
					 ( timestamp_isnil(hgh) || (val.days < hgh.days || (val.days == hgh.days && val.msecs <= hgh.msecs)))) != anti){
					if(cand){
						if( cl){ while(cn-- >= 0 && *cl < o) cl++; if ( *cl == o){ *v++= o; c++;}}
						else { while(cn-- >= 0 && oc < o) oc++; if ( oc == o){ *v++= o; c++;} }
					} else {*v++ = o; c++;}
				}
				if( (msg = MTIMEtimestamp_add(&val, &val, &s)) != MAL_SUCCEED)
					goto wrapup;
			}
		} else {
			if (cand)
				BBPunfix(cand->batCacheid);
			throw(MAL,"generator.thetasubselect","Illegal generator arguments");
		}
	}

wrapup:
	if( cndid)
		BBPunfix(cndid);
	if( bn){
		bn->tsorted = 1;
		bn->trevsorted = 0;
		bn->tkey = 1;
		bn->tnil = 0;
		bn->tnonil = 1;
		BATsetcount(bn,c);
		BBPkeepref(*getArgReference_bat(stk,pci,0)= bn->batCacheid);
	}
	return msg;
}

#define VLTprojection(TPE) {\
	TPE f,l,s, val;\
	TPE *v;\
	f = *getArgReference_##TPE(stk,p, 1);\
	l = *getArgReference_##TPE(stk,p, 2);\
	if ( p->argc == 3) \
		s = f<l? (TPE) 1: (TPE)-1;\
	else s = * getArgReference_##TPE(stk, p, 3); \
	if ( s == 0 || (f> l && s>0) || (f<l && s < 0))\
		throw(MAL,"generator.projection","Illegal range");\
	bn = COLnew(0, TYPE_##TPE, cnt, TRANSIENT);\
	if( bn == NULL){\
		BBPunfix(bid);\
		throw(MAL,"generator.projection",MAL_MALLOC_FAIL);\
	}\
	v = (TPE*) Tloc(bn,0);\
	for(; cnt-- > 0; ol ? *ol++ : o++){\
		val = f + ((TPE) ( b->ttype == TYPE_void?o:*ol)) * s;\
		if ( (s > 0 &&  (val < f || val >= l)) || (s < 0 && (val<l || val >=f))) \
			continue;\
		*v++ = val;\
		c++;\
	}\
}

str VLTgenerator_projection(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int c= 0, tpe;
	bat bid = 0, *ret;
	BAT *b, *bn = NULL;
	BUN cnt;
	oid *ol =0, o= 0;
	InstrPtr p;
	str msg;

	(void) cntxt;
	p = findGeneratorDefinition(mb,pci,pci->argv[2]);

	ret = getArgReference_bat(stk,pci,0);
	b = BATdescriptor(bid = *getArgReference_bat(stk,pci,1));
	if( b == NULL)
		throw(MAL,"generator.projection",RUNTIME_OBJECT_MISSING);

	// if it does not exist we should fall back to the ordinary projection to try
	// it might have been materialized already
	if( p == NULL){
		bn = BATdescriptor( *getArgReference_bat(stk,pci,2));
		if( bn == NULL) {
			BBPunfix(b->batCacheid);
			throw(MAL,"generator.projection",RUNTIME_OBJECT_MISSING);
		}
		msg = ALGprojection(ret, &b->batCacheid, &bn->batCacheid);
		BBPunfix(b->batCacheid);
		BBPunfix(bn->batCacheid);
		return msg;
	}

	cnt = BATcount(b);
	if ( b->ttype == TYPE_void)
		o = b->tseqbase;
	else
		ol = (oid*) Tloc(b,0);

	/* the actual code to perform a projection over generators */
	switch( tpe = getArgType(mb,p,1)){
	case TYPE_bte:  VLTprojection(bte); break;
	case TYPE_sht:  VLTprojection(sht); break;
	case TYPE_int:  VLTprojection(int); break;
	case TYPE_lng:  VLTprojection(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge:  VLTprojection(hge); break;
#endif
	case TYPE_flt:  VLTprojection(flt); break;
	case TYPE_dbl:  VLTprojection(dbl); break;
	default:
		if ( tpe == TYPE_timestamp){
			timestamp f,l, val;
			lng s,t;
			timestamp *v;
			f = *getArgReference_TYPE(stk,p, 1, timestamp);
			l = *getArgReference_TYPE(stk,p, 2, timestamp);
			if ( p->argc == 3) {
				BBPunfix(b->batCacheid);
				throw(MAL,"generator.table","Timestamp step missing");
			}
			s =  *getArgReference_lng(stk,p, 3);
			if ( s == 0 ||
				(s< 0 &&	(f.days< l.days || (f.days == l.days && f.msecs < l.msecs))) ||
			     (s> 0 &&	(l.days< f.days || (l.days == f.days && l.msecs < f.msecs))) ) {
				BBPunfix(b->batCacheid);
				throw(MAL,"generator.projection","Illegal range");
			}

			bn = COLnew(0, TYPE_timestamp, cnt, TRANSIENT);
			if( bn == NULL){
				BBPunfix(bid);
				throw(MAL,"generator.projection",MAL_MALLOC_FAIL);
			}

			v = (timestamp*) Tloc(bn,0);

			for(; cnt-- > 0; ol ? *ol++ : o++){
				t = ((lng) ( b->ttype == TYPE_void?o:*ol)) * s;
				if( (msg = MTIMEtimestamp_add(&val, &f, &t)) != MAL_SUCCEED)
					return msg;

				if ( timestamp_isnil(val))
					continue;
				if (s > 0 && ((val.days < f.days || (val.days == f.days && val.msecs < f.msecs)) || ((val.days>l.days || (val.days== l.days && val.msecs >= l.msecs)))  ) )
					continue;
				if (s < 0 && ((val.days < l.days || (val.days == l.days && val.msecs < l.msecs)) || ((val.days>f.days || (val.days== f.days && val.msecs >= f.msecs)))  ) )
					continue;
				*v++ = val;
				c++;
			}
		}
	}

	/* adminstrative wrapup of the projection */
	BBPunfix(bid);
	if( bn){
		bn->tsorted = bn->trevsorted = 0;
		bn->tkey = 0;
		bn->tnil = 0;
		bn->tnonil = 0;
		BATsetcount(bn,c);
		BBPkeepref(*getArgReference_bat(stk,pci,0)= bn->batCacheid);
	}
	return MAL_SUCCEED;
}

/* The operands of a join operation can either be defined on a generator */
#define VLTjoin(TPE, ABS) \
	{ TPE f,l,s; TPE *v; BUN w;\
	f = *getArgReference_##TPE(stk,p, 1);\
	l = *getArgReference_##TPE(stk,p, 2);\
	if ( p->argc == 3) \
		s = f<l? (TPE) 1: (TPE)-1;\
	else s = * getArgReference_##TPE(stk, p, 3); \
	incr = s > 0;\
	v = (TPE*) Tloc(b,0);\
	if ( s == 0 || (f> l && s>0) || (f<l && s < 0))\
		throw(MAL,"generator.join","Illegal range");\
	for( ; cnt >0; cnt--,o++,v++){\
		w = (BUN) (ABS(*v -f)/ABS(s));\
		if ( f + (TPE)(w * s) == *v ){\
			*ol++ = (oid) w;\
			*or++ = o;\
			c++;\
		}\
	} }\

str VLTgenerator_join(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT  *b, *bl = NULL, *br = NULL, *bln = NULL, *brn= NULL;
	BUN cnt,c =0;
	oid o= 0, *ol, *or;
	int tpe, incr=0, materialized = 0;
	InstrPtr p = NULL, q = NULL;
	str msg = MAL_SUCCEED;

	(void) cntxt;
	// we assume at most one of the arguments to refer to the generator
	p = findGeneratorDefinition(mb,pci,pci->argv[2]);
	q = findGeneratorDefinition(mb,pci,pci->argv[3]);

	if (p == NULL && q == NULL) {
		bit zero = 0;
		return ALGjoin(getArgReference_bat(stk, pci, 0),
			       getArgReference_bat(stk, pci, 1),
			       getArgReference_bat(stk, pci, 2),
			       getArgReference_bat(stk, pci, 3),
			       NULL,  /* left candidate */
			       NULL,  /* right candidate */
			       &zero, /* nil_matches */
			       NULL); /* estimate */
	}

	if( p == NULL){
		bl = BATdescriptor(*getArgReference_bat(stk,pci,2));
		if( bl == NULL)
			throw(MAL,"generator.join",RUNTIME_OBJECT_MISSING);
	}
	if ( q == NULL){
		/* p != NULL, hence bl == NULL */
		br = BATdescriptor(*getArgReference_bat(stk,pci,3));
		if( br == NULL)
			throw(MAL,"generator.join",RUNTIME_OBJECT_MISSING);
	}

	// in case of both generators  || getModuleId(q) == generatorRef)materialize the 'smallest' one first
	// or implement more knowledge, postponed
	if (p && q ){
		msg =  VLTgenerator_table_(&bl, cntxt, mb, stk, p);
		if( msg || bl == NULL )
			throw(MAL,"generator.join","Join over generator pairs not supported");
		else
			p = NULL;
		materialized =1;
	}

	// switch roles to have a single target bat[:oid,:any] designated 
	// by b and reference instruction p for the generator
	b = q? bl : br;
	p = q? q : p;
	cnt = BATcount(b);
	tpe = b->ttype;
	o= b->hseqbase;
	
	bln = COLnew(0,TYPE_oid, cnt, TRANSIENT);
	brn = COLnew(0,TYPE_oid, cnt, TRANSIENT);
	if( bln == NULL || brn == NULL){
		if(bln) BBPunfix(bln->batCacheid);
		if(brn) BBPunfix(brn->batCacheid);
		if(bl) BBPunfix(bl->batCacheid);
		if(br) BBPunfix(br->batCacheid);
		throw(MAL,"generator.join",MAL_MALLOC_FAIL);
	}
	ol = (oid*) Tloc(bln,0);
	or = (oid*) Tloc(brn,0);

	/* The actual join code for generators be injected here */
	switch(tpe){
	case TYPE_bte: VLTjoin(bte,abs); break;
	case TYPE_sht: VLTjoin(sht,abs); break;
	case TYPE_int: VLTjoin(int,abs); break;
	case TYPE_lng: VLTjoin(lng,llabs); break;
#ifdef HAVE_HGE
	case TYPE_hge: VLTjoin(hge,HGE_ABS); break;
#endif
	case TYPE_flt: VLTjoin(flt,fabsf); break;
	case TYPE_dbl: VLTjoin(dbl,fabs); break;
	default:
		if( tpe == TYPE_timestamp){ 
			// it is easier to produce the timestamp series
			// then to estimate the possible index
			}
		throw(MAL,"generator.join","Illegal type");
	}

	bln->tsorted = bln->trevsorted = 0;
	bln->tkey = 0;
	bln->tnil = 0;
	bln->tnonil = 0;
	BATsetcount(bln,c);
	bln->tsorted = incr || c <= 1;
	bln->trevsorted = !incr || c <= 1;
	
	brn->tsorted = brn->trevsorted = 0;
	brn->tkey = 0;
	brn->tnil = 0;
	brn->tnonil = 0;
	BATsetcount(brn,c);
	brn->tsorted = incr || c <= 1;
	brn->trevsorted = !incr || c <= 1;
	if( q){
		BBPkeepref(*getArgReference_bat(stk,pci,0)= brn->batCacheid);
		BBPkeepref(*getArgReference_bat(stk,pci,1)= bln->batCacheid);
	} else {
		BBPkeepref(*getArgReference_bat(stk,pci,0)= bln->batCacheid);
		BBPkeepref(*getArgReference_bat(stk,pci,1)= brn->batCacheid);
	}
	if ( materialized){
		BBPreclaim(bl);
		bl = 0;
	}
	if(bl) BBPunfix(bl->batCacheid);
	if(br) BBPunfix(br->batCacheid);
	return msg;
}

#define VLTrangeExpand() \
{	limit+= cnt * (limit/(done?done:1)+1);\
	if (BATextend(bln, limit) != GDK_SUCCEED) {	\
		BBPunfix(blow->batCacheid);\
		BBPunfix(bhgh->batCacheid);\
		BBPunfix(bln->batCacheid);\
		BBPunfix(brn->batCacheid);\
		throw(MAL,"generator.rangejoin",MAL_MALLOC_FAIL);\
	}\
	if (BATextend(brn, limit) != GDK_SUCCEED) {	\
		BBPunfix(blow->batCacheid);\
		BBPunfix(bhgh->batCacheid);\
		BBPunfix(bln->batCacheid);\
		BBPunfix(brn->batCacheid);\
		throw(MAL,"generator.rangejoin",MAL_MALLOC_FAIL);\
	}\
	ol = (oid*) Tloc(bln,0) + c;\
	or = (oid*) Tloc(brn,0) + c;\
}

/* The operands of a join operation can either be defined on a generator */
#define VLTrangejoin(TPE, ABS, FLOOR) \
{ TPE f,f1,l,s; TPE *vlow,*vhgh; BUN w;\
	f = *getArgReference_##TPE(stk,p, 1);\
	l = *getArgReference_##TPE(stk,p, 2);\
	if ( p->argc == 3) \
		s = f<l? (TPE) 1: (TPE)-1;\
	else s = * getArgReference_##TPE(stk, p, 3); \
	incr = s > 0;\
	if ( s == 0 || (f> l && s>0) || (f<l && s < 0))\
		throw(MAL,"generator.rangejoin","Illegal range");\
	vlow = (TPE*) Tloc(blow,0);\
	vhgh = (TPE*) Tloc(bhgh,0);\
	for( ; cnt >0; cnt--, done++, o++,vlow++,vhgh++){\
		f1 = f + FLOOR(ABS(*vlow-f)/ABS(s)) * s;\
		if ( f1 < *vlow ) f1+= s;\
		w = (BUN) FLOOR(ABS(f1-f)/ABS(s));\
		for( ; (f1 > *vlow || (li && f1 == *vlow)) && (f1 < *vhgh || (ri && f1 == *vhgh)); f1 += s, w++){\
			if(c == limit)\
				VLTrangeExpand();\
			*ol++ = (oid) w;\
			*or++ = o;\
			c++;\
		}\
} }

str VLTgenerator_rangejoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT  *blow = NULL, *bhgh = NULL, *bln = NULL, *brn= NULL;
	bit li,ri;
	BUN limit, cnt, done=0, c =0;
	oid o= 0, *ol, *or;
	int tpe, incr=0;
	InstrPtr p = NULL;
	str msg = MAL_SUCCEED;

	(void) cntxt;
	// the left join argument should be a generator
	p = findGeneratorDefinition(mb,pci,pci->argv[2]);
	if( p == NULL)
		throw(MAL,"generator.rangejoin","invalid arguments");

	blow = BATdescriptor(*getArgReference_bat(stk,pci,3));
	if( blow == NULL)
		throw(MAL,"generator.rangejoin",RUNTIME_OBJECT_MISSING);

	bhgh = BATdescriptor(*getArgReference_bat(stk,pci,4));
	if( bhgh == NULL){
		BBPunfix(blow->batCacheid);
		throw(MAL,"generator.rangejoin",RUNTIME_OBJECT_MISSING);
	}
	li = *getArgReference_bit(stk,pci,5);
	ri = *getArgReference_bit(stk,pci,6);

	cnt = BATcount(blow);
	limit = 2 * cnt; //top off result before expansion
	tpe = blow->ttype;
	o= blow->hseqbase;
	
	bln = COLnew(0,TYPE_oid, limit, TRANSIENT);
	brn = COLnew(0,TYPE_oid, limit, TRANSIENT);
	if( bln == NULL || brn == NULL){
		if(bln) BBPunfix(bln->batCacheid);
		if(brn) BBPunfix(brn->batCacheid);
		if(blow) BBPunfix(blow->batCacheid);
		if(bhgh) BBPunfix(bhgh->batCacheid);
		throw(MAL,"generator.rangejoin",MAL_MALLOC_FAIL);
	}
	ol = (oid*) Tloc(bln,0);
	or = (oid*) Tloc(brn,0);

	/* The actual join code for generators be injected here */
	switch(tpe){
	case TYPE_bte: VLTrangejoin(bte,abs,IDENTITY); break;
	case TYPE_sht: VLTrangejoin(sht,abs,IDENTITY); break;
	case TYPE_int: VLTrangejoin(int,abs,IDENTITY); break;
	case TYPE_lng: VLTrangejoin(lng,llabs,IDENTITY); break;
#ifdef HAVE_HGE
	case TYPE_hge: VLTrangejoin(hge,HGE_ABS,IDENTITY); break;
#endif
	case TYPE_flt: VLTrangejoin(flt,fabsf,floorf); break;
	case TYPE_dbl: VLTrangejoin(dbl,fabs,floor); break;
	default:
		if( tpe == TYPE_timestamp){ 
			// it is easier to produce the timestamp series
			// then to estimate the possible index
			}
		throw(MAL,"generator.rangejoin","Illegal type");
	}

	bln->tsorted = bln->trevsorted = 0;
	bln->tkey = 0;
	bln->tnil = 0;
	bln->tnonil = 0;
	BATsetcount(bln,c);
	bln->tsorted = incr || c <= 1;
	bln->trevsorted = !incr || c <= 1;
	
	brn->tsorted = brn->trevsorted = 0;
	brn->tkey = 0;
	brn->tnil = 0;
	brn->tnonil = 0;
	BATsetcount(brn,c);
	brn->tsorted = incr || c <= 1;
	brn->trevsorted = !incr || c <= 1;
	BBPkeepref(*getArgReference_bat(stk,pci,0)= bln->batCacheid);
	BBPkeepref(*getArgReference_bat(stk,pci,1)= brn->batCacheid);
	return msg;
}
