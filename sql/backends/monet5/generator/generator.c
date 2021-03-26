/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * (c) Martin Kersten, Sjoerd Mullender
 * Series generating module for integer, decimal, real, double and timestamps.
 */

#include "monetdb_config.h"
#include "opt_prelude.h"
#include "generator.h"
#include "gdk_time.h"


#define IDENTITY(x)	(x)

/*
 * The noop simply means that we keep the properties for the generator object.
 */
#define VLTnoop(TPE)												\
	do {															\
		TPE s;														\
		s = pci->argc == 3 ? 1: *getArgReference_##TPE(stk,pci, 3); \
		zeroerror = (s == 0);										\
		nullerr = is_##TPE##_nil(s);								\
	} while (0)

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
		} else throw(MAL,"generator.noop", SQLSTATE(42000) "unknown data type %d", getArgType(mb,pci,1));
	}
	if( zeroerror)
		throw(MAL,"generator.noop", SQLSTATE(42000) "Zero step size not allowed");
	if( nullerr)
		throw(MAL,"generator.noop", SQLSTATE(42000) "Null step size not allowed");
	return MAL_SUCCEED;
}

/*
 * The base line consists of materializing the generator iterator value
 */
#define VLTmaterialize(TPE)												\
	do {																\
		TPE *v, f, l, s;												\
		f = *getArgReference_##TPE(stk, pci, 1);						\
		l = *getArgReference_##TPE(stk, pci, 2);						\
		if ( pci->argc == 3)											\
			s = f<l? (TPE) 1: (TPE)-1;									\
		else s =  *getArgReference_##TPE(stk,pci, 3);					\
		if (s == 0 || (s > 0 && f > l) || (s < 0 && f < l) || is_##TPE##_nil(f) || is_##TPE##_nil(l)) \
			throw(MAL, "generator.table",								\
			      SQLSTATE(42000) "Illegal generator range");			\
		n = (BUN) ((l - f) / s);										\
		if ((TPE) (n * s + f) != l)										\
			n++;														\
		bn = COLnew(0, TYPE_##TPE, n, TRANSIENT);						\
		if (bn == NULL)													\
			throw(MAL, "generator.table", SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
		v = (TPE*) Tloc(bn, 0);											\
		for (c = 0; c < n; c++)											\
			*v++ = (TPE) (f + c * s);									\
		bn->tsorted = s > 0 || n <= 1;									\
		bn->trevsorted = s < 0 || n <= 1;								\
	} while (0)

static str
VLTgenerator_table_(BAT **result, Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BUN c, n;
	BAT *bn;
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
				      SQLSTATE(42000) "Illegal generator expression range");
			f = *getArgReference_TYPE(stk, pci, 1, timestamp);
			l = *getArgReference_TYPE(stk, pci, 2, timestamp);
			if ( pci->argc == 3)
					throw(MAL,"generator.table", SQLSTATE(42000) "Timestamp step missing");
			s = *getArgReference_lng(stk, pci, 3);
			if (s == 0 ||
			    (s > 0 && ret.val.btval > 0) ||
			    (s < 0 && ret.val.btval < 0) ||
				is_timestamp_nil(f) || is_timestamp_nil(l))
				throw(MAL, "generator.table",
				      SQLSTATE(42000) "Illegal generator range");
			/* casting one value to lng causes the whole
			 * computation to be done as lng, reducing the
			 * risk of overflow */
			s *= 1000; /* msec -> usec */
			n = (BUN) (timestamp_diff(l, f) / s);
			bn = COLnew(0, TYPE_timestamp, n + 1, TRANSIENT);
			if (bn == NULL)
				throw(MAL, "generator.table", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			v = (timestamp *) Tloc(bn, 0);
			for (c = 0; c < n; c++) {
				*v++ = f;
				f = timestamp_add_usec(f, s);
				if (is_timestamp_nil(f)) {
					BBPreclaim(bn);
					throw(MAL, "generator.table", SQLSTATE(22003) "overflow in calculation");
				}
			}
			if (f != l) {
				*v++ = f;
				n++;
			}
			bn->tsorted = s > 0 || n <= 1;
			bn->trevsorted = s < 0 || n <= 1;
		} else {
			throw(MAL, "generator.table", SQLSTATE(42000) "Unsupported type");
		}
		break;
	}
	BATsetcount(bn, c);
	bn->tkey = true;
	bn->tnil = false;
	bn->tnonil = true;
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

#define calculate_range(TPE, TPE2)										\
	do {																\
		TPE f, l, s, low, hgh;											\
																		\
		f = * getArgReference_##TPE(stk, p, 1);							\
		l = * getArgReference_##TPE(stk, p, 2);							\
		if ( p->argc == 3)												\
			s = f<l? (TPE) 1: (TPE)-1;									\
		else s = * getArgReference_##TPE(stk, p, 3);					\
		if (s == 0 || (s > 0 && f > l) || (s < 0 && f < l) || is_##TPE##_nil(f) || is_##TPE##_nil(l)) \
			throw(MAL, "generator.select",								\
			      SQLSTATE(42000) "Illegal generator range");			\
		n = (BUN) (((TPE2) l - (TPE2) f) / (TPE2) s);					\
		if ((TPE)(n * s + f) != l)										\
			n++;														\
																		\
		low = * getArgReference_##TPE(stk, pci, i);						\
		hgh = * getArgReference_##TPE(stk, pci, i + 1);					\
																		\
		if (!is_##TPE##_nil(low) && low == hgh)							\
			hi = li;													\
		if (is_##TPE##_nil(low) && is_##TPE##_nil(hgh)) {				\
			if (li && hi && !anti) {									\
				/* match NILs (of which there aren't */					\
				/* any) */												\
				o1 = o2 = 0;											\
			} else {													\
				/* match all non-NIL values, */							\
				/* i.e. everything */									\
				o1 = 0;													\
				o2 = (oid) n;											\
			}															\
		} else if (s > 0) {												\
			if (is_##TPE##_nil(low) || low < f)							\
				o1 = 0;													\
			else {														\
				o1 = (oid) (((TPE2) low - (TPE2) f) / (TPE2) s);		\
				if ((TPE) (f + o1 * s) < low ||							\
				    (!li && (TPE) (f + o1 * s) == low))					\
					o1++;												\
			}															\
			if (is_##TPE##_nil(hgh))									\
				o2 = (oid) n;											\
			else if (hgh < f)											\
				o2 = 0;													\
			else {														\
				o2 = (oid) (((TPE2) hgh - (TPE2) f) / (TPE2) s);		\
				if ((hi && (TPE) (f + o2 * s) == hgh) ||				\
				    (TPE) (f + o2 * s) < hgh)							\
					o2++;												\
			}															\
		} else {														\
			if (is_##TPE##_nil(low))									\
				o2 = (oid) n;											\
			else if (low > f)											\
				o2 = 0;													\
			else {														\
				o2 = (oid) (((TPE2) low - (TPE2) f) / (TPE2) s);		\
				if ((li && (TPE) (f + o2 * s) == low) ||				\
				    (TPE) (f + o2 * s) > low)							\
					o2++;												\
			}															\
			if (is_##TPE##_nil(hgh) || hgh > f)							\
				o1 = 0;													\
			else {														\
				o1 = (oid) (((TPE2) hgh - (TPE2) f) / (TPE2) s);		\
				if ((!hi && (TPE) (f + o1 * s) == hgh) ||				\
				    (TPE) (f + o1 * s) > hgh)							\
					o1++;												\
			}															\
		}																\
	} while (0)

str
VLTgenerator_subselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit li, hi, anti;
	int i;
	oid o1, o2;
	BUN n = 0;
	BAT *bn, *cand = NULL;
	struct canditer ci = (struct canditer) {.tpe = cand_dense};
	InstrPtr p;
	int tpe;

	(void) cntxt;
	p = findGeneratorDefinition(mb, pci, pci->argv[1]);
	if (p == NULL)
		throw(MAL, "generator.select",
		      SQLSTATE(42000) "Could not locate definition for object");

	if (pci->argc == 8) {	/* candidate list included */
		bat candid = *getArgReference_bat(stk, pci, 2);
		if (candid) {
			cand = BATdescriptor(candid);
			if (cand == NULL)
				throw(MAL, "generator.select",
				      SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			canditer_init(&ci, NULL, cand);
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
			oid *ol;

			tsf = *getArgReference_TYPE(stk, p, 1, timestamp);
			tsl = *getArgReference_TYPE(stk, p, 2, timestamp);
			if ( p->argc == 3)
				throw(MAL,"generator.table", SQLSTATE(42000) "Timestamp step missing");
			tss = *getArgReference_lng(stk, p, 3);
			if ( tss == 0 ||
				is_timestamp_nil(tsf) || is_timestamp_nil(tsl) ||
				 (tss > 0 && tsf > tsl ) ||
				 (tss < 0 && tsf < tsl )
				)
				throw(MAL, "generator.select",  SQLSTATE(42000) "Illegal generator range");

			tlow = *getArgReference_TYPE(stk,pci,i, timestamp);
			thgh = *getArgReference_TYPE(stk,pci,i+1, timestamp);

			if (!is_timestamp_nil(tlow) && tlow == thgh)
				hi = li;
			if( hi && !is_timestamp_nil(thgh)) {
				thgh = timestamp_add_usec(thgh, 1);
				if (is_timestamp_nil(thgh))
					throw(MAL, "generator.select", SQLSTATE(22003) "overflow in calculation");
			}
			if( !li && !is_timestamp_nil(tlow)) {
				tlow = timestamp_add_usec(tlow, 1);
				if (is_timestamp_nil(tlow))
					throw(MAL, "generator.select", SQLSTATE(22003) "overflow in calculation");
			}

			/* casting one value to lng causes the whole
			 * computation to be done as lng, reducing the
			 * risk of overflow */
			tss *= 1000; /* msec -> usec */
			o2 = (BUN) (timestamp_diff(tsl, tsf) / tss);
			bn = COLnew(0, TYPE_oid, o2 + 1, TRANSIENT);
			if (bn == NULL)
				throw(MAL, "generator.select", SQLSTATE(HY013) MAL_MALLOC_FAIL);

			// simply enumerate the sequence and filter it by predicate and candidate list
			ol = (oid *) Tloc(bn, 0);
			for (o1=0; o1 <= o2; o1++) {
				if(((is_timestamp_nil(tlow) || tsf >= tlow) &&
				    (is_timestamp_nil(thgh) || tsf < thgh)) != anti ){
					/* could be improved when no candidate list is available into a void/void BAT */
					if( cand == NULL || canditer_contains(&ci, o1)) {
						*ol++ = o1;
						n++;
					}
				}
				tsf = timestamp_add_usec(tsf, tss);
				if (is_timestamp_nil(tsf)) {
					BBPreclaim(bn);
					throw(MAL, "generator.select", SQLSTATE(22003) "overflow in calculation");
				}
			}
			BATsetcount(bn, (BUN) n);
			bn->tsorted = true;
			bn->trevsorted = BATcount(bn) <= 1;
			bn->tkey = true;
			bn->tnil = false;
			bn->tnonil = true;
			* getArgReference_bat(stk, pci, 0) = bn->batCacheid;
			BBPkeepref(bn->batCacheid);
			return MAL_SUCCEED;
		} else
			throw(MAL, "generator.select", SQLSTATE(42000) "Unsupported type in select");
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
		if (anti) {
			bn = canditer_slice2val(&ci, oid_nil, o1, o2, oid_nil);
		} else {
			bn = canditer_sliceval(&ci, o1, o2);
		}
		BBPunfix(cand->batCacheid);
		if (bn == NULL)
			throw(MAL, "generator.select",
			      SQLSTATE(HY013) MAL_MALLOC_FAIL);
	} else {
		if (anti) {
			oid o;
			oid *op;

			bn = COLnew(0, TYPE_oid, n - (o2 - o1), TRANSIENT);
			if (bn == NULL)
				throw(MAL, "generator.select",
				      SQLSTATE(HY013) MAL_MALLOC_FAIL);
			BATsetcount(bn, n - (o2 - o1));
			op = (oid *) Tloc(bn, 0);
			for (o = 0; o < o1; o++)
				*op++ = o;
			for (o = o2; o < (oid) n; o++)
				*op++ = o;
			bn->tnil = false;
			bn->tnonil = true;
			bn->tsorted = true;
			bn->trevsorted = BATcount(bn) <= 1;
			bn->tkey = true;
		} else {
			bn = BATdense(0, o1, (BUN) (o2 - o1));
			if (bn == NULL)
				throw(MAL, "generator.select",
				      SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	* getArgReference_bat(stk, pci, 0) = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

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

#define VLTthetasubselect(TPE,ABS)										\
	do {																\
		TPE f,l,s, low, hgh;											\
		BUN j; oid *v;													\
		f = *getArgReference_##TPE(stk,p, 1);							\
		l = *getArgReference_##TPE(stk,p, 2);							\
		if ( p->argc == 3)												\
			s = f<l? (TPE) 1: (TPE)-1;									\
		else s =  *getArgReference_##TPE(stk,p, 3);						\
		if( s == 0 || (f<l && s < 0) || (f>l && s> 0))					\
			throw(MAL,"generator.thetaselect", SQLSTATE(42000) "Illegal range"); \
		cap = (BUN)(ABS(l-f)/ABS(s));									\
		bn = COLnew(0, TYPE_oid, cap, TRANSIENT);						\
		if( bn == NULL)													\
			throw(MAL,"generator.thetaselect", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
		low= hgh = TPE##_nil;											\
		v = (oid*) Tloc(bn,0);											\
		if ( strcmp(oper,"<") == 0){									\
			hgh= *getArgReference_##TPE(stk,pci,3);						\
			hgh = PREVVALUE##TPE(hgh);									\
		} else if ( strcmp(oper,"<=") == 0){							\
			hgh= *getArgReference_##TPE(stk,pci,3);						\
		} else if ( strcmp(oper,">") == 0){								\
			low= *getArgReference_##TPE(stk,pci,3);						\
			low = NEXTVALUE##TPE(low);									\
		} else if ( strcmp(oper,">=") == 0){							\
			low= *getArgReference_##TPE(stk,pci,3);						\
		} else if ( strcmp(oper,"!=") == 0 || strcmp(oper, "<>") == 0){ \
			hgh= low= *getArgReference_##TPE(stk,pci,3);				\
			anti = 1;													\
		} else if ( strcmp(oper,"==") == 0 || strcmp(oper, "=") == 0){	\
			hgh= low= *getArgReference_##TPE(stk,pci,3);				\
		} else															\
			throw(MAL,"generator.thetaselect", SQLSTATE(42000) "Unknown operator");	\
		for(j=0;j<cap;j++, f+=s, o++)									\
			if( ((is_##TPE##_nil(low) || f >= low) && (is_##TPE##_nil(hgh) || f <= hgh)) != anti){ \
				if(cand == NULL || canditer_contains(&ci, o)) {			\
					*v++ = o;											\
					c++;												\
				}														\
			}															\
	} while (0)


str VLTgenerator_thetasubselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int c= 0, anti =0,tpe;
	bat cndid =0;
	BAT *cand = 0, *bn = NULL;
	struct canditer ci = (struct canditer) {.tpe = cand_dense};
	BUN cap,j;
	oid o = 0;
	InstrPtr p;
	str oper, msg= MAL_SUCCEED;

	(void) cntxt;
	p = findGeneratorDefinition(mb,pci,pci->argv[1]);
	if( p == NULL)
		throw(MAL,"generator.thetaselect",SQLSTATE(42000) "Could not locate definition for object");

	assert(pci->argc == 5); // candidate list included
	cndid = *getArgReference_bat(stk,pci, 2);
	if( !is_bat_nil(cndid)){
		cand = BATdescriptor(cndid);
		if( cand == NULL)
			throw(MAL,"generator.select", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		canditer_init(&ci, NULL, cand);
	}
	oper= *getArgReference_str(stk,pci,4);

	// check the step direction

	switch( tpe =getArgType(mb,pci,3)){
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
				throw(MAL,"generator.table", SQLSTATE(42000) "Timestamp step missing");
			}
			s = *getArgReference_lng(stk,p, 3);
			if ( s == 0 ||
				 (s > 0 && f > l) ||
				 (s < 0 && f < l)
				) {
				if (cand)
					BBPunfix(cand->batCacheid);
				throw(MAL, "generator.select", SQLSTATE(42000) "Illegal generator range");
			}

			hgh = low = timestamp_nil;
			if ( strcmp(oper,"<") == 0){
				hgh= *getArgReference_TYPE(stk,pci,3, timestamp);
				hgh = timestamp_add_usec(hgh, -1);
				if (is_timestamp_nil(hgh)) {
					if (cand)
						BBPunfix(cand->batCacheid);
					throw(MAL, "generator.select", SQLSTATE(22003) "overflow in calculation");
				}
			} else
			if ( strcmp(oper,"<=") == 0){
				hgh= *getArgReference_TYPE(stk,pci,3, timestamp) ;
			} else
			if ( strcmp(oper,">") == 0){
				low= *getArgReference_TYPE(stk,pci,3, timestamp);
				low = timestamp_add_usec(low, 1);
				if (is_timestamp_nil(low)) {
					if (cand)
						BBPunfix(cand->batCacheid);
					throw(MAL, "generator.select", SQLSTATE(22003) "overflow in calculation");
				}
			} else
			if ( strcmp(oper,">=") == 0){
				low= *getArgReference_TYPE(stk,pci,3, timestamp);
			} else
			if ( strcmp(oper,"!=") == 0 || strcmp(oper, "<>") == 0){
				hgh= low= *getArgReference_TYPE(stk,pci,3, timestamp);
				anti = 1;
			} else
			if ( strcmp(oper,"==") == 0 || strcmp(oper, "=") == 0){
				hgh= low= *getArgReference_TYPE(stk,pci,3, timestamp);
			} else {
				if (cand)
					BBPunfix(cand->batCacheid);
				throw(MAL,"generator.thetaselect", SQLSTATE(42000) "Unknown operator");
			}

			s *= 1000; /* msec -> usec */
			cap = (BUN) (timestamp_diff(l, f) / s);
			bn = COLnew(0, TYPE_oid, cap, TRANSIENT);
			if( bn == NULL) {
				if (cand)
					BBPunfix(cand->batCacheid);
				throw(MAL,"generator.thetaselect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			v = (oid*) Tloc(bn,0);

			val = f;
			for(j = 0; j< cap; j++,  o++){
				if (((is_timestamp_nil(low) || val >= low) &&
				     (is_timestamp_nil(hgh) || val <= hgh)) != anti){
					if(cand == NULL || canditer_contains(&ci, o)){
						*v++ = o;
						c++;
					}
				}
				val = timestamp_add_usec(val, s);
				if (is_timestamp_nil(val)) {
					msg = createException(MAL, "generator.thetaselect", SQLSTATE(22003) "overflow in calculation");
					goto wrapup;
				}
			}
		} else {
			if (cand)
				BBPunfix(cand->batCacheid);
			throw(MAL,"generator.thetaselect", SQLSTATE(42000) "Illegal generator arguments");
		}
	}

wrapup:
	if( cndid)
		BBPunfix(cndid);
	if( bn){
		bn->tsorted = true;
		bn->trevsorted = false;
		bn->tkey = true;
		bn->tnil = false;
		bn->tnonil = true;
		BATsetcount(bn,c);
		BBPkeepref(*getArgReference_bat(stk,pci,0)= bn->batCacheid);
	}
	return msg;
}

#define VLTprojection(TPE)												\
	do {																\
		TPE f,l,s, val;													\
		TPE *v;															\
		f = *getArgReference_##TPE(stk,p, 1);							\
		l = *getArgReference_##TPE(stk,p, 2);							\
		if ( p->argc == 3)												\
			s = f<l? (TPE) 1: (TPE)-1;									\
		else															\
			s = * getArgReference_##TPE(stk, p, 3);						\
		if ( s == 0 || (f> l && s>0) || (f<l && s < 0))					\
			throw(MAL,"generator.projection", SQLSTATE(42000) "Illegal range");	\
		bn = COLnew(0, TYPE_##TPE, cnt, TRANSIENT);						\
		if( bn == NULL){												\
			BBPunfix(bid);												\
			throw(MAL,"generator.projection", SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
		}																\
		v = (TPE*) Tloc(bn,0);											\
		for(; cnt-- > 0; o++){											\
			val = f + ((TPE) (ol == NULL  ? o : ol[o])) * s;			\
			if ( (s > 0 &&  (val < f || val >= l)) || (s < 0 && (val <= l || val > f))) \
				continue;												\
			*v++ = val;													\
			c++;														\
		}																\
	} while (0)

str VLTgenerator_projection(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int c= 0, tpe;
	bat bid = 0, *ret;
	BAT *b, *bn = NULL;
	BUN cnt;
	oid *ol = NULL, o= 0;
	InstrPtr p;

	(void) cntxt;
	p = findGeneratorDefinition(mb,pci,pci->argv[2]);

	ret = getArgReference_bat(stk,pci,0);
	b = BATdescriptor(bid = *getArgReference_bat(stk,pci,1));
	if( b == NULL)
		throw(MAL,"generator.projection", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	// if it does not exist we should fall back to the ordinary projection to try
	// it might have been materialized already
	if( p == NULL){
		bn = BATdescriptor( *getArgReference_bat(stk,pci,2));
		if( bn == NULL) {
			BBPunfix(b->batCacheid);
			throw(MAL,"generator.projection", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		BAT *bp = BATproject(b, bn);
		BBPunfix(b->batCacheid);
		BBPunfix(bn->batCacheid);
		if (bp == NULL)
			throw(MAL, "generator.projection", GDK_EXCEPTION);
		*ret = bp->batCacheid;
		BBPkeepref(*ret);
		return MAL_SUCCEED;
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
				throw(MAL,"generator.table", SQLSTATE(42000) "Timestamp step missing");
			}
			s =  *getArgReference_lng(stk,p, 3);
			if ( s == 0 ||
			     (s< 0 && f < l) ||
			     (s> 0 && l < f) ) {
				BBPunfix(b->batCacheid);
				throw(MAL,"generator.projection", SQLSTATE(42000) "Illegal range");
			}

			s *= 1000; /* msec -> usec */
			bn = COLnew(0, TYPE_timestamp, cnt, TRANSIENT);
			if( bn == NULL){
				BBPunfix(bid);
				throw(MAL,"generator.projection", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}

			v = (timestamp*) Tloc(bn,0);

			for(; cnt-- > 0; o++){
				t = ((lng) (ol == NULL ? o : ol[o])) * s;
				val = timestamp_add_usec(f, t);
				if (is_timestamp_nil(val))
					throw(MAL, "generator.projection", SQLSTATE(22003) "overflow in calculation");

				if ( is_timestamp_nil(val))
					continue;
				if (s > 0 && (val < f || val >= l) )
					continue;
				if (s < 0 && (val <= l || val > f) )
					continue;
				*v++ = val;
				c++;
			}
		}
	}

	/* adminstrative wrapup of the projection */
	BBPunfix(bid);
	if( bn){
		bn->tsorted = bn->trevsorted = false;
		bn->tkey = false;
		bn->tnil = false;
		bn->tnonil = false;
		BATsetcount(bn,c);
		BBPkeepref(*getArgReference_bat(stk,pci,0)= bn->batCacheid);
	}
	return MAL_SUCCEED;
}

/* The operands of a join operation can either be defined on a generator */
#define VLTjoin(TPE, ABS)												\
	do {																\
		TPE f,l,s; TPE *v; BUN w;										\
		f = *getArgReference_##TPE(stk,p, 1);							\
		l = *getArgReference_##TPE(stk,p, 2);							\
		if ( p->argc == 3)												\
			s = f<l? (TPE) 1: (TPE)-1;									\
		else															\
			s = * getArgReference_##TPE(stk, p, 3);						\
		incr = s > 0;													\
		v = (TPE*) Tloc(b,0);											\
		if ( s == 0 || (f> l && s>0) || (f<l && s < 0))					\
			throw(MAL,"generator.join", SQLSTATE(42000) "Illegal range"); \
		for( ; cnt >0; cnt--,o++,v++){									\
			w = (BUN) (ABS(*v -f)/ABS(s));								\
			if ( f + (TPE)(w * s) == *v ){								\
				*ol++ = (oid) w;										\
				*or++ = o;												\
				c++;													\
			}															\
		}																\
	} while (0)

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
		bl = BATdescriptor(*getArgReference_bat(stk, pci, 2));
		br = BATdescriptor(*getArgReference_bat(stk, pci, 3));
		if (bl == NULL || br == NULL) {
			if (bl)
				BBPunfix(bl->batCacheid);
			if (br)
				BBPunfix(br->batCacheid);
			throw(MAL,"generator.join", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		gdk_return rc = BATjoin(&bln, &brn, bl, br, NULL, NULL, false, BUN_NONE);
		BBPunfix(bl->batCacheid);
		BBPunfix(br->batCacheid);
		if (rc != GDK_SUCCEED)
			throw(MAL,"generator.join", GDK_EXCEPTION);
		*getArgReference_bat(stk, pci, 0) = bln->batCacheid;
		*getArgReference_bat(stk, pci, 1) = brn->batCacheid;
		BBPkeepref(bln->batCacheid);
		BBPkeepref(brn->batCacheid);
		return MAL_SUCCEED;
	}

	if( p == NULL){
		bl = BATdescriptor(*getArgReference_bat(stk,pci,2));
		if( bl == NULL)
			throw(MAL,"generator.join", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ( q == NULL){
		/* p != NULL, hence bl == NULL */
		br = BATdescriptor(*getArgReference_bat(stk,pci,3));
		if( br == NULL)
			throw(MAL,"generator.join", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	// in case of both generators  || getModuleId(q) == generatorRef)materialize the 'smallest' one first
	// or implement more knowledge, postponed
	if (p && q ){
		msg =  VLTgenerator_table_(&bl, cntxt, mb, stk, p);
		if( msg || bl == NULL )
			throw(MAL,"generator.join",SQLSTATE(42000) "Join over generator pairs not supported");
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
		throw(MAL,"generator.join", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
		throw(MAL,"generator.join", SQLSTATE(42000) "Illegal type");
	}

	bln->tsorted = bln->trevsorted = false;
	bln->tkey = false;
	bln->tnil = false;
	bln->tnonil = false;
	BATsetcount(bln,c);
	bln->tsorted = incr || c <= 1;
	bln->trevsorted = !incr || c <= 1;

	brn->tsorted = brn->trevsorted = false;
	brn->tkey = false;
	brn->tnil = false;
	brn->tnonil = false;
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

#define VLTrangeExpand()												\
	do {																\
		limit+= cnt * (limit/(done?done:1)+1);							\
		if (BATextend(bln, limit) != GDK_SUCCEED) {						\
			BBPunfix(blow->batCacheid);									\
			BBPunfix(bhgh->batCacheid);									\
			BBPunfix(bln->batCacheid);									\
			BBPunfix(brn->batCacheid);									\
			throw(MAL,"generator.rangejoin", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
		}																\
		if (BATextend(brn, limit) != GDK_SUCCEED) {						\
			BBPunfix(blow->batCacheid);									\
			BBPunfix(bhgh->batCacheid);									\
			BBPunfix(bln->batCacheid);									\
			BBPunfix(brn->batCacheid);									\
			throw(MAL,"generator.rangejoin", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
		}																\
		ol = (oid*) Tloc(bln,0) + c;									\
		or = (oid*) Tloc(brn,0) + c;									\
	} while (0)

/* The operands of a join operation can either be defined on a generator */
#define VLTrangejoin(TPE, ABS, FLOOR)									\
	do {																\
		TPE f,f1,l,s; TPE *vlow,*vhgh; BUN w;							\
		f = *getArgReference_##TPE(stk,p, 1);							\
		l = *getArgReference_##TPE(stk,p, 2);							\
		if ( p->argc == 3)												\
			s = f<l? (TPE) 1: (TPE)-1;									\
		else															\
			s = * getArgReference_##TPE(stk, p, 3);						\
		incr = s > 0;													\
		if ( s == 0 || (f> l && s>0) || (f<l && s < 0))					\
			throw(MAL,"generator.rangejoin", SQLSTATE(42000) "Illegal range"); \
		vlow = (TPE*) Tloc(blow,0);										\
		vhgh = (TPE*) Tloc(bhgh,0);										\
		for( ; cnt >0; cnt--, done++, o++,vlow++,vhgh++){				\
			f1 = f + FLOOR(ABS(*vlow-f)/ABS(s)) * s;					\
			if ( f1 < *vlow )											\
				f1+= s;													\
			w = (BUN) FLOOR(ABS(f1-f)/ABS(s));							\
			for( ; (f1 > *vlow || (li && f1 == *vlow)) && (f1 < *vhgh || (ri && f1 == *vhgh)); f1 += s, w++){ \
				if(c == limit)											\
					VLTrangeExpand();									\
				*ol++ = (oid) w;										\
				*or++ = o;												\
				c++;													\
			}															\
		}																\
	} while (0)

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
		throw(MAL,"generator.rangejoin",SQLSTATE(42000) "Invalid arguments");

	blow = BATdescriptor(*getArgReference_bat(stk,pci,3));
	if( blow == NULL)
		throw(MAL,"generator.rangejoin", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	bhgh = BATdescriptor(*getArgReference_bat(stk,pci,4));
	if( bhgh == NULL){
		BBPunfix(blow->batCacheid);
		throw(MAL,"generator.rangejoin", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
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
		throw(MAL,"generator.rangejoin", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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

	bln->tsorted = bln->trevsorted = false;
	bln->tkey = false;
	bln->tnil = false;
	bln->tnonil = false;
	BATsetcount(bln,c);
	bln->tsorted = incr || c <= 1;
	bln->trevsorted = !incr || c <= 1;

	brn->tsorted = brn->trevsorted = false;
	brn->tkey = false;
	brn->tnil = false;
	brn->tnonil = false;
	BATsetcount(brn,c);
	brn->tsorted = incr || c <= 1;
	brn->trevsorted = !incr || c <= 1;
	BBPkeepref(*getArgReference_bat(stk,pci,0)= bln->batCacheid);
	BBPkeepref(*getArgReference_bat(stk,pci,1)= brn->batCacheid);
	return msg;
}

#include "mel.h"
static mel_func generator_init_funcs[] = {
 pattern("generator", "series", VLTgenerator_table, false, "", args(1,3, batarg("",bte),arg("first",bte),arg("limit",bte))),
 pattern("generator", "series", VLTgenerator_table, false, "", args(1,3, batarg("",sht),arg("first",sht),arg("limit",sht))),
 pattern("generator", "series", VLTgenerator_table, false, "", args(1,3, batarg("",int),arg("first",int),arg("limit",int))),
 pattern("generator", "series", VLTgenerator_table, false, "", args(1,3, batarg("",lng),arg("first",lng),arg("limit",lng))),
 pattern("generator", "series", VLTgenerator_table, false, "", args(1,3, batarg("",flt),arg("first",flt),arg("limit",flt))),
 pattern("generator", "series", VLTgenerator_table, false, "", args(1,3, batarg("",dbl),arg("first",dbl),arg("limit",dbl))),
 pattern("generator", "series", VLTgenerator_table, false, "", args(1,4, batarg("",bte),arg("first",bte),arg("limit",bte),arg("step",bte))),
 pattern("generator", "series", VLTgenerator_table, false, "", args(1,4, batarg("",sht),arg("first",sht),arg("limit",sht),arg("step",sht))),
 pattern("generator", "series", VLTgenerator_table, false, "", args(1,4, batarg("",int),arg("first",int),arg("limit",int),arg("step",int))),
 pattern("generator", "series", VLTgenerator_table, false, "", args(1,4, batarg("",lng),arg("first",lng),arg("limit",lng),arg("step",lng))),
 pattern("generator", "series", VLTgenerator_table, false, "", args(1,4, batarg("",flt),arg("first",flt),arg("limit",flt),arg("step",flt))),
 pattern("generator", "series", VLTgenerator_table, false, "Create and materialize a generator table", args(1,4, batarg("",dbl),arg("first",dbl),arg("limit",dbl),arg("step",dbl))),
 pattern("generator", "series", VLTgenerator_table, false, "", args(1,4, batarg("",timestamp),arg("first",timestamp),arg("limit",timestamp),arg("step",lng))),
 pattern("generator", "parameters", VLTgenerator_noop, false, "", args(1,4, batarg("",bte),arg("first",bte),arg("limit",bte),arg("step",bte))),
 pattern("generator", "parameters", VLTgenerator_noop, false, "", args(1,4, batarg("",sht),arg("first",sht),arg("limit",sht),arg("step",sht))),
 pattern("generator", "parameters", VLTgenerator_noop, false, "", args(1,4, batarg("",int),arg("first",int),arg("limit",int),arg("step",int))),
 pattern("generator", "parameters", VLTgenerator_noop, false, "", args(1,4, batarg("",lng),arg("first",lng),arg("limit",lng),arg("step",lng))),
 pattern("generator", "parameters", VLTgenerator_noop, false, "", args(1,4, batarg("",flt),arg("first",flt),arg("limit",flt),arg("step",flt))),
 pattern("generator", "parameters", VLTgenerator_noop, false, "", args(1,4, batarg("",dbl),arg("first",dbl),arg("limit",dbl),arg("step",dbl))),
 pattern("generator", "parameters", VLTgenerator_noop, false, "Retain the table definition, but don't materialize", args(1,4, batarg("",timestamp),arg("first",timestamp),arg("limit",timestamp),arg("step",lng))),
 pattern("generator", "parameters", VLTgenerator_noop, false, "", args(1,3, batarg("",bte),arg("first",bte),arg("limit",bte))),
 pattern("generator", "parameters", VLTgenerator_noop, false, "", args(1,3, batarg("",sht),arg("first",sht),arg("limit",sht))),
 pattern("generator", "parameters", VLTgenerator_noop, false, "", args(1,3, batarg("",int),arg("first",int),arg("limit",int))),
 pattern("generator", "parameters", VLTgenerator_noop, false, "", args(1,3, batarg("",lng),arg("first",lng),arg("limit",lng))),
 pattern("generator", "parameters", VLTgenerator_noop, false, "", args(1,3, batarg("",flt),arg("first",flt),arg("limit",flt))),
 pattern("generator", "parameters", VLTgenerator_noop, false, "", args(1,3, batarg("",dbl),arg("first",dbl),arg("limit",dbl))),
 pattern("generator", "thetaselect", VLTgenerator_thetasubselect, false, "", args(1,5, batarg("",oid),batarg("b",bte),batarg("cnd",oid),arg("low",bte),arg("oper",str))),
 pattern("generator", "thetaselect", VLTgenerator_thetasubselect, false, "", args(1,5, batarg("",oid),batarg("b",sht),batarg("cnd",oid),arg("low",sht),arg("oper",str))),
 pattern("generator", "thetaselect", VLTgenerator_thetasubselect, false, "", args(1,5, batarg("",oid),batarg("b",int),batarg("cnd",oid),arg("low",int),arg("oper",str))),
 pattern("generator", "thetaselect", VLTgenerator_thetasubselect, false, "", args(1,5, batarg("",oid),batarg("b",lng),batarg("cnd",oid),arg("low",lng),arg("oper",str))),
 pattern("generator", "thetaselect", VLTgenerator_thetasubselect, false, "", args(1,5, batarg("",oid),batarg("b",flt),batarg("cnd",oid),arg("low",flt),arg("oper",str))),
 pattern("generator", "thetaselect", VLTgenerator_thetasubselect, false, "", args(1,5, batarg("",oid),batarg("b",dbl),batarg("cnd",oid),arg("low",dbl),arg("oper",str))),
 pattern("generator", "thetaselect", VLTgenerator_thetasubselect, false, "Overloaded selection routine", args(1,5, batarg("",oid),batarg("b",timestamp),batarg("cnd",oid),arg("low",timestamp),arg("oper",str))),
 pattern("generator", "select", VLTgenerator_subselect, false, "", args(1,7, batarg("",oid),batarg("b",bte),arg("low",bte),arg("high",bte),arg("li",bit),arg("hi",bit),arg("anti",bit))),
 pattern("generator", "select", VLTgenerator_subselect, false, "", args(1,7, batarg("",oid),batarg("b",sht),arg("low",sht),arg("high",sht),arg("li",bit),arg("hi",bit),arg("anti",bit))),
 pattern("generator", "select", VLTgenerator_subselect, false, "", args(1,7, batarg("",oid),batarg("b",int),arg("low",int),arg("high",int),arg("li",bit),arg("hi",bit),arg("anti",bit))),
 pattern("generator", "select", VLTgenerator_subselect, false, "", args(1,7, batarg("",oid),batarg("b",lng),arg("low",lng),arg("high",lng),arg("li",bit),arg("hi",bit),arg("anti",bit))),
 pattern("generator", "select", VLTgenerator_subselect, false, "", args(1,7, batarg("",oid),batarg("b",flt),arg("low",flt),arg("high",flt),arg("li",bit),arg("hi",bit),arg("anti",bit))),
 pattern("generator", "select", VLTgenerator_subselect, false, "", args(1,7, batarg("",oid),batarg("b",dbl),arg("low",dbl),arg("high",dbl),arg("li",bit),arg("hi",bit),arg("anti",bit))),
 pattern("generator", "select", VLTgenerator_subselect, false, "Overloaded selection routine", args(1,7, batarg("",oid),batarg("b",timestamp),arg("low",timestamp),arg("high",timestamp),arg("li",bit),arg("hi",bit),arg("anti",bit))),
 pattern("generator", "select", VLTgenerator_subselect, false, "", args(1,8, batarg("",oid),batarg("b",bte),batarg("cand",oid),arg("low",bte),arg("high",bte),arg("li",bit),arg("hi",bit),arg("anti",bit))),
 pattern("generator", "select", VLTgenerator_subselect, false, "", args(1,8, batarg("",oid),batarg("b",sht),batarg("cand",oid),arg("low",sht),arg("high",sht),arg("li",bit),arg("hi",bit),arg("anti",bit))),
 pattern("generator", "select", VLTgenerator_subselect, false, "", args(1,8, batarg("",oid),batarg("b",int),batarg("cand",oid),arg("low",int),arg("high",int),arg("li",bit),arg("hi",bit),arg("anti",bit))),
 pattern("generator", "select", VLTgenerator_subselect, false, "", args(1,8, batarg("",oid),batarg("b",lng),batarg("cand",oid),arg("low",lng),arg("high",lng),arg("li",bit),arg("hi",bit),arg("anti",bit))),
 pattern("generator", "select", VLTgenerator_subselect, false, "", args(1,8, batarg("",oid),batarg("b",flt),batarg("cand",oid),arg("low",flt),arg("high",flt),arg("li",bit),arg("hi",bit),arg("anti",bit))),
 pattern("generator", "select", VLTgenerator_subselect, false, "", args(1,8, batarg("",oid),batarg("b",dbl),batarg("cand",oid),arg("low",dbl),arg("high",dbl),arg("li",bit),arg("hi",bit),arg("anti",bit))),
 pattern("generator", "select", VLTgenerator_subselect, false, "Overloaded selection routine", args(1,8, batarg("",oid),batarg("b",timestamp),batarg("cand",oid),arg("low",timestamp),arg("high",timestamp),arg("li",bit),arg("hi",bit),arg("anti",bit))),
 pattern("generator", "projection", VLTgenerator_projection, false, "", args(1,3, batarg("",bte),batarg("b",oid),batarg("cand",bte))),
 pattern("generator", "projection", VLTgenerator_projection, false, "", args(1,3, batarg("",sht),batarg("b",oid),batarg("cand",sht))),
 pattern("generator", "projection", VLTgenerator_projection, false, "", args(1,3, batarg("",int),batarg("b",oid),batarg("cand",int))),
 pattern("generator", "projection", VLTgenerator_projection, false, "", args(1,3, batarg("",lng),batarg("b",oid),batarg("cand",lng))),
 pattern("generator", "projection", VLTgenerator_projection, false, "", args(1,3, batarg("",flt),batarg("b",oid),batarg("cand",flt))),
 pattern("generator", "projection", VLTgenerator_projection, false, "", args(1,3, batarg("",dbl),batarg("b",oid),batarg("cand",dbl))),
 pattern("generator", "projection", VLTgenerator_projection, false, "Overloaded projection operation", args(1,3, batarg("",timestamp),batarg("b",oid),batarg("cand",timestamp))),
 pattern("generator", "join", VLTgenerator_join, false, "", args(2,4, batarg("l",oid),batarg("r",oid),batarg("b",bte),batarg("gen",bte))),
 pattern("generator", "join", VLTgenerator_join, false, "", args(2,4, batarg("l",oid),batarg("r",oid),batarg("b",sht),batarg("gen",sht))),
 pattern("generator", "join", VLTgenerator_join, false, "", args(2,4, batarg("l",oid),batarg("r",oid),batarg("b",int),batarg("gen",int))),
 pattern("generator", "join", VLTgenerator_join, false, "", args(2,4, batarg("l",oid),batarg("r",oid),batarg("b",lng),batarg("gen",lng))),
 pattern("generator", "join", VLTgenerator_join, false, "", args(2,4, batarg("l",oid),batarg("r",oid),batarg("b",flt),batarg("gen",flt))),
 pattern("generator", "join", VLTgenerator_join, false, "Overloaded join operation", args(2,4, batarg("l",oid),batarg("r",oid),batarg("b",dbl),batarg("gen",dbl))),
 pattern("generator", "join", VLTgenerator_rangejoin, false, "", args(2,7, batarg("l",oid),batarg("r",oid),batarg("gen",bte),batarg("low",bte),batarg("hgh",bte),arg("li",bit),arg("ri",bit))),
 pattern("generator", "join", VLTgenerator_rangejoin, false, "", args(2,7, batarg("l",oid),batarg("r",oid),batarg("gen",sht),batarg("low",sht),batarg("hgh",sht),arg("li",bit),arg("ri",bit))),
 pattern("generator", "join", VLTgenerator_rangejoin, false, "", args(2,7, batarg("l",oid),batarg("r",oid),batarg("gen",int),batarg("low",int),batarg("hgh",int),arg("li",bit),arg("ri",bit))),
 pattern("generator", "join", VLTgenerator_rangejoin, false, "", args(2,7, batarg("l",oid),batarg("r",oid),batarg("gen",lng),batarg("low",lng),batarg("hgh",lng),arg("li",bit),arg("ri",bit))),
 pattern("generator", "join", VLTgenerator_rangejoin, false, "", args(2,7, batarg("l",oid),batarg("r",oid),batarg("gen",flt),batarg("low",flt),batarg("hgh",flt),arg("li",bit),arg("ri",bit))),
 pattern("generator", "join", VLTgenerator_rangejoin, false, "Overloaded range join operation", args(2,7, batarg("l",oid),batarg("r",oid),batarg("gen",dbl),batarg("low",dbl),batarg("hgh",dbl),arg("li",bit),arg("ri",bit))),
#ifdef HAVE_HGE
 pattern("generator", "series", VLTgenerator_table, false, "", args(1,3, batarg("",hge),arg("first",hge),arg("limit",hge))),
 pattern("generator", "series", VLTgenerator_table, false, "Create and materialize a generator table", args(1,4, batarg("",hge),arg("first",hge),arg("limit",hge),arg("step",hge))),
 pattern("generator", "parameters", VLTgenerator_noop, false, "Retain the table definition, but don't materialize", args(1,4, batarg("",hge),arg("first",hge),arg("limit",hge),arg("step",hge))),
 pattern("generator", "parameters", VLTgenerator_noop, false, "", args(1,3, batarg("",hge),arg("first",hge),arg("limit",hge))),
 pattern("generator", "thetaselect", VLTgenerator_thetasubselect, false, "Overloaded selection routine", args(1,5, batarg("",oid),batarg("b",hge),batarg("cnd",oid),arg("low",hge),arg("oper",str))),
 pattern("generator", "select", VLTgenerator_subselect, false, "Overloaded selection routine", args(1,7, batarg("",oid),batarg("b",hge),arg("low",hge),arg("high",hge),arg("li",bit),arg("hi",bit),arg("anti",bit))),
 pattern("generator", "select", VLTgenerator_subselect, false, "Overloaded selection routine", args(1,8, batarg("",oid),batarg("b",hge),batarg("cand",oid),arg("low",hge),arg("high",hge),arg("li",bit),arg("hi",bit),arg("anti",bit))),
 pattern("generator", "projection", VLTgenerator_projection, false, "Overloaded projection operation", args(1,3, batarg("",hge),batarg("b",oid),batarg("cand",hge))),
 pattern("generator", "join", VLTgenerator_join, false, "Overloaded join operation", args(2,4, batarg("l",oid),batarg("r",oid),batarg("b",hge),batarg("gen",hge))),
#endif
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_generator_mal)
{ mal_module("generator", NULL, generator_init_funcs); }
