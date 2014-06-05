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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * (c) Martin Kersten, Sjoerd Mullender
 * Series generating module for integer, decimal, real, double and timestamps.
 */

#include "monetdb_config.h"
#include "opt_prelude.h"
#include "generator.h"
#include "mtime.h"
#include "math.h"


static int
assignedOnce(MalBlkPtr mb, int varid)
{
	InstrPtr p;
	int i,j, c=0;

	for(i = 1; i< mb->stop; i++){
		p = getInstrPtr(mb,i);
		for( j = 0; j < p->retc; j++)
		if( getArg(p,j) == varid){
			c++;
			break;
		}
	}
	return c == 1;
}
static int
useCount(MalBlkPtr mb, int varid)
{
	InstrPtr p;
	int i,j, d,c=0;

	for(i = 1; i< mb->stop; i++){
		p = getInstrPtr(mb,i);
		d= 0;
		for( j = p->retc; j < p->argc; j++)
		if( getArg(p,j) == varid)
			d++;
		c += d > 0;
	}
	return c;
}

static int
VLTgenerator_optimizer(Client cntxt, MalBlkPtr mb)
{
	InstrPtr p,q;
	int i,j, used= 0, cases;
	str vaultRef = putName("vault",5);
	str generateRef = putName("generate_series",15);
	str generatorRef = putName("generator",9);
	str noopRef = putName("noop",4);

	for( i=1; i < mb->stop; i++){
		p = getInstrPtr(mb,i);
		if ( getModuleId(p) == vaultRef && getFunctionId(p) == generateRef){
			/* found a target for propagation */
			if ( assignedOnce(mb, getArg(p,0)) ){
				cases = useCount(mb, getArg(p,0));
				for( j = i+1; j< mb->stop; j++){
					q = getInstrPtr(mb,j);
					if ( getModuleId(q) == algebraRef && getFunctionId(q) == subselectRef && getArg(q,1) == getArg(p,0)){
						setModuleId(q, generatorRef);
						typeChecker(cntxt->fdout, cntxt->nspace, mb, q, TRUE);
						used++;
					}
					if ( getModuleId(q) == algebraRef && getFunctionId(q) == thetasubselectRef && getArg(q,1) == getArg(p,0)){
						setModuleId(q, generatorRef);
						typeChecker(cntxt->fdout, cntxt->nspace, mb, q, TRUE);
						used++;
					}
					if ( getModuleId(q) == algebraRef && getFunctionId(q) == leftfetchjoinRef && getArg(q,2) == getArg(p,0)){
						// projection over a series
						setModuleId(q, generatorRef);
						typeChecker(cntxt->fdout, cntxt->nspace, mb, q, TRUE);
						used++;
					}
					if ( getModuleId(q) == algebraRef && getFunctionId(q) == joinRef && (getArg(q,2) == getArg(p,0) || getArg(q,3) == getArg(p,0))){
						// projection over a series
						setModuleId(q, generatorRef);
						typeChecker(cntxt->fdout, cntxt->nspace, mb, q, TRUE);
						used++;
					}
					if ( getModuleId(q) == languageRef && getFunctionId(q) == passRef && getArg(q,1) == getArg(p,0))
						// nothing happens in this instruction
						used++;
				}
				// fix the original, only when all use cases are replaced by the overloaded function
				if(used == cases){
					setModuleId(p, generatorRef);
					setFunctionId(p, noopRef);
					typeChecker(cntxt->fdout, cntxt->nspace, mb, p, TRUE);
				} else used = 0;
#ifdef VLT_DEBUG
				mnstr_printf(cntxt->fdout,"#generator target %d cases %d used %d error %d\n",getArg(p,0), cases, used, p->typechk);
#endif
			}
		}
	}
#ifdef VLT_DEBUG
	printFunction(cntxt->fdout,mb,0,LIST_MAL_ALL);
#endif
	return used== 0;
}

/*
 * The noop simply means that we keep the properties for the generator object.
 */
#define VLTnoop(TPE)\
		{	TPE s;\
			s = pci->argc == 3 ? 1: *(TPE*) getArgReference(stk,pci, 3);\
			if( s == 0) zeroerror++;\
			if( s == TPE##_nil) nullerr++;\
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
	case TYPE_flt: VLTnoop(flt); break;
	case TYPE_dbl: VLTnoop(dbl); break;
	default:
	{	timestamp s;
		if (tpe == TYPE_timestamp){
			s = *(timestamp*) getArgReference(stk,pci, 3);
			if( timestamp_isnil(s)) nullerr++;
		} else throw(MAL,"vault.generator","unknown data type %d", getArgType(mb,pci,1));
	}
	}
	if( zeroerror)
		throw(MAL,"vault.generator","zero step size not allowed");
	if( nullerr)
		throw(MAL,"vault.generator","null step size not allowed");
	return MAL_SUCCEED;
}

/*
 * The base line consists of materializing the generator iterator value
 */
#define VLTmaterialize(TPE)						\
	do {								\
		TPE *v, f, l, s;					\
		f = *(TPE*) getArgReference(stk, pci, 1);		\
		l = *(TPE*) getArgReference(stk, pci, 2);		\
		s = pci->argc == 3 ? 1 : *(TPE*) getArgReference(stk, pci, 3); \
		if (s == 0 || (s > 0 && f > l) || (s < 0 && f < l))	\
			throw(MAL, "generator.table",			\
			      "illegal generator arguments");		\
		n = (lng) ((l - f) / s);				\
		assert(n >= 0);						\
		if (n * s + f != l)					\
			n++;						\
		bn = BATnew(TYPE_void, TYPE_##TPE, (BUN) n);		\
		if (bn == NULL)						\
			throw(MAL, "generator.table", MAL_MALLOC_FAIL);	\
		v = (TPE*) Tloc(bn, BUNfirst(bn));			\
		for (c = 0; c < n; c++)					\
			*v++ = (TPE) (f + c * s);			\
		bn->tsorted = s > 0 || n <= 1;				\
		bn->trevsorted = s < 0 || n <= 1;			\
	} while (0)

str
VLTgenerator_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	lng c, n;
	BAT *bn;
	str msg;
	int tpe;
	(void) cntxt;

	if ((msg = VLTgenerator_noop(cntxt, mb, stk, pci)) != MAL_SUCCEED)
		return msg;
	if (VLTgenerator_optimizer(cntxt, mb) == 0)
		return MAL_SUCCEED;

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
	case TYPE_wrd:
		VLTmaterialize(wrd);
		break;
	case TYPE_lng:
		VLTmaterialize(lng);
		break;
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
				       &stk->stk[pci->argv[2]]) == GDK_FAIL)
				throw(MAL, "generator.table",
				      "illegal generator arguments");
			f = *(timestamp *) getArgReference(stk, pci, 1);
			l = *(timestamp *) getArgReference(stk, pci, 2);
			s = *(lng *) getArgReference(stk, pci, 3);
			if (s == 0 ||
			    (s > 0 && ret.val.btval > 0) ||
			    (s < 0 && ret.val.btval < 0))
				throw(MAL, "generator.table",
				      "illegal generator arguments");
			/* casting one value to lng causes the whole
			 * computation to be done as lng, reducing the
			 * risk of overflow */
			n = (BUN) ((((lng) l.days - f.days) * 24*60*60*1000 + l.msecs - f.msecs) / s);
			bn = BATnew(TYPE_void, tpe, n + 1);
			if (bn == NULL)
				throw(MAL, "generator.table", MAL_MALLOC_FAIL);
			v = (timestamp *) Tloc(bn, BUNfirst(bn));
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
	BATseqbase(bn, 0);
	bn->tkey = 1;
	bn->T->nil = 0;
	bn->T->nonil = 1;
	*(bat*) getArgReference(stk, pci, 0) = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

/*
 * Selection over the generator table does not require a materialization of the table
 * An optimizer can replace the subselect directly into a generator specific one.
 * The target to look for is vault.generator(A1,A2,A3)
 * We need the generator parameters, which are injected to replace the target column
 */
static InstrPtr
findLastAssign(MalBlkPtr mb, InstrPtr pci, int target)
{
	InstrPtr q, p = NULL;
	int i;
	str vaultRef = putName("generator",9);

	for (i = 1; i < mb->stop; i++) {
		q = getInstrPtr(mb, i);
		if (q->argv[0] == target && getModuleId(q) == vaultRef)
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
		f = * (TPE *) getArgReference(stk, p, 1);		\
		l = * (TPE *) getArgReference(stk, p, 2);		\
		s = p->argc == 3 ? 1 : * (TPE *) getArgReference(stk, p, 3); \
		if (s == 0 || (s > 0 && f > l) || (s < 0 && f < l))	\
			throw(MAL, "generator.subselect",		\
			      "illegal generator arguments");		\
		n = (lng) (((TPE2) l - (TPE2) f) / (TPE2) s);		\
		assert(n >= 0);						\
		if (n * s + f != l)					\
			n++;						\
									\
		low = * (TPE *) getArgReference(stk, pci, i);		\
		hgh = * (TPE *) getArgReference(stk, pci, i + 1);	\
									\
		if (low == TPE##_nil && hgh == TPE##_nil) {		\
			if (li && hi && !anti) {			\
				/* match NILs (of which there aren't */	\
				/* any) */				\
				o1 = o2 = 0;				\
			} else {					\
				/* match all non-NIL values, */		\
				/* i.e. everything */			\
				o1 = 0;					\
				o2 = n;					\
			}						\
		} else if (s > 0) {					\
			if (low == TPE##_nil || low < f)		\
				o1 = 0;					\
			else {						\
				o1 = (lng) (((TPE2) low - (TPE2) f) / (TPE2) s); \
				if (f + o1 * s < low ||			\
				    (!li && f + o1 * s == low))		\
					o1++;				\
			}						\
			if (hgh == TPE##_nil)				\
				o2 = n;					\
			else if (hgh < f)				\
				o2 = 0;					\
			else {						\
				o2 = (lng) (((TPE2) hgh - (TPE2) f) / (TPE2) s); \
				if ((hi && f + o2 * s == hgh) ||	\
				    f + o2 * s < hgh)			\
					o2++;				\
			}						\
		} else {						\
			if (low == TPE##_nil)				\
				o2 = n;					\
			else if (low > f)				\
				o2 = 0;					\
			else {						\
				o2 = (lng) (((TPE2) low - (TPE2) f) / (TPE2) s); \
				if ((li && f + o2 * s == low) ||	\
				    f + o2 * s > low)			\
					o2++;				\
			}						\
			if (hgh == TPE##_nil || hgh > f)		\
				o1 = 0;					\
			else {						\
				o1 = (lng) (((TPE2) hgh - (TPE2) f) / (TPE2) s); \
				if ((!hi && f + o1 * s == hgh) ||	\
				    f + o1 * s > hgh)			\
					o1++;				\
			}						\
		}							\
	} while (0)

str
VLTgenerator_subselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int li, hi, anti, i;
	lng o1, o2;
	lng n;
	BAT *bn, *cand = NULL;
	InstrPtr p;

	(void) cntxt;
	p = findLastAssign(mb, pci, pci->argv[1]);
	if (p == NULL)
		throw(MAL, "generator.subselect",
		      "Could not locate definition for object");

	if (pci->argc == 8) {	/* candidate list included */
		bat candid = *(bat*) getArgReference(stk, pci, 2);
		if (candid) {
			cand = BATdescriptor(candid);
			if (cand == NULL)
				throw(MAL, "generator.subselect",
				      RUNTIME_OBJECT_MISSING);
		}
		i = 3;
	} else
		i = 2;

	li = * (bit *) getArgReference(stk, pci, i + 2);
	hi = * (bit *) getArgReference(stk, pci, i + 3);
	anti = * (bit *) getArgReference(stk, pci, i + 4);

	switch (getArgType(mb, pci, i)) {
	case TYPE_bte: calculate_range(bte, int); break;
	case TYPE_sht: calculate_range(sht, int); break;
	case TYPE_int: calculate_range(int, lng); break;
	case TYPE_wrd: calculate_range(wrd, lng); break;
	case TYPE_lng: calculate_range(lng, lng); break;
	case TYPE_flt: calculate_range(flt, dbl); break;
	case TYPE_dbl: calculate_range(dbl, dbl); break;
	default:
		/* timestamp to be implemented */
		throw(MAL, "generator.subselect", "unsupported type");
	}
	if (o1 > n)
		o1 = n;
	if (o2 > n)
		o2 = n;
	assert(o1 >= 0);
	assert(o1 <= o2);
	assert(o2 - o1 <= n);
	if (anti && o1 == o2) {
		o1 = 0;
		o2 = n;
		anti = 0;
	}
	if (cand) {
		oid o;
		o = (oid) o1;
		o1 = (lng) SORTfndfirst(cand, &o);
		o = (oid) o2;
		o2 = (lng) SORTfndfirst(cand, &o);
		n = (lng) BATcount(cand);
		if (anti && o1 < o2) {
			bn = BATnew(TYPE_void, TYPE_oid, (BUN) (n - (o2 - o1)));
			if (bn) {
				oid *op = (oid *) Tloc(bn, BUNfirst(bn));
				const oid *cp = (const oid *) Tloc(cand, BUNfirst(cand));
				BATsetcount(bn, (BUN) (n - (o2 - o1)));
				BATseqbase(bn, 0);
				bn->T->nil = 0;
				bn->T->nonil = 1;
				bn->tsorted = 1;
				bn->trevsorted = BATcount(bn) <= 1;
				bn->tkey = 1;
				for (o = 0; o < (oid) o1; o++)
					*op++ = cp[o];
				for (o = (oid) o2; o < (oid) n; o++)
					*op++ = cp[o];
			}
		} else {
			if (anti) {
				o1 = 0;
				o2 = n;
			}
			bn = BATslice(cand, (BUN) o1, (BUN) o2);
		}
		BBPreleaseref(cand->batCacheid);
		if (bn == NULL)
			throw(MAL, "generator.subselect",
			      MAL_MALLOC_FAIL);
	} else {
		if (anti) {
			lng o;
			oid *op;

			bn = BATnew(TYPE_void, TYPE_oid, (BUN) (n - (o2 - o1)));
			if (bn == NULL)
				throw(MAL, "generator.subselect",
				      MAL_MALLOC_FAIL);
			BATsetcount(bn, (BUN) (n - (o2 - o1)));
			BATseqbase(bn, 0);
			op = (oid *) Tloc(bn, BUNfirst(bn));
			for (o = 0; o < o1; o++)
				*op++ = (oid) o;
			for (o = o2; o < n; o++)
				*op++ = (oid) o;
			bn->T->nil = 0;
			bn->T->nonil = 1;
			bn->tsorted = 1;
			bn->trevsorted = BATcount(bn) <= 1;
			bn->tkey = 1;
		} else {
			bn = BATnew(TYPE_void, TYPE_void, (BUN) (o2 - o1));
			if (bn == NULL)
				throw(MAL, "generator.subselect",
				      MAL_MALLOC_FAIL);
			BATsetcount(bn, o2 - o1);
			BATseqbase(bn, 0);
			BATseqbase(BATmirror(bn), o1);
		}
	}
	* (bat *) getArgReference(stk, pci, 0) = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}


#define VLTthetasubselect(TPE) {\
	TPE f,l,s, low, hgh;\
	oid *v;\
	f = *(TPE*) getArgReference(stk,p, 1);\
	l = *(TPE*) getArgReference(stk,p, 2);\
	s = pci->argc == 3 ? 1:  *(TPE*) getArgReference(stk,p, 3);\
	cap = (l>f ? (l-f+abs(s))/abs(s):(f-l+abs(s))/abs(s));\
	bn = BATnew(TYPE_void, TYPE_oid, cap);\
	if( bn == NULL)\
		throw(MAL,"generator.thetasubselect",MAL_MALLOC_FAIL);\
	low= hgh = TPE##_nil;\
	v = (oid*) Tloc(bn,BUNfirst(bn));\
	if ( strcmp(oper,"<") == 0){\
		hgh= *(TPE*) getArgReference(stk,pci,idx);\
	} else\
	if ( strcmp(oper,"<=") == 0){\
		hgh= *(TPE*) getArgReference(stk,pci,idx) +1;\
	} else\
	if ( strcmp(oper,">") == 0){\
		low= *(TPE*) getArgReference(stk,pci,idx)+1;\
	} else\
	if ( strcmp(oper,">=") == 0){\
		low= *(TPE*) getArgReference(stk,pci,idx);\
	} else\
	if ( strcmp(oper,"!=") == 0){\
		hgh= low= *(TPE*) getArgReference(stk,pci,idx);\
		anti++;\
	} else\
	if ( strcmp(oper,"==") == 0){\
		hgh= low= *(TPE*) getArgReference(stk,pci,idx);\
	} else\
		throw(MAL,"generator.thetasubselect","Unknown operator");\
	if( f < l && s > 0){\
		for(; f<l; f+= s, o++)\
		if( ((low == TPE##_nil || f >= low) && (f <= hgh || hgh == TPE##_nil)) || anti){\
			*v++ = o;\
			c++;\
		} \
	} else\
	if( f > l && s < 0){\
		for(; f>l; f+= s, o++)\
		if( ((low == TPE##_nil || f >= low) && (f <= hgh || hgh == TPE##_nil)) || anti){\
			*v++ = o;\
			c++;\
		} \
	} else\
		throw(MAL,"generator.thetasubselect","illegal generator arguments");\
}


str VLTgenerator_thetasubselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx, cndid =0, c= 0, anti =0,tpe;
	BAT *cnd = 0, *bn = NULL;
	BUN cap;
	oid o = 0;
	InstrPtr p;
	str oper, msg= MAL_SUCCEED;

	(void) cntxt;
	p = findLastAssign(mb,pci,pci->argv[1]);
	if( p == NULL)
		throw(MAL,"generator.thetasubselect","Could not locate definition for object");

	if( pci->argc == 5){ // candidate list included
		cndid = *(int*) getArgReference(stk,pci, 2);
		if( cndid){
			cnd = BATdescriptor(cndid);
			if( cnd == NULL)
				throw(MAL,"generator.subselect",RUNTIME_OBJECT_MISSING);
		} else throw(MAL,"generator.subselect","candidate list not implemented");
		idx = 3;
	} else idx = 2;
	oper= *(str*) getArgReference(stk,pci,idx+1);

	switch( tpe =getArgType(mb,pci,idx)){
	case TYPE_bte: VLTthetasubselect(bte);break;
	case TYPE_int: VLTthetasubselect(int);break;
	case TYPE_sht: VLTthetasubselect(sht);break;
	case TYPE_lng: VLTthetasubselect(lng);break;
	case TYPE_flt: VLTthetasubselect(flt);break;
	case TYPE_dbl: VLTthetasubselect(dbl);break;
	default:
		if ( tpe == TYPE_timestamp){
			timestamp f,l, low, hgh;
			lng  s;
			oid *v;
			f = *(timestamp*) getArgReference(stk,p, 1);
			l = *(timestamp*) getArgReference(stk,p, 2);
			s = *(lng*) getArgReference(stk,p, 3);
			low = *(timestamp*) getArgReference(stk,pci, idx);
			hgh = *(timestamp*) getArgReference(stk,pci, idx+1);

			cap = l.days > f.days ? ((l.days -f.days)*24*60*60 +abs(s))/abs(s):((f.days -l.days)*24*60*60 +abs(s))/abs(s);
			bn = BATnew(TYPE_void, TYPE_oid, cap);
			if( bn == NULL)
				throw(MAL,"generator.thetasubselect",MAL_MALLOC_FAIL);

			if( timestamp_isnil(low) ){
				low = f;
			}
			if( timestamp_isnil(hgh)){
				hgh = l;
			}
			v = (oid*) Tloc(bn,BUNfirst(bn));

			if ( strcmp(oper,"<") == 0){
				hgh= *(timestamp*) getArgReference(stk,pci,idx);
			} else
			if ( strcmp(oper,"<=") == 0){
				hgh= *(timestamp*) getArgReference(stk,pci,idx) ;
				hgh.msecs++;
			} else
			if ( strcmp(oper,">") == 0){
				low= *(timestamp*) getArgReference(stk,pci,idx);
				low.msecs++;
			} else
			if ( strcmp(oper,">=") == 0){
				low= *(timestamp*) getArgReference(stk,pci,idx);
			} else
			if ( strcmp(oper,"!=") == 0){
				hgh= low= *(timestamp*) getArgReference(stk,pci,idx);
				anti++;
			} else
			if ( strcmp(oper,"==") == 0){
				hgh= low= *(timestamp*) getArgReference(stk,pci,idx);
			} else
				throw(MAL,"generator.thetasubselect","Unknown operator");

			if( (f.days < l.days || (f.days = l.days && f.msecs <l.msecs)) && s > 0){
				for(; f.days<l.days || (f.days == l.days && f.msecs <l.msecs); o++)
				if( ((timestamp_isnil(low) || (f.days > low.days || (f.days == l.days && f.msecs >= l.msecs)) ) && ((f.days<hgh.days|| (f.days== hgh.days && f.msecs < hgh.msecs))  || timestamp_isnil(hgh))) || anti){
					*v++ = o;
					if( (msg = MTIMEtimestamp_add(&f, &f, &s)) != MAL_SUCCEED)
						return msg;
					c++;
				} 
			} else
			if( (f.days > l.days || (f.days = l.days && f.msecs >= l.msecs)) && s < 0){
				for(; f.days>l.days || (f.days == l.days && f.msecs > l.msecs);o++ )
				if( ((timestamp_isnil(low) || (f.days > low.days || (f.days == l.days && f.msecs >= l.msecs)) ) && ((f.days<hgh.days|| (f.days== hgh.days && f.msecs < hgh.msecs))  || timestamp_isnil(hgh))) || anti){
					*v++ = o;
					if( (msg = MTIMEtimestamp_add(&f, &f, &s)) != MAL_SUCCEED)
						return msg;
					c++;
				} 
			} else
				throw(MAL,"generator.subselect","illegal generator arguments");
		}
	}

	if( cndid)
		BBPreleaseref(cndid);
	if( bn){
		BATsetcount(bn,c);
		bn->hdense = 1;
		bn->hseqbase = 0;
		bn->hkey = 1;
		BATderiveProps(bn,0);
		BBPkeepref(*(int*)getArgReference(stk,pci,0)= bn->batCacheid);
	}
	return MAL_SUCCEED;
}

#define VLTleftfetchjoin(TPE) {\
	TPE f,l,s, val;\
	TPE *v;\
	f = *(TPE*) getArgReference(stk,p, 1);\
	l = *(TPE*) getArgReference(stk,p, 2);\
	s = *(TPE*) getArgReference(stk,p, 3);\
	bn = BATnew(TYPE_void, TYPE_##TPE, cnt);\
	if( bn == NULL){\
		BBPreleaseref(bid);\
		throw(MAL,"generator.thetasubselect",MAL_MALLOC_FAIL);\
	}\
	v = (TPE*) Tloc(bn,BUNfirst(bn));\
	for(; cnt-- > 0; genoid++, o++){\
		val = f + ((TPE) ( b->ttype == TYPE_void?genoid:*o)) * s;\
		if ( val < f || val >= l)\
			continue;\
		*v++ = val;\
		c++;\
	}\
}

str VLTgenerator_leftfetchjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int bid =0, c= 0, tpe;
	BAT *b, *bn = NULL;
	BUN cnt;
	oid *o =0, genoid= 0;
	InstrPtr p;
	str msg;

	(void) cntxt;
	p = findLastAssign(mb,pci,pci->argv[2]);
	// if it does not exist we should fall back to the ordinary join to try
	if( p == NULL)
		throw(MAL,"generator.leftfetchjoin","Could not locate definition for object");

	b = BATdescriptor(bid = *(int*) getArgReference(stk,pci,1));
	if( b == NULL)
		throw(MAL,"generator.leftfetchjoin",RUNTIME_OBJECT_MISSING);
	cnt = BATcount(b);
	if ( b->ttype == TYPE_void)
		genoid = b->tseqbase;
	else
		o = (oid*) Tloc(b,BUNfirst(b));

	/* the actual code to perform a leftfetchjoin over generators */
	switch( tpe = getArgType(mb,p,1)){
	case TYPE_bte:  VLTleftfetchjoin(bte); break;
	case TYPE_sht:  VLTleftfetchjoin(sht); break;
	case TYPE_int:  VLTleftfetchjoin(int); break;
	case TYPE_lng:  VLTleftfetchjoin(lng); break;
	case TYPE_flt:  VLTleftfetchjoin(flt); break;
	case TYPE_dbl:  VLTleftfetchjoin(dbl); break;
	default:
		if ( tpe == TYPE_timestamp){
			timestamp f,l, val;
			lng s,t;
			timestamp *v;
			f = *(timestamp*) getArgReference(stk,p, 1);
			l = *(timestamp*) getArgReference(stk,p, 2);
			s =  *(lng*) getArgReference(stk,p, 3);

			bn = BATnew(TYPE_void, tpe, cnt);
			if( bn == NULL){
				BBPreleaseref(bid);
				throw(MAL,"generator.thetasubselect",MAL_MALLOC_FAIL);
			}

			v = (timestamp*) Tloc(bn,BUNfirst(bn));

			for(; cnt-- > 0; genoid++, o++){
				t = ((lng) ( b->ttype == TYPE_void?genoid:*o)) * s;
				if( (msg = MTIMEtimestamp_add(&val, &f, &t)) != MAL_SUCCEED)
					return msg;

				if ((val.days < f.days || (val.days == f.days && val.msecs < f.msecs)) || ((val.days>l.days || (val.days== l.days && val.msecs >= l.msecs)))  || timestamp_isnil(val))
					continue;
				*v++ = val;
				c++;
			}
		}
	}

	/* adminstrative wrapup of the leftfetchjoin */
	BBPreleaseref(bid);
	if( bn){
		BATsetcount(bn,c);
		bn->hdense = 1;
		bn->hseqbase = 0;
		bn->hkey = 1;
		BATderiveProps(bn,0);
		BBPkeepref(*(int*)getArgReference(stk,pci,0)= bn->batCacheid);
	}
	return MAL_SUCCEED;
}

/* The operands of a join operation can either be defined on a generator */
#define VLTjoin(TPE) \
	{ TPE f,l,s; TPE *v; BUN w;\
	f = *(TPE*) getArgReference(stk,p, 1);\
	l = *(TPE*) getArgReference(stk,p, 2);\
	s = *(TPE*) getArgReference(stk,p, 3);\
	v = (TPE*) Tloc(bl,BUNfirst(bl));\
	for( ; cnt >0; cnt--,genoid++,v++){\
		w = (BUN) floor( (double)((*v -f)/s));\
		if ( *v >= f && *v < l && f + (TPE)(w * s) == *v ){\
			*or++ = w;\
			*ol++ = genoid;\
			c++;\
		}\
	} }\

str VLTgenerator_join(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT  *b, *bl = NULL, *br = NULL, *bln = NULL, *brn= NULL;
	BUN cnt,c =0;
	oid genoid= 0, *ol, *or;
	int tpe;
	InstrPtr p = NULL, q = NULL;
	str msg = MAL_SUCCEED;

	(void) cntxt;
	// we assume at most one of the arguments to refer to the generator
	p = findLastAssign(mb,pci,pci->argv[2]);
	if( p == NULL){
		bl = BATdescriptor(*(int*) getArgReference(stk,pci,2));
		if( bl == NULL)
			throw(MAL,"generator.join",RUNTIME_OBJECT_MISSING);
	}
	q = findLastAssign(mb,pci,pci->argv[3]);
	if ( q == NULL){
		br = BATdescriptor(*(int*) getArgReference(stk,pci,3));
		if( br == NULL){
			BBPreleaseref(bl->batCacheid);
			throw(MAL,"generator.join",RUNTIME_OBJECT_MISSING);
		}
	}

	// in case of both generators materialize the 'smallest' one first
	// or implement more knowledge, postponed
	assert(!( p && q));
	assert(p || q);

mnstr_printf(cntxt->fdout,"Left? %d Right %d\n", p != NULL, q!= NULL);
	// switch roles to have a single target bat[:oid,:any] designated 
	// by b and reference instruction p for the generator
	b = q? bl : br;
	p = q? q : p;
	cnt = BATcount(b);
	tpe = b->ttype;
	genoid = b->tseqbase;
	
	bln = BATnew(TYPE_void,TYPE_oid, cnt);
	brn = BATnew(TYPE_void,TYPE_oid, cnt);
mnstr_printf(cntxt->fdout,"lid? %d rid %d\n", bln->batCacheid, brn->batCacheid);
	if( bln == NULL || brn == NULL){
		if(bln) BBPreleaseref(bln->batCacheid);
		if(brn) BBPreleaseref(brn->batCacheid);
		if(bl) BBPreleaseref(bl->batCacheid);
		if(br) BBPreleaseref(br->batCacheid);
		throw(MAL,"generator.join",MAL_MALLOC_FAIL);
	}
	ol = (oid*) Tloc(bln,BUNfirst(bln));
	or = (oid*) Tloc(brn,BUNfirst(brn));

	/* The actual join code for generators be injected here */
	switch(tpe){
	case TYPE_bte: //VLTjoin(bte); break; 
	{ bte f,l,s; bte *v; BUN w;
	f = *(bte*) getArgReference(stk,p, 1);
	l = *(bte*) getArgReference(stk,p, 2);
	s = *(bte*) getArgReference(stk,p, 3);
	v = (bte*) Tloc(b,BUNfirst(b));
	for( ; cnt >0; cnt--,genoid++,v++){
		w = (BUN) floor((*v -f)/s);
		if ( *v >= f && *v < l && f + (bte)( w * s) == *v ){
			*or++ = (oid) w;
			*ol++ = genoid;
			c++;
		}
	} }
	break;
	case TYPE_sht: VLTjoin(sht); break;
	case TYPE_int: VLTjoin(int); break;
	case TYPE_lng: VLTjoin(lng); break;
	case TYPE_flt: VLTjoin(flt); break;
	case TYPE_dbl: VLTjoin(dbl); break;
/* to be fixed
	default:
	if( tpe == TYPE_timestamp){ 
		timestamp f,l,val;
		timestamp *v,w;
		lng s,offset;

		f = *(timestamp*) getArgReference(stk,p, 1);
		l = *(timestamp*) getArgReference(stk,p, 2);
		s = *(lng*) getArgReference(stk,p, 3);
		v = (timestamp*) Tloc(bl,BUNfirst(bl));
		for( ; cnt >0; cnt--,genoid++, v++){
			offset = ((lng)*o) * s;
			if( (msg = MTIMEtimestamp_add(&val, &f, &offset)) != MAL_SUCCEED)
				return msg;

			if ( w * s == *v && (oid) w < lo){
				*or++ = (oid) w;
				*ol++ = *o;
				c++;
			}
		}
		}
*/
	}

	BATsetcount(bln,c);
	bln->hdense = 1;
	bln->hseqbase = 0;
	bln->hkey = 1;
	BATderiveProps(bln,0);
	
	BATsetcount(brn,c);
	brn->hdense = 1;
	brn->hseqbase = 0;
	brn->hkey = 1;
	BATderiveProps(brn,0);
	if( q){
		BBPkeepref(*(int*)getArgReference(stk,pci,0)= brn->batCacheid);
		BBPkeepref(*(int*)getArgReference(stk,pci,1)= bln->batCacheid);
	} else {
		// switch their role
		BBPkeepref(*(int*)getArgReference(stk,pci,0)= bln->batCacheid);
		BBPkeepref(*(int*)getArgReference(stk,pci,1)= brn->batCacheid);
	}
	return msg;
/*
wrapup:
	if(bl) BBPreleaseRef(bl->batCacheid);
	BBPreleaseRef(bln->batCacheid);
	if(br)BBPreleaseRef(br->batCacheid);
	BBPreleaseRef(brn->batCacheid);
	return msg;
*/
}
