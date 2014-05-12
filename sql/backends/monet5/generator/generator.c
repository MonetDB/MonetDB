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
 * (c) Martin Kersten
 * Series generating module for integer, decimal, real, double and timestamps.
 */

#include "monetdb_config.h"
#include "opt_prelude.h"
#include "generator.h"
#include "mtime.h"


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
		c += d;
	}
	return c == 1;
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
						setModuleId(q, generatorRef);
						typeChecker(cntxt->fdout, cntxt->nspace, mb, q, TRUE);
						used++;
					}
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
#define VLTmaterialize(TPE) {\
			TPE *v,f,l,s;\
			f = *(TPE*) getArgReference(stk,pci, 1);\
			l = *(TPE*) getArgReference(stk,pci, 2);\
			s = pci->argc == 3 ? 1:  *(TPE*) getArgReference(stk,pci, 3);\
			bn = BATnew(TYPE_void, TYPE_##TPE, (l>f ? (l-f+abs(s))/abs(s):(f-l+abs(s))/abs(s)));\
			if( bn == NULL)\
				throw(MAL,"generator.table",MAL_MALLOC_FAIL);\
			v = (TPE*) Tloc(bn,BUNfirst(bn));\
			if( f < l && s > 0)\
				for(; f<l; f+= s){\
					*v++ = f;\
					c++;\
				}\
			else\
			if( f > l && s < 0)\
				for(; f>l; f+= s){\
					*v++ = f;\
					c++;\
				}\
			else\
				throw(MAL,"generator.table","illegal generator arguments");\
		}

str
VLTgenerator_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BUN c= 0;
	BAT *bn= 0;
	str msg= MAL_SUCCEED;
	int tpe;
	(void) cntxt;

	if ( (msg= VLTgenerator_noop(cntxt,mb,stk,pci)) )
		return msg;
	if( VLTgenerator_optimizer(cntxt,mb) == 0 )
		return MAL_SUCCEED;

	switch( tpe = getArgType(mb,pci,1)){
	case TYPE_bte: VLTmaterialize(bte); break;
	case TYPE_sht: VLTmaterialize(sht); break;
	case TYPE_int: VLTmaterialize(int); break;
	case TYPE_lng: VLTmaterialize(lng); break;
	case TYPE_flt: VLTmaterialize(flt); break;
	case TYPE_dbl: VLTmaterialize(dbl); break;
	default:
		if ( tpe == TYPE_timestamp){
			timestamp *v,f,l;
			lng s;
			f = *(timestamp*) getArgReference(stk,pci, 1);
			l = *(timestamp*) getArgReference(stk,pci, 2);
			s =  *(lng *) getArgReference(stk,pci, 3) ;
			bn = BATnew(TYPE_void, tpe, (l.days > f.days ? ((l.days -f.days)*24*60*60 +abs(s))/abs(s):((f.days -l.days)*24*60*60 +abs(s))/abs(s)));
			if( bn == NULL)
				throw(MAL,"generator.table",MAL_MALLOC_FAIL);
			v = (timestamp*) Tloc(bn,BUNfirst(bn));
			if( (f.days < l.days || (f.days = l.days && f.msecs <l.msecs)) && s > 0){
				for(; f.days<l.days || (f.days == l.days && f.msecs <l.msecs); ){
					*v++ = f;
					if( (msg=MTIMEtimestamp_add(&f, &f, &s)) != MAL_SUCCEED)
						return msg;
					c++;
				}
			} else
			if( f.days > l.days && s < 0)
				for(; f.days>l.days || (f.days == l.days && f.msecs > l.msecs); ){
					*v++ = f;
					if( (msg = MTIMEtimestamp_add(&f, &f, &s)) != MAL_SUCCEED)
						return msg;
					c++;
				}
			else
				throw(MAL,"generator.table","illegal generator arguments");
		}
	}
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

	for( i= 1; i< mb->stop; i++){
		q= getInstrPtr(mb,i);
		if( q->argv[0] == target)
			p = q;
		if( q == pci)
			return p;
	}
	return p;
}

#define VLTsubselect(TPE) {\
	TPE f,l,s, low,hgh;\
	oid *v;\
	f = *(TPE*) getArgReference(stk,p, 1);\
	l = *(TPE*) getArgReference(stk,p, 2);\
	s = pci->argc == 3 ? 1:  *(TPE*) getArgReference(stk,p, 3);\
	low = *(TPE*) getArgReference(stk,pci, i);\
	hgh = *(TPE*) getArgReference(stk,pci, i+1);\
	bn = BATnew(TYPE_void, TYPE_oid, (l>f ? (l-f+abs(s))/abs(s):(f-l+abs(s))/abs(s)));\
	if( bn == NULL)\
		throw(MAL,"generator.subselect",MAL_MALLOC_FAIL);\
	if( low == TPE##_nil ) low = li?f:f+1;\
	if( hgh == TPE##_nil ) hgh = hi?l+1:l;\
	v = (oid*) Tloc(bn,BUNfirst(bn));\
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
		throw(MAL,"generator.subselect","illegal generator arguments");\
}

str VLTgenerator_subselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, hi,li, anti, cndid= 0, c= 0;
	BAT *cnd= 0, *bn= 0;
	InstrPtr p;
	oid o = 0;
	int tpe;
	str msg= MAL_SUCCEED;

	(void) cntxt;
	p = findLastAssign(mb,pci,pci->argv[1]);
	if( p == NULL)
		throw(MAL,"generator.subselect","Could not locate definition for object");

	if( pci->argc == 8){ // candidate list included
		cndid = *(int*) getArgReference(stk,pci, 2);
		if( cndid){
			cnd = BATdescriptor(cndid);
			if( cnd == NULL)
				throw(MAL,"generator.subselect",RUNTIME_OBJECT_MISSING);
		} else throw(MAL,"generator.subselect","candidate list not implemented");
		i = 3;
	} else i = 2;

	li = *(bit*) getArgReference(stk,pci, i+2);
	hi = *(bit*) getArgReference(stk,pci, i+3);
	anti = *(bit*) getArgReference(stk,pci, i+4);

	switch( tpe = getArgType(mb,pci,i)){
	case TYPE_bte: VLTsubselect(bte); break;
	case TYPE_sht: VLTsubselect(sht); break;
	case TYPE_int: VLTsubselect(int); break;
	case TYPE_lng: VLTsubselect(lng); break;
	case TYPE_flt: VLTsubselect(flt); break;
	case TYPE_dbl: VLTsubselect(dbl); break;
	default:
		if ( tpe == TYPE_timestamp){
			timestamp f,l, low,hgh;
			lng s;
			oid *v;

			f = *(timestamp*) getArgReference(stk,p, 1);
			l = *(timestamp*) getArgReference(stk,p, 2);
			s =  *(lng*) getArgReference(stk,p, 3);
			low = *(timestamp*) getArgReference(stk,pci, i);
			hgh = *(timestamp*) getArgReference(stk,pci, i+1);
			bn = BATnew(TYPE_void, TYPE_oid, (l.days > f.days ? ((l.days -f.days)*24*60*60 +abs(s))/abs(s):((f.days -l.days)*24*60*60 +abs(s))/abs(s)));
			if( bn == NULL)
				throw(MAL,"generator.subselect",MAL_MALLOC_FAIL);

			if( timestamp_isnil(low) ){
				low = f;
				if( li)f.msecs++;
			}
			if( timestamp_isnil(hgh)){
				hgh = l;
				if( hi) l.msecs++;
			}
			v = (oid*) Tloc(bn,BUNfirst(bn));

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

	if( cnd)
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
\
	bn = BATnew(TYPE_void, TYPE_##TPE, cap);\
	if( bn == NULL){\
		BBPreleaseref(bid);\
		throw(MAL,"generator.thetasubselect",MAL_MALLOC_FAIL);\
	}\
	v = (TPE*) Tloc(bn,BUNfirst(bn));\
	for(; cap-- > 0; o++){\
		val = f + ((TPE)*o) * s;\
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
	BUN cap;
	oid *o;
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
	cap = BATcount(b);
	o = (oid*) Tloc(b,BUNfirst(b));

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

			bn = BATnew(TYPE_void, tpe, cap);
			if( bn == NULL){
				BBPreleaseref(bid);
				throw(MAL,"generator.thetasubselect",MAL_MALLOC_FAIL);
			}

			v = (timestamp*) Tloc(bn,BUNfirst(bn));

			for(; cap-- > 0; o++){
				t= (lng)((int)*o) * s;
				if( (msg = MTIMEtimestamp_add(&val, &f, &t)) != MAL_SUCCEED)
					return msg;

				if ((val.days < f.days || (f.days == l.days && f.msecs < l.msecs)) || ((val.days>l.days || (val.days== l.days && val.msecs >= l.msecs)))  || timestamp_isnil(val))
					continue;
				*v++ = val;
				c++;
			}
		}
	}

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
