/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * M.L. Kersten
 */
#include "monetdb_config.h"
#include "manifold.h"
#include "mal_resolve.h"
#include "mal_builder.h"

//#define _DEBUG_MANIFOLD_

/* The default iterator over known scalar commands.
 * It can be less efficient then the vector based implementations,
 * but saves quite some hacking in non-essential cases or
 * expensive user defined functions.
 *
 * To keep things simple and reasonably performant we limit the
 * implementation to those cases where a single BAT is returned.
 * Arguments may be of any type. The MAL signature should be a COMMAND.
 *
 * The functionality has been extended to also perform the manifold
 * over aligned BATs, provided the underlying scalar function carries
 * the 'manifold' property.
 */

typedef struct{
	BAT *b;
	void *first;
	void *last;
	int	size;
	int type;
	BUN cnt;
	BATiter bi;
	BUN  o;
	BUN  q;
	str *s;
} MULTIarg;

typedef struct{
	Client cntxt;
	MalBlkPtr mb;
	MalStkPtr stk;
	InstrPtr pci;
	int fvar,lvar;
	MULTIarg *args;
} MULTItask;


// Loop through the first BAT
// keep the last error message received
#define ManifoldLoop(Type, ...)											\
	do {																\
		Type *v = (Type*) mut->args[0].first;							\
		oid oo, olimit = mut->args[mut->fvar].cnt;						\
		for( oo= 0; oo < olimit; oo++){									\
			msg = (*mut->pci->fcn)(v, __VA_ARGS__);						\
			if (msg) break;												\
			for( i = mut->fvar; i<= mut->lvar; i++) {					\
				if(ATOMstorage(mut->args[i].type) == TYPE_void ){		\
					args[i] = (void*)  &mut->args[i].o;					\
					mut->args[i].o++;									\
				} else if(mut->args[i].size == 0) {						\
					;													\
				} else if(ATOMstorage(mut->args[i].type) < TYPE_str ) {	\
					args[i] += mut->args[i].size;						\
				} else if (ATOMvarsized(mut->args[i].type)) {			\
					mut->args[i].o++;									\
					mut->args[i].s = (str *) BUNtail(mut->args[i].bi, mut->args[i].o); \
					args[i] = (void*)  &mut->args[i].s;					\
				} else {												\
					mut->args[i].o++;									\
					mut->args[i].s = (str *) Tloc(mut->args[i].b, mut->args[i].o); \
					args[i] = (void*)  &mut->args[i].s;					\
				}														\
			}															\
			v++;														\
		}																\
	} while (0)

// The target BAT tail type determines the result variable
#ifdef HAVE_HGE
#define Manifoldbody_hge(...)					\
	case TYPE_hge: ManifoldLoop(hge,__VA_ARGS__); break
#else
#define Manifoldbody_hge(...)
#endif
#define Manifoldbody(...)												\
	do {																\
		switch(ATOMstorage(mut->args[0].b->ttype)){						\
		case TYPE_bte: ManifoldLoop(bte,__VA_ARGS__); break;			\
		case TYPE_sht: ManifoldLoop(sht,__VA_ARGS__); break;			\
		case TYPE_int: ManifoldLoop(int,__VA_ARGS__); break;			\
		case TYPE_lng: ManifoldLoop(lng,__VA_ARGS__); break;			\
		Manifoldbody_hge(__VA_ARGS__);									\
		case TYPE_oid: ManifoldLoop(oid,__VA_ARGS__); break;			\
		case TYPE_flt: ManifoldLoop(flt,__VA_ARGS__); break;			\
		case TYPE_dbl: ManifoldLoop(dbl,__VA_ARGS__); break;			\
		case TYPE_str:													\
		default: {														\
			oid oo, olimit = mut->args[mut->fvar].cnt;					\
			for( oo= 0; oo < olimit; oo++){								\
				msg = (*mut->pci->fcn)(&y, __VA_ARGS__);				\
				if (msg)												\
					break;												\
				bunfastapp(mut->args[0].b, (void*) y);					\
				GDKfree(y); y = NULL;									\
				for( i = mut->fvar; i<= mut->lvar; i++) {				\
					if(ATOMstorage(mut->args[i].type) == TYPE_void ){ 	\
						args[i] = (void*)  &mut->args[i].o;				\
						mut->args[i].o++;								\
					} else if(mut->args[i].size == 0) {					\
						;												\
					} else if (ATOMstorage(mut->args[i].type) < TYPE_str){ \
						args[i] += mut->args[i].size;					\
					} else if(ATOMvarsized(mut->args[i].type)){			\
						mut->args[i].o++;								\
						mut->args[i].s = (str*) BUNtail(mut->args[i].bi, mut->args[i].o); \
						args[i] =  (void*) & mut->args[i].s;			\
					} else {											\
						mut->args[i].o++;								\
						mut->args[i].s = (str*) Tloc(mut->args[i].b, mut->args[i].o); \
						args[i] =  (void*) & mut->args[i].s;			\
					}													\
				}														\
			}															\
			break;														\
		}																\
		}																\
	} while (0)

// single argument is preparatory step for GDK_mapreduce
// Only the last error message is returned, the value of
// an erroneous call depends on the operator itself.
static str
MANIFOLDjob(MULTItask *mut)
{	int i;
	char **args;
	str y = NULL, msg= MAL_SUCCEED;

	args = (char**) GDKzalloc(sizeof(char*) * mut->pci->argc);
	if( args == NULL)
		throw(MAL,"mal.manifold",MAL_MALLOC_FAIL);
	
	// the mod.fcn arguments are ignored from the call
	for( i = mut->pci->retc+2; i< mut->pci->argc; i++) {
		if ( mut->args[i].b ){
			if(ATOMstorage(mut->args[i].type) < TYPE_str){ 	
				args[i] = (char*) mut->args[i].first;
			} else if(ATOMvarsized(mut->args[i].type)){	
				mut->args[i].s = (str*) BUNtail(mut->args[i].bi, mut->args[i].o);
				args[i] =  (void*) & mut->args[i].s; 
			} else {
				mut->args[i].s = (str*) Tloc(mut->args[i].b, mut->args[i].o);
				args[i] =  (void*) & mut->args[i].s; 
			}
		} else {
			args[i] = (char *) getArgReference(mut->stk,mut->pci,i);
		}
	}

#ifdef _DEBUG_MANIFOLD_
	mnstr_printf(mut->cntxt->fdout,"#MANIFOLDjob fvar %d lvar %d type %d\n",mut->fvar,mut->lvar, ATOMstorage(mut->args[mut->fvar].b->ttype));
#endif
	// use limited argument list expansion.
	switch(mut->pci->argc){
	case 4: Manifoldbody(args[3]); break;
	case 5: Manifoldbody(args[3],args[4]); break;
	case 6: Manifoldbody(args[3],args[4],args[5]); break;
	case 7: Manifoldbody(args[3],args[4],args[5],args[6]); break;
	case 8: Manifoldbody(args[3],args[4],args[5],args[6],args[7]); break;
	default:
		msg= createException(MAL,"mal.manifold","manifold call limitation ");
	}
	if (ATOMextern(mut->args[0].type) && y) 
		GDKfree(y); 
bunins_failed:
	GDKfree(args);
	return msg;
}

/* The manifold optimizer should check for the possibility
 * to use this implementation instead of the MAL loop.
 */
MALfcn
MANIFOLDtypecheck(Client cntxt, MalBlkPtr mb, InstrPtr pci){
	int i, k, tpe= 0;
	InstrPtr q=0;
	MalBlkPtr nmb;
	MALfcn fcn;

	if (pci->retc >1 || pci->argc > 8 || getModuleId(pci) == NULL) // limitation on MANIFOLDjob
		return NULL;
	// We need a private MAL context to resolve the function call
	nmb = newMalBlk(MAXVARS);
	if( nmb == NULL)
		return NULL;
	// the scalar function
	q = newStmt(nmb,
		getVarConstant(mb,getArg(pci,pci->retc)).val.sval,
		getVarConstant(mb,getArg(pci,pci->retc+1)).val.sval);

	// Prepare the single result variable
	tpe =getBatType(getArgType(mb,pci,0));
	k= getArg(q,0);
	setVarType(nmb,k,tpe);
	if ( isVarFixed(nmb,k)) 
		setVarFixed(nmb,k);
	if (isVarUDFtype(nmb,k))
		setVarUDFtype(nmb,k);
	
	// extract their scalar argument type
	for ( i = pci->retc+2; i < pci->argc; i++){
		tpe = getBatType(getArgType(mb,pci,i));
		q= pushArgument(nmb,q, k= newTmpVariable(nmb, tpe));
		setVarFixed(nmb,k);
		setVarUDFtype(nmb,k);
	}

#ifdef _DEBUG_MANIFOLD_
	mnstr_printf(cntxt->fdout,"#MANIFOLD operation\n");
	printInstruction(cntxt->fdout,mb,0,pci,LIST_MAL_ALL);
	printInstruction(cntxt->fdout,nmb,0,q,LIST_MAL_ALL);
#endif
	// Localize the underlying scalar operator
	typeChecker(cntxt->fdout, cntxt->nspace, nmb, q, TRUE);
	if (nmb->errors || q->fcn == NULL || q->token != CMDcall ||
		(q->blk && q->blk->unsafeProp) )
		fcn = NULL;
	else {
		fcn = q->fcn;
		// retain the type detected
		if ( !isVarFixed(mb, getArg(pci,0)))
			setVarType( mb, getArg(pci,0), newBatType(getArgType(nmb,q,0)) );
	}
#ifdef _DEBUG_MANIFOLD_
	mnstr_printf(cntxt->fdout,"success? %s\n",(fcn == NULL? "no":"yes"));
	printInstruction(cntxt->fdout,nmb,0,q,LIST_MAL_ALL);
#endif
	freeMalBlk(nmb);
	return fcn;
}

/*
 * The manifold should support aligned BATs as well
 */
str
MANIFOLDevaluate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	MULTItask mut;
	MULTIarg *mat;
	int i, tpe= 0;
	BUN cnt = 0;
	oid o = 0;
	str msg = MAL_SUCCEED;
	MALfcn fcn;

	fcn= MANIFOLDtypecheck(cntxt,mb,pci);
	if( fcn == NULL)
		throw(MAL, "mal.manifold", "Illegal manifold function call");

	mat = (MULTIarg *) GDKzalloc(sizeof(MULTIarg) * pci->argc);
	if( mat == NULL)
		throw(MAL, "mal.manifold", MAL_MALLOC_FAIL);
	
	// mr-job structure preparation
	mut.fvar = mut.lvar = 0;
	mut.cntxt= cntxt;
	mut.mb= mb;
	mut.stk= stk;
	mut.args= mat;
	mut.pci = pci;

	// prepare iterators
	for( i = pci->retc+2; i < pci->argc; i++){
		if ( isaBatType(getArgType(mb,pci,i)) ){
			mat[i].b = BATdescriptor( *getArgReference_bat(stk,pci,i));
			if ( mat[i].b == NULL){
				msg = createException(MAL,"mal.manifold", MAL_MALLOC_FAIL);
				goto wrapup;
			}
			mat[i].type = tpe = getBatType(getArgType(mb,pci,i));
			if (mut.fvar == 0){
				mut.fvar = i;
				cnt = BATcount(mat[i].b);
			} else if (BATcount(mat[i].b)!= cnt){
				msg = createException(MAL,"mal.manifold","Columns must be of same length");
				goto wrapup;
			} 
			mut.lvar = i;
			if (ATOMstorage(tpe) == TYPE_str) 
				mat[i].size = Tsize(mat[i].b);
			else
				mat[i].size = ATOMsize(tpe);
			mat[i].cnt = cnt;
			if ( mat[i].b->ttype == TYPE_void){
				o = mat[i].b->tseqbase;
				mat[i].first = mat[i].last = (void*) &o;
			} else {
				mat[i].first = (void*)  Tloc(mat[i].b, 0);
				mat[i].last = (void*) Tloc(mat[i].b, BUNlast(mat[i].b));
			}
			mat[i].bi = bat_iterator(mat[i].b);
			mat[i].o = 0;
			mat[i].q = BUNlast(mat[i].b);
		} else {
			mat[i].last = mat[i].first = (void *) getArgReference(stk,pci,i);
			mat[i].type = getArgType(mb, pci, i);
		}
	}

	// Then iterator over all BATs
	if( mut.fvar ==0){
		msg= createException(MAL,"mal.manifold","At least one column required");
		goto wrapup;
	}

	// prepare result variable
	mat[0].b =COLnew(mat[mut.fvar].b->hseqbase, getBatType(getArgType(mb,pci,0)), cnt, TRANSIENT);
	if ( mat[0].b == NULL){
		msg= createException(MAL,"mal.manifold",MAL_MALLOC_FAIL);
		goto wrapup;
	}
	mat[0].b->tnonil=0;
	mat[0].b->tsorted=0;
	mat[0].b->trevsorted=0;
	mat[0].bi = bat_iterator(mat[0].b);
	mat[0].first = (void *)  Tloc(mat[0].b, 0);
	mat[0].last = (void *)  Tloc(mat[0].b, BUNlast(mat[0].b));

	mut.pci = copyInstruction(pci);
	mut.pci->fcn = fcn;
	msg = MANIFOLDjob(&mut);
	freeInstruction(mut.pci);

	// consolidate the properties
	if (ATOMstorage(mat[0].b->ttype) < TYPE_str)
		BATsetcount(mat[0].b,cnt);
	BATsettrivprop(mat[0].b);
	BBPkeepref(*getArgReference_bat(stk,pci,0)=mat[0].b->batCacheid);
wrapup:
	// restore the argument types
	for (i = pci->retc; i < pci->argc; i++){
		if ( mat[i].b)
			BBPunfix(mat[i].b->batCacheid);
	}
	GDKfree(mat);
	return msg;
}

// The old code
str
MANIFOLDremapMultiplex(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
    (void) mb;
    (void) cntxt;
    throw(MAL, "opt.remap", "Function '%s.%s' not defined",
		  *getArgReference_str(stk, p, p->retc),
		  *getArgReference_str(stk, p, p->retc + 1));
}
