/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * (authors) M. Kersten
 *
 * MAL builder
 * The MAL builder library containst the primitives to simplify construction
 * of programs by compilers. It has grown out of the MonetDB/SQL code generator.
 * The strings being passed as arguments are copied in the process.
 *
 */
#include "monetdb_config.h"
#include "mal_builder.h"
#include "mal_function.h"
#include "mal_namespace.h"

InstrPtr
newAssignment(MalBlkPtr mb)
{
	InstrPtr q = newInstruction(mb,NULL,NULL);

	if ( q == NULL)
		return NULL;
	if ((getArg(q,0)= newTmpVariable(mb,TYPE_any)) < 0 || mb->errors != MAL_SUCCEED) {
		freeInstruction(q);
		return NULL;
	}
	pushInstruction(mb, q);
	return q;
}

InstrPtr
newStmt(MalBlkPtr mb, const char *module, const char *name)
{
	InstrPtr q;
	str mName = putName(module), nName = putName(name);

	if(mName == NULL || nName == NULL) {
		return NULL;
	}
	q = newInstruction(mb, mName, nName);
	if ( q == NULL)
		return NULL;
	setDestVar(q, newTmpVariable(mb, TYPE_any));
	if (getDestVar(q) < 0 || mb->errors != MAL_SUCCEED) {
		freeInstruction(q);
		return NULL;
	}
	pushInstruction(mb, q);
	return q;
}

InstrPtr
newReturnStmt(MalBlkPtr mb)
{
	InstrPtr q = newInstruction(mb, NULL, NULL);

	if ( q == NULL)
		return NULL;
	if ((getArg(q,0)= newTmpVariable(mb,TYPE_any)) < 0 || mb->errors != MAL_SUCCEED) {
		freeInstruction(q);
		return NULL;
	}
	q->barrier= RETURNsymbol;
	pushInstruction(mb, q);
	return q;
}

InstrPtr
newFcnCall(MalBlkPtr mb, char *mod, char *fcn)
{
	InstrPtr q = newAssignment(mb);
	str fcnName, modName;

	if ( q == NULL || mod == NULL || fcn == NULL)
		return NULL;
	modName = putName(mod);
	fcnName = putName(fcn);
	if(modName == NULL || fcnName == NULL) {
		freeInstruction(q);
		return NULL;
	}
	setModuleId(q, modName);
	setFunctionId(q, fcnName);
	return q;
}

InstrPtr
newComment(MalBlkPtr mb, const char *val)
{
	InstrPtr q = newInstruction(mb, NULL, NULL);
	ValRecord cst;

	if (q == NULL)
		return NULL;
	q->token = REMsymbol;
	q->barrier = 0;
	cst.vtype= TYPE_str;
	if ((cst.val.sval= GDKstrdup(val)) == NULL) {
		freeInstruction(q);
		return NULL;
	}
	cst.len= (int) strlen(cst.val.sval);
	getArg(q,0) = defConstant(mb,TYPE_str,&cst);
	clrVarConstant(mb,getArg(q,0));
	setVarDisabled(mb,getArg(q,0));
	if (mb->errors != MAL_SUCCEED) {
		freeInstruction(q);
		return NULL;
	}
	pushInstruction(mb, q);
	return q;
}

InstrPtr
newCatchStmt(MalBlkPtr mb, str nme)
{
	InstrPtr q = newAssignment(mb);
	int i= findVariable(mb,nme);

	if ( q == NULL)
		return NULL;
	q->barrier = CATCHsymbol;
	if ( i< 0) {
		if ((getArg(q,0)= newVariable(mb, nme, strlen(nme),TYPE_str)) < 0 || mb->errors != MAL_SUCCEED) {
			freeInstruction(q);
			return NULL;
		}
		setVarUDFtype(mb,getArg(q,0));
	} else getArg(q,0) = i;
	return q;
}

InstrPtr
newRaiseStmt(MalBlkPtr mb, str nme)
{
	InstrPtr q = newAssignment(mb);
	int i= findVariable(mb,nme);

	if ( q == NULL)
		return NULL;
	q->barrier = RAISEsymbol;
	if ( i< 0) {
		if ((getArg(q,0)= newVariable(mb, nme, strlen(nme),TYPE_str)) < 0 || mb->errors != MAL_SUCCEED) {
			freeInstruction(q);
			return NULL;
		}
	} else
		getArg(q,0) = i;
	return q;
}

InstrPtr
newExitStmt(MalBlkPtr mb, str nme)
{
	InstrPtr q = newAssignment(mb);
	int i= findVariable(mb,nme);

	if ( q == NULL)
		return NULL;
	q->barrier = EXITsymbol;
	if ( i< 0) {
		if ((getArg(q,0)= newVariable(mb, nme,strlen(nme),TYPE_str)) < 0 || mb->errors != MAL_SUCCEED) {
			freeInstruction(q);
			return NULL;
		}
	} else
		getArg(q,0) = i;
	return q;
}

InstrPtr
pushEndInstruction(MalBlkPtr mb)
{
    InstrPtr q;

    q = newInstruction(mb,NULL, NULL);
	if ( q == NULL)
		return NULL;
    q->token = ENDsymbol;
    q->barrier = 0;
    q->argc = 0;
    q->retc = 0;
    q->argv[0] = 0;
    pushInstruction(mb, q);
	return q;
}

int
getIntConstant(MalBlkPtr mb, int val)
{
	int _t;
	ValRecord cst;

	cst.vtype= TYPE_int;
	cst.val.ival= val;
	cst.len = 0;
	_t= fndConstant(mb, &cst, mb->vtop);
	if( _t < 0)
		_t = defConstant(mb, TYPE_int,&cst);
	return _t;
}

InstrPtr
pushInt(MalBlkPtr mb, InstrPtr q, int val)
{
	int _t;
	ValRecord cst;

	if (q == NULL)
		return NULL;
	cst.vtype= TYPE_int;
	cst.val.ival= val;
	cst.len = 0;
	_t = defConstant(mb, TYPE_int,&cst);
	return pushArgument(mb, q, _t);
}

int
getBteConstant(MalBlkPtr mb, bte val)
{
	int _t;
	ValRecord cst;

	cst.vtype= TYPE_bte;
	cst.val.btval= val;
	cst.len = 0;
	_t= fndConstant(mb, &cst, mb->vtop);
	if( _t < 0)
		_t = defConstant(mb, TYPE_bte, &cst);
	return _t;
}

InstrPtr
pushBte(MalBlkPtr mb, InstrPtr q, bte val)
{
	int _t;
	ValRecord cst;

	if (q == NULL)
		return NULL;
	cst.vtype= TYPE_bte;
	cst.val.btval= val;
	cst.len = 0;
	_t = defConstant(mb, TYPE_bte,&cst);
	return pushArgument(mb, q, _t);
}

int
getOidConstant(MalBlkPtr mb, oid val)
{
	int _t;
	ValRecord cst;

	cst.vtype= TYPE_oid;
	cst.val.oval= val;
	cst.len = 0;
	_t= fndConstant(mb, &cst, mb->vtop);
	if( _t < 0)
		_t = defConstant(mb, TYPE_oid, &cst);
	return _t;
}

InstrPtr 
pushOid(MalBlkPtr mb, InstrPtr q, oid val)
{
	int _t;
	ValRecord cst;

	if (q == NULL)
		return NULL;
	cst.vtype= TYPE_oid;
	cst.val.oval= val;
	cst.len = 0;
	_t = defConstant(mb,TYPE_oid,&cst);
	return pushArgument(mb, q, _t);
}

InstrPtr
pushVoid(MalBlkPtr mb, InstrPtr q)
{
	int _t;
	ValRecord cst;

	if (q == NULL)
		return NULL;
	cst.vtype= TYPE_void;
	cst.val.oval= oid_nil;
	cst.len = 0;
	_t = defConstant(mb,TYPE_void,&cst);
	return pushArgument(mb, q, _t);
}

int
getLngConstant(MalBlkPtr mb, lng val)
{
	int _t;
	ValRecord cst;

	cst.vtype= TYPE_lng;
	cst.val.lval= val;
	cst.len = 0;
	_t= fndConstant(mb, &cst, mb->vtop);
	if( _t < 0)
		_t = defConstant(mb, TYPE_lng, &cst);
	return _t;
}

InstrPtr
pushLng(MalBlkPtr mb, InstrPtr q, lng val)
{
	int _t;
	ValRecord cst;

	if (q == NULL)
		return NULL;
	cst.vtype= TYPE_lng;
	cst.val.lval= val;
	cst.len = 0;
	_t = defConstant(mb,TYPE_lng,&cst);
	return pushArgument(mb, q, _t);
}

int
getShtConstant(MalBlkPtr mb, sht val)
{
	int _t;
	ValRecord cst;

	cst.vtype= TYPE_sht;
	cst.val.shval= val;
	cst.len = 0;
	_t= fndConstant(mb, &cst, mb->vtop);
	if( _t < 0)
		_t = defConstant(mb, TYPE_sht, &cst);
	return _t;
}

InstrPtr
pushSht(MalBlkPtr mb, InstrPtr q, sht val)
{
	int _t;
	ValRecord cst;

	if ( q == NULL)
		return NULL;
	cst.vtype= TYPE_sht;
	cst.val.shval= val;
	cst.len = 0;
	_t = defConstant(mb,TYPE_sht,&cst);
	return pushArgument(mb, q, _t);
}

#ifdef HAVE_HGE
int
getHgeConstant(MalBlkPtr mb, hge val)
{
	int _t;
	ValRecord cst;

	cst.vtype= TYPE_oid;
	cst.val.hval= val;
	cst.len = 0;
	_t= fndConstant(mb, &cst, mb->vtop);
	if( _t < 0)
		_t = defConstant(mb, TYPE_hge, &cst);
	return _t;
}

InstrPtr
pushHge(MalBlkPtr mb, InstrPtr q, hge val)
{
	int _t;
	ValRecord cst;

	if ( q == NULL)
		return NULL;
	cst.vtype= TYPE_hge;
	cst.val.hval= val;
	cst.len = 0;
	_t = defConstant(mb,TYPE_hge,&cst);
	return pushArgument(mb, q, _t);
}
#endif

int
getDblConstant(MalBlkPtr mb, dbl val)
{
	int _t;
	ValRecord cst;

	cst.vtype= TYPE_dbl;
	cst.val.dval= val;
	cst.len = 0;
	_t= fndConstant(mb, &cst, mb->vtop);
	if( _t < 0)
		_t = defConstant(mb, TYPE_dbl, &cst);
	return _t;
}

InstrPtr
pushDbl(MalBlkPtr mb, InstrPtr q, dbl val)
{
	int _t;
	ValRecord cst;

	if (q == NULL)
		return NULL;
	cst.vtype= TYPE_dbl;
	cst.val.dval= val;
	cst.len = 0;
	_t = defConstant(mb,TYPE_dbl,&cst);
	return pushArgument(mb, q, _t);
}

int
getFltConstant(MalBlkPtr mb, flt val)
{
	int _t;
	ValRecord cst;

	cst.vtype= TYPE_flt;
	cst.val.fval= val;
	cst.len = 0;
	_t= fndConstant(mb, &cst, mb->vtop);
	if( _t < 0)
		_t = defConstant(mb, TYPE_flt, &cst);
	return _t;
}

InstrPtr
pushFlt(MalBlkPtr mb, InstrPtr q, flt val)
{
	int _t;
	ValRecord cst;

	if (q == NULL)
		return NULL;
	cst.vtype= TYPE_flt;
	cst.val.fval= val;
	cst.len = 0;
	_t = defConstant(mb,TYPE_flt,&cst);
	return pushArgument(mb, q, _t);
}

int
getStrConstant(MalBlkPtr mb, str val)
{
	int _t;
	ValRecord cst;

	cst.vtype = TYPE_str;
	cst.val.sval = val; 
	cst.len = (int) strlen(val);
	_t= fndConstant(mb, &cst, mb->vtop);
	if( _t < 0) {
		if ((cst.val.sval= GDKstrdup(val)) == NULL) 
			return -1;
		_t = defConstant(mb, TYPE_str, &cst);
	}
	return _t;
}

InstrPtr
pushStr(MalBlkPtr mb, InstrPtr q, const char *Val)
{
	int _t;
	ValRecord cst;

	if (q == NULL)
		return NULL;
	cst.vtype= TYPE_str;
	if ((cst.val.sval= GDKstrdup(Val)) == NULL) {
		freeInstruction(q);
		return NULL;
	}
	cst.len= (int) strlen(cst.val.sval);
	_t = defConstant(mb,TYPE_str,&cst);
	return pushArgument(mb, q, _t);
}

int
getBitConstant(MalBlkPtr mb, bit val)
{
	int _t;
	ValRecord cst;

	cst.vtype= TYPE_bit;
	cst.val.btval= val;
	cst.len = 0;
	_t= fndConstant(mb, &cst, mb->vtop);
	if( _t < 0)
		_t = defConstant(mb, TYPE_bit, &cst);
	return _t;
}

InstrPtr
pushBit(MalBlkPtr mb, InstrPtr q, bit val)
{
	int _t;
	ValRecord cst;

	if (q == NULL)
		return NULL;
	cst.vtype= TYPE_bit;
	cst.val.btval= val;
	cst.len = 0;
	_t = defConstant(mb,TYPE_bit,&cst);

	return pushArgument(mb, q, _t);
}

InstrPtr
pushNil(MalBlkPtr mb, InstrPtr q, int tpe)
{
	int _t;
	ValRecord cst;

	if (q == NULL)
		return NULL;
	cst.len = 0;
	if( !isaBatType(tpe) && tpe != TYPE_bat ) {
		assert(tpe < MAXATOMS);	/* in particular, tpe!=TYPE_any */
		if (!tpe) {
			cst.vtype=TYPE_void;
			cst.val.oval= oid_nil;
		} else if (ATOMextern(tpe)) {
			ptr p = ATOMnil(tpe);
			if( p == NULL){
				freeInstruction(q);
				return NULL;
			}
			VALset(&cst, tpe, p);
		} else {
			if (VALinit(&cst, tpe, ATOMnilptr(tpe)) == NULL) {
				freeInstruction(q);
				return NULL;
			}
		}
		_t = defConstant(mb,tpe,&cst);
	} else {
		cst.vtype = TYPE_bat;
		cst.val.bval = bat_nil;
		_t = defConstant(mb,TYPE_bat,&cst);
		getVarType(mb,_t) = tpe;
	}
	q= pushArgument(mb, q, _t);
	setVarUDFtype(mb,getArg(q,q->argc-1)); /* needed */
	return q;
}

InstrPtr
pushNilType(MalBlkPtr mb, InstrPtr q, char *tpe)
{
	int _t,idx;
	ValRecord cst;
	str msg;

	if (q == NULL)
		return NULL;
	idx= getAtomIndex(tpe, -1, TYPE_any);
	if( idx < 0 || idx >= GDKatomcnt || idx >= MAXATOMS)
		return NULL;
	cst.vtype=TYPE_void;
	cst.val.oval= oid_nil;
	cst.len = 0;
	msg = convertConstant(idx, &cst);
	if (msg != MAL_SUCCEED) {
		freeException(msg);
		return NULL;
	}
	_t = defConstant(mb,idx,&cst);
	setVarUDFtype(mb,_t);

	return pushArgument(mb, q, _t);
}
InstrPtr
pushType(MalBlkPtr mb, InstrPtr q, int tpe)
{
	int _t;
	ValRecord cst;
	str msg;

	if (q == NULL)
		return NULL;
	cst.vtype=TYPE_void;
	cst.val.oval= oid_nil;
	cst.len = 0;
	msg = convertConstant(tpe, &cst);
	if (msg != MAL_SUCCEED) {
		freeException(msg);
		return NULL;
	}
	_t = defConstant(mb,tpe,&cst);
	setVarUDFtype(mb,_t);

	return pushArgument(mb, q, _t);
}

InstrPtr
pushZero(MalBlkPtr mb, InstrPtr q, int tpe)
{
	int _t;
	ValRecord cst;
	str msg;

	if (q == NULL)
		return NULL;
	cst.vtype=TYPE_int;
	cst.val.ival= 0;
	cst.len = 0;
	msg = convertConstant(tpe, &cst);
	if (msg != MAL_SUCCEED) {
		freeException(msg);
		return NULL;
	}
	_t = defConstant(mb,tpe,&cst);

	return pushArgument(mb, q, _t);
}

InstrPtr
pushEmptyBAT(MalBlkPtr mb, InstrPtr q, int tpe)
{
	if (q == NULL)
		return NULL;
	getModuleId(q) = getName("bat");
	getFunctionId(q) = getName("new");

	q = pushArgument(mb, q, newTypeVariable(mb,TYPE_void));
	q = pushArgument(mb, q, newTypeVariable(mb,getBatType(tpe)));
	q = pushZero(mb,q,TYPE_lng);
	return q;
}

InstrPtr
pushValue(MalBlkPtr mb, InstrPtr q, ValPtr vr)
{
	int _t;
	ValRecord cst;

	if (q == NULL)
		return NULL;
	if (VALcopy(&cst, vr) == NULL) {
		freeInstruction(q);
		return NULL;
	}
	_t = defConstant(mb,cst.vtype,&cst);
	return pushArgument(mb, q, _t);
}
