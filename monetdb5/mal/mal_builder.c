/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
newAssignmentArgs(MalBlkPtr mb, int args)
{
	InstrPtr q = newInstructionArgs(mb, NULL, NULL, args);
	int k;

	if (q == NULL)
		return NULL;
	k = newTmpVariable(mb,TYPE_any);
	if (k < 0) {
		// construct an exception message to be passed to upper layers using ->errors
		str msg = createException(MAL, "newAssignment", "Can not allocate variable");
		addMalException(mb, msg);
		freeException(msg);
	} else
		getArg(q,0) =  k;
	pushInstruction(mb, q);
	return q;
}

InstrPtr
newAssignment(MalBlkPtr mb)
{
	return newAssignmentArgs(mb, MAXARG);
}

InstrPtr
newStmt(MalBlkPtr mb, const char *module, const char *name)
{
	return newStmtArgs(mb, module, name, MAXARG);
}

InstrPtr
newStmtArgs(MalBlkPtr mb, const char *module, const char *name, int args)
{
	InstrPtr q;
	const char *mName = putName(module), *nName = putName(name);

	q = newInstructionArgs(mb, mName, nName, args);
	if (q == NULL)
		return NULL;

	setDestVar(q, newTmpVariable(mb, TYPE_any));
	if (getDestVar(q) < 0 || mb->errors != MAL_SUCCEED) {
		str msg = createException(MAL, "newStmtArgs", "Can not allocate variable");
		addMalException(mb, msg);
		freeException(msg);
	}
	pushInstruction(mb, q);
	return q;
}

InstrPtr
newReturnStmt(MalBlkPtr mb)
{
	InstrPtr q = newInstruction(mb, NULL, NULL);
	int k;

	if (q == NULL)
		return NULL;
	k = newTmpVariable(mb,TYPE_any);
	if (k < 0 ){
		str msg = createException(MAL, "newReturnStmt", "Can not allocate return variable");
		addMalException(mb, msg);
		freeException(msg);
	} else
		getArg(q,0) = k;
	q->barrier= RETURNsymbol;
	pushInstruction(mb, q);
	return q;
}

InstrPtr
newFcnCallArgs(MalBlkPtr mb, const char *mod, const char *fcn, int args)
{
	InstrPtr q = newAssignmentArgs(mb, args);
	const char *fcnName, *modName;

	modName = putName(mod);
	fcnName = putName(fcn);
	setModuleId(q, modName);
	setFunctionId(q, fcnName);
	return q;
}

InstrPtr
newFcnCall(MalBlkPtr mb, const char *mod, const char *fcn)
{
	return newFcnCallArgs(mb, mod, fcn, MAXARG);
}

InstrPtr
newComment(MalBlkPtr mb, const char *val)
{
	InstrPtr q = newInstruction(mb, NULL, NULL);
	ValRecord cst;
	int k;

	if (q == NULL)
		return NULL;
	q->token = REMsymbol;
	q->barrier = 0;
	cst.vtype= TYPE_str;
	if ((cst.val.sval= GDKstrdup(val)) == NULL) {
		str msg = createException(MAL, "newComment", "Can not allocate comment");
		addMalException(mb, msg);
		freeException(msg);
	} else {
		cst.len = strlen(cst.val.sval);
		k = defConstant(mb, TYPE_str, &cst);
		if( k >= 0){
			getArg(q,0) = k;
			clrVarConstant(mb,getArg(q,0));
			setVarDisabled(mb,getArg(q,0));
		}
	}
	pushInstruction(mb, q);
	return q;
}

InstrPtr
newCatchStmt(MalBlkPtr mb, const char *nme)
{
	InstrPtr q = newAssignment(mb);
	int i= findVariable(mb,nme);
	int k;

	if (q == NULL)
		return NULL;
	q->barrier = CATCHsymbol;
	if ( i< 0) {
		k = newVariable(mb, nme, strlen(nme),TYPE_str);
		if (k<0){
			str msg = createException(MAL, "newCatchStmt", "Can not allocate variable");
			addMalException(mb, msg);
			freeException(msg);
		}else{
			getArg(q,0) = k;
		}
	} else getArg(q,0) = i;
	return q;
}

InstrPtr
newRaiseStmt(MalBlkPtr mb, const char *nme)
{
	InstrPtr q = newAssignment(mb);
	int i= findVariable(mb,nme);
	int k;

	if (q == NULL)
		return NULL;
	q->barrier = RAISEsymbol;
	if ( i< 0) {
		k = newVariable(mb, nme, strlen(nme),TYPE_str);
		if (k< 0 || mb->errors != MAL_SUCCEED) {
			str msg = createException(MAL, "newRaiseStmt", "Can not allocate variable");
			addMalException(mb, msg);
			freeException(msg);
		} else
			getArg(q,0) = k;
	} else
		getArg(q,0) = i;
	return q;
}

InstrPtr
newExitStmt(MalBlkPtr mb, const char *nme)
{
	InstrPtr q = newAssignment(mb);
	int i= findVariable(mb,nme);
	int k;

	if (q == NULL)
		return NULL;
	q->barrier = EXITsymbol;
	if ( i< 0) {
		k= newVariable(mb, nme,strlen(nme),TYPE_str);
		if (k < 0 ){
			str msg = createException(MAL, "newExitStmt", "Can not allocate variable");
			addMalException(mb, msg);
			freeException(msg);
		}else
			getArg(q,0) = k;
	} else
		getArg(q,0) = i;
	return q;
}

InstrPtr
pushEndInstruction(MalBlkPtr mb)
{
    InstrPtr q = newInstruction(mb,NULL, NULL);

	if (q == NULL)
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
	_t= fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if( _t < 0)
		_t = defConstant(mb, TYPE_int, &cst);
	assert(_t >= 0);
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
	_t = defConstant(mb, TYPE_int, &cst);
	if( _t >= 0)
		return pushArgument(mb, q, _t);
	return q;
}

int
getBteConstant(MalBlkPtr mb, bte val)
{
	int _t;
	ValRecord cst;

	cst.vtype= TYPE_bte;
	cst.val.btval= val;
	cst.len = 0;
	_t= fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if( _t < 0)
		_t = defConstant(mb, TYPE_bte, &cst);
	assert(_t >= 0);
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
	if( _t >= 0)
		return pushArgument(mb, q, _t);
	return q;
}

int
getOidConstant(MalBlkPtr mb, oid val)
{
	int _t;
	ValRecord cst;

	cst.vtype= TYPE_oid;
	cst.val.oval= val;
	cst.len = 0;
	_t= fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if( _t < 0)
		_t = defConstant(mb, TYPE_oid, &cst);
	assert(_t >= 0);
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
	if( _t >= 0)
		return pushArgument(mb, q, _t);
	return q;
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
	if( _t >= 0)
		return pushArgument(mb, q, _t);
	return q;
}

int
getLngConstant(MalBlkPtr mb, lng val)
{
	int _t;
	ValRecord cst;

	cst.vtype= TYPE_lng;
	cst.val.lval= val;
	cst.len = 0;
	_t= fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if( _t < 0)
		_t = defConstant(mb, TYPE_lng, &cst);
	assert(_t >= 0);
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
	if( _t >= 0)
		return pushArgument(mb, q, _t);
	return q;
}

int
getShtConstant(MalBlkPtr mb, sht val)
{
	int _t;
	ValRecord cst;

	cst.vtype= TYPE_sht;
	cst.val.shval= val;
	cst.len = 0;
	_t= fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if( _t < 0)
		_t = defConstant(mb, TYPE_sht, &cst);
	assert(_t >=0);
	return _t;
}

InstrPtr
pushSht(MalBlkPtr mb, InstrPtr q, sht val)
{
	int _t;
	ValRecord cst;

	if (q == NULL)
		return NULL;
	cst.vtype= TYPE_sht;
	cst.val.shval= val;
	cst.len = 0;
	_t = defConstant(mb,TYPE_sht,&cst);
	if( _t >= 0)
		return pushArgument(mb, q, _t);
	return q;
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
	_t= fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if( _t < 0)
		_t = defConstant(mb, TYPE_hge, &cst);
	assert(_t >= 0);
	return _t;
}

InstrPtr
pushHge(MalBlkPtr mb, InstrPtr q, hge val)
{
	int _t;
	ValRecord cst;

	if (q == NULL)
		return NULL;
	cst.vtype= TYPE_hge;
	cst.val.hval= val;
	cst.len = 0;
	_t = defConstant(mb,TYPE_hge,&cst);
	if (_t >= 0)
		return pushArgument(mb, q, _t);
	return q;
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
	_t= fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if( _t < 0)
		_t = defConstant(mb, TYPE_dbl, &cst);
	assert(_t >= 0);
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
	if( _t >= 0)
		return pushArgument(mb, q, _t);
	return q;
}

int
getFltConstant(MalBlkPtr mb, flt val)
{
	int _t;
	ValRecord cst;

	cst.vtype= TYPE_flt;
	cst.val.fval= val;
	cst.len = 0;
	_t= fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if( _t < 0)
		_t = defConstant(mb, TYPE_flt, &cst);
	assert(_t >= 0);
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
	if( _t >= 0)
		return pushArgument(mb, q, _t);
	return q;
}

int
getStrConstant(MalBlkPtr mb, str val)
{
	int _t;
	ValRecord cst;

	cst.vtype = TYPE_str;
	cst.val.sval = val;
	cst.len = strlen(val);
	_t= fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if( _t < 0) {
		if ((cst.val.sval= GDKstrdup(val)) == NULL)
			return -1;
		_t = defConstant(mb, TYPE_str, &cst);
	}
	assert(_t >= 0);
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
		str msg = createException(MAL, "pushStr", "Can not allocate string variable");
		addMalException(mb, msg);
		freeException(msg);
	} else{
		cst.len = strlen(cst.val.sval);
		_t = defConstant(mb,TYPE_str,&cst);
		if( _t >= 0)
			return pushArgument(mb, q, _t);
	}
	return q;
}

int
getBitConstant(MalBlkPtr mb, bit val)
{
	int _t;
	ValRecord cst;

	cst.vtype= TYPE_bit;
	cst.val.btval= val;
	cst.len = 0;
	_t= fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if( _t < 0)
		_t = defConstant(mb, TYPE_bit, &cst);
	assert(_t >= 0);
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
	if( _t >= 0)
		return pushArgument(mb, q, _t);
	return q;
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
				str msg = createException(MAL, "pushNil", "Can not allocate nil variable");
				addMalException(mb, msg);
				freeException(msg);
			} else
				VALset(&cst, tpe, p);
		} else {
			if (VALinit(&cst, tpe, ATOMnilptr(tpe)) == NULL) {
				str msg =  createException(MAL, "pushNil", "Can not allocate nil variable");
				addMalException(mb, msg);
				freeException(msg);
			}
		}
		_t = defConstant(mb,tpe,&cst);
	} else {
		cst.vtype = TYPE_bat;
		cst.val.bval = bat_nil;
		_t = defConstant(mb,TYPE_bat,&cst);
		getVarType(mb,_t) = tpe;
	}
	if( _t >= 0){
		q= pushArgument(mb, q, _t);
	}
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
	idx= getAtomIndex(tpe, strlen(tpe), TYPE_any);
	if( idx < 0 || idx >= GDKatomcnt || idx >= MAXATOMS){
		str msg = createException(MAL, "pushNilType", "Can not allocate type variable");
		addMalException(mb, msg);
		freeException(msg);
	} else {
		cst.vtype=TYPE_void;
		cst.val.oval= oid_nil;
		cst.len = 0;
		msg = convertConstant(idx, &cst);
		if (msg != MAL_SUCCEED) {
			addMalException(mb, msg);
			freeException(msg);
		} else {
			_t = defConstant(mb,idx,&cst);
			if( _t >= 0){
				return pushArgument(mb, q, _t);
			}
		}
	}
	return q;
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
	if (msg != MAL_SUCCEED){
		addMalException(mb, msg);
		freeException(msg);
	} else {
		_t = defConstant(mb,tpe,&cst);
		if( _t >= 0){
			return pushArgument(mb, q, _t);
		}
	}
	return q;
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
		addMalException(mb, msg);
		freeException(msg);
	} else {
		_t = defConstant(mb,tpe,&cst);
		if( _t >= 0)
			return pushArgument(mb, q, _t);
	}
	return q;
}

InstrPtr
pushEmptyBAT(MalBlkPtr mb, InstrPtr q, int tpe)
{
	if (q == NULL)
		return NULL;
	setModuleId(q, getName("bat"));
	setFunctionId(q, getName("new"));

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
		str msg = createException(MAL, "pushValue", "Can not allocate variable");
		addMalException(mb, msg);
		freeException(msg);
	} else {
		_t = defConstant(mb,cst.vtype,&cst);
		if( _t >=0 )
			return pushArgument(mb, q, _t);
	}
	return q;
}
