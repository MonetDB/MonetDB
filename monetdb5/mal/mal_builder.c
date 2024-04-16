/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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
	k = newTmpVariable(mb, TYPE_any);
	if (k < 0) {
		// construct an exception message to be passed to upper layers using ->errors
		str msg = createException(MAL, "newAssignment",
								  "Can not allocate variable");
		addMalException(mb, msg);
		freeException(msg);
		freeInstruction(q);
		return NULL;
	}
	getArg(q, 0) = k;
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

	if (mName == NULL || nName == NULL)
		return NULL;

	q = newInstructionArgs(mb, mName, nName, args);
	if (q == NULL)
		return NULL;

	setDestVar(q, newTmpVariable(mb, TYPE_any));
	if (getDestVar(q) < 0) {
		str msg = createException(MAL, "newStmtArgs",
								  "Can not allocate variable");
		addMalException(mb, msg);
		freeException(msg);
		freeInstruction(q);
		return NULL;
	}
	return q;
}

InstrPtr
newReturnStmt(MalBlkPtr mb)
{
	InstrPtr q = newAssignment(mb);

	if (q != NULL)
		q->barrier = RETURNsymbol;
	return q;
}

InstrPtr
newFcnCallArgs(MalBlkPtr mb, const char *mod, const char *fcn, int args)
{
	const char *fcnName, *modName;
	modName = putName(mod);
	fcnName = putName(fcn);
	if (modName == NULL || fcnName == NULL)
		return NULL;

	InstrPtr q = newAssignmentArgs(mb, args);

	if (q != NULL) {
		setModuleId(q, modName);
		setFunctionId(q, fcnName);
	}
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
	if (VALinit(&cst, TYPE_str, val) == NULL) {
		str msg = createException(MAL, "newComment", "Can not allocate comment");
		addMalException(mb, msg);
		freeException(msg);
		freeInstruction(q);
		return NULL;
	}
	k = defConstant(mb, TYPE_str, &cst);
	if (k < 0) {
		freeInstruction(q);
		return NULL;
	}
	getArg(q, 0) = k;
	clrVarConstant(mb, getArg(q, 0));
	setVarDisabled(mb, getArg(q, 0));
	return q;
}

InstrPtr
newCatchStmt(MalBlkPtr mb, const char *nme)
{
	InstrPtr q = newAssignment(mb);
	int i = findVariable(mb, nme);

	if (q == NULL)
		return NULL;
	q->barrier = CATCHsymbol;
	if (i < 0) {
		i = newVariable(mb, nme, strlen(nme), TYPE_str);
		if (i < 0) {
			str msg = createException(MAL, "newCatchStmt",
									  "Can not allocate variable");
			addMalException(mb, msg);
			freeException(msg);
			freeInstruction(q);
			return NULL;
		}
	}
	getArg(q, 0) = i;
	return q;
}

InstrPtr
newRaiseStmt(MalBlkPtr mb, const char *nme)
{
	InstrPtr q = newAssignment(mb);
	int i = findVariable(mb, nme);

	if (q == NULL)
		return NULL;
	q->barrier = RAISEsymbol;
	if (i < 0) {
		i = newVariable(mb, nme, strlen(nme), TYPE_str);
		if (i < 0) {
			str msg = createException(MAL, "newRaiseStmt",
									  "Can not allocate variable");
			addMalException(mb, msg);
			freeException(msg);
			freeInstruction(q);
			return NULL;
		}
	}
	getArg(q, 0) = i;
	return q;
}

InstrPtr
newExitStmt(MalBlkPtr mb, const char *nme)
{
	InstrPtr q = newAssignment(mb);
	int i = findVariable(mb, nme);

	if (q == NULL)
		return NULL;
	q->barrier = EXITsymbol;
	if (i < 0) {
		i = newVariable(mb, nme, strlen(nme), TYPE_str);
		if (i < 0) {
			str msg = createException(MAL, "newExitStmt",
									  "Can not allocate variable");
			addMalException(mb, msg);
			freeException(msg);
			freeInstruction(q);
			return NULL;
		}
	}
	getArg(q, 0) = i;
	return q;
}

InstrPtr
pushEndInstruction(MalBlkPtr mb)
{
	if (mb->errors)
		return NULL;
	InstrPtr q = newInstruction(mb, NULL, NULL);

	if (q == NULL)
		return NULL;
	q->token = ENDsymbol;
	q->barrier = 0;
	q->argc = 0;
	q->retc = 0;
	q->argv[0] = 0;
	pushInstruction(mb, q);
	if (mb->errors)
		return NULL;
	return q;
}

int
getIntConstant(MalBlkPtr mb, int val)
{
	int _t;
	ValRecord cst = { .vtype = TYPE_int, .val.ival = val };

	_t = fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if (_t < 0)
		_t = defConstant(mb, TYPE_int, &cst);
	return _t;
}

InstrPtr
pushInt(MalBlkPtr mb, InstrPtr q, int val)
{
	int _t;

	if (q == NULL || mb->errors)
		return q;
	ValRecord cst = { .vtype = TYPE_int, .val.ival = val };
	_t = defConstant(mb, TYPE_int, &cst);
	if (_t >= 0)
		return pushArgument(mb, q, _t);
	return q;
}

int
getBteConstant(MalBlkPtr mb, bte val)
{
	int _t;
	ValRecord cst = { .vtype = TYPE_bte, .val.btval = val };

	_t = fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if (_t < 0)
		_t = defConstant(mb, TYPE_bte, &cst);
	return _t;
}

InstrPtr
pushBte(MalBlkPtr mb, InstrPtr q, bte val)
{
	int _t;

	if (q == NULL || mb->errors)
		return q;
	ValRecord cst = { .vtype = TYPE_bte, .val.btval = val };
	_t = defConstant(mb, TYPE_bte, &cst);
	if (_t >= 0)
		return pushArgument(mb, q, _t);
	return q;
}

int
getOidConstant(MalBlkPtr mb, oid val)
{
	int _t;

	ValRecord cst = { .vtype = TYPE_oid, .val.oval = val };
	_t = fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if (_t < 0)
		_t = defConstant(mb, TYPE_oid, &cst);
	return _t;
}

InstrPtr
pushOid(MalBlkPtr mb, InstrPtr q, oid val)
{
	int _t;

	if (q == NULL || mb->errors)
		return q;
	ValRecord cst = { .vtype = TYPE_oid, .val.oval = val };
	_t = defConstant(mb, TYPE_oid, &cst);
	if (_t >= 0)
		return pushArgument(mb, q, _t);
	return q;
}

InstrPtr
pushVoid(MalBlkPtr mb, InstrPtr q)
{
	int _t;

	if (q == NULL || mb->errors)
		return q;
	ValRecord cst = { .vtype = TYPE_void, .val.oval = oid_nil };
	_t = defConstant(mb, TYPE_void, &cst);
	if (_t >= 0)
		return pushArgument(mb, q, _t);
	return q;
}

int
getLngConstant(MalBlkPtr mb, lng val)
{
	int _t;

	ValRecord cst = { .vtype = TYPE_lng, .val.lval = val };
	_t = fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if (_t < 0)
		_t = defConstant(mb, TYPE_lng, &cst);
	return _t;
}

InstrPtr
pushLng(MalBlkPtr mb, InstrPtr q, lng val)
{
	int _t;

	if (q == NULL || mb->errors)
		return q;
	ValRecord cst = { .vtype = TYPE_lng, .val.lval = val };
	_t = defConstant(mb, TYPE_lng, &cst);
	if (_t >= 0)
		return pushArgument(mb, q, _t);
	return q;
}

int
getShtConstant(MalBlkPtr mb, sht val)
{
	int _t;

	ValRecord cst = { .vtype = TYPE_sht, .val.shval = val };
	_t = fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if (_t < 0)
		_t = defConstant(mb, TYPE_sht, &cst);
	return _t;
}

InstrPtr
pushSht(MalBlkPtr mb, InstrPtr q, sht val)
{
	int _t;

	if (q == NULL || mb->errors)
		return q;
	ValRecord cst = { .vtype = TYPE_sht, .val.shval = val };
	_t = defConstant(mb, TYPE_sht, &cst);
	if (_t >= 0)
		return pushArgument(mb, q, _t);
	return q;
}

#ifdef HAVE_HGE
int
getHgeConstant(MalBlkPtr mb, hge val)
{
	int _t;

	ValRecord cst = { .vtype = TYPE_hge, .val.hval = val };
	_t = fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if (_t < 0)
		_t = defConstant(mb, TYPE_hge, &cst);
	return _t;
}

InstrPtr
pushHge(MalBlkPtr mb, InstrPtr q, hge val)
{
	int _t;

	if (q == NULL || mb->errors)
		return q;
	ValRecord cst = { .vtype = TYPE_hge, .val.hval = val };
	_t = defConstant(mb, TYPE_hge, &cst);
	if (_t >= 0)
		return pushArgument(mb, q, _t);
	return q;
}
#endif

int
getDblConstant(MalBlkPtr mb, dbl val)
{
	int _t;

	ValRecord cst = { .vtype = TYPE_dbl, .val.dval = val };
	_t = fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if (_t < 0)
		_t = defConstant(mb, TYPE_dbl, &cst);
	return _t;
}

InstrPtr
pushDbl(MalBlkPtr mb, InstrPtr q, dbl val)
{
	int _t;

	if (q == NULL || mb->errors)
		return q;
	ValRecord cst = { .vtype = TYPE_dbl, .val.dval = val };
	_t = defConstant(mb, TYPE_dbl, &cst);
	if (_t >= 0)
		return pushArgument(mb, q, _t);
	return q;
}

int
getFltConstant(MalBlkPtr mb, flt val)
{
	int _t;

	ValRecord cst = { .vtype = TYPE_flt, .val.fval = val };
	_t = fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if (_t < 0)
		_t = defConstant(mb, TYPE_flt, &cst);
	return _t;
}

InstrPtr
pushFlt(MalBlkPtr mb, InstrPtr q, flt val)
{
	int _t;

	if (q == NULL || mb->errors)
		return q;
	ValRecord cst = { .vtype = TYPE_flt, .val.fval = val };
	_t = defConstant(mb, TYPE_flt, &cst);
	if (_t >= 0)
		return pushArgument(mb, q, _t);
	return q;
}

int
getStrConstant(MalBlkPtr mb, str val)
{
	int _t;
	ValRecord cst;

	VALset(&cst, TYPE_str, val);
	_t = fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if (_t < 0) {
		if ((cst.val.sval = GDKmalloc(cst.len)) == NULL)
			return -1;
		memcpy(cst.val.sval, val, cst.len);	/* includes terminating \0 */
		_t = defConstant(mb, TYPE_str, &cst);
	}
	return _t;
}

InstrPtr
pushStr(MalBlkPtr mb, InstrPtr q, const char *Val)
{
	int _t;
	ValRecord cst;

	if (q == NULL || mb->errors)
		return q;
	if (VALinit(&cst, TYPE_str, Val) == NULL) {
		str msg = createException(MAL, "pushStr",
								  "Can not allocate string variable");
		addMalException(mb, msg);
		freeException(msg);
	} else {
		_t = defConstant(mb, TYPE_str, &cst);
		if (_t >= 0)
			return pushArgument(mb, q, _t);
	}
	return q;
}

int
getBitConstant(MalBlkPtr mb, bit val)
{
	int _t;

	ValRecord cst = { .vtype = TYPE_bit, .val.btval = val };
	_t = fndConstant(mb, &cst, MAL_VAR_WINDOW);
	if (_t < 0)
		_t = defConstant(mb, TYPE_bit, &cst);
	return _t;
}

InstrPtr
pushBit(MalBlkPtr mb, InstrPtr q, bit val)
{
	int _t;

	if (q == NULL || mb->errors)
		return q;
	ValRecord cst = { .vtype = TYPE_bit, .val.btval = val };
	_t = defConstant(mb, TYPE_bit, &cst);
	if (_t >= 0)
		return pushArgument(mb, q, _t);
	return q;
}

InstrPtr
pushNil(MalBlkPtr mb, InstrPtr q, int tpe)
{
	int _t;
	ValRecord cst = { .len = 0 };

	if (q == NULL || mb->errors)
		return q;
	if (!isaBatType(tpe)) {
		assert(tpe < MAXATOMS);	/* in particular, tpe!=TYPE_any */
		if (tpe == TYPE_void) {
			cst.vtype = TYPE_void;
			cst.val.oval = oid_nil;
		} else {
			if (VALinit(&cst, tpe, ATOMnilptr(tpe)) == NULL) {
				str msg = createException(MAL, "pushNil",
										  "Can not allocate nil variable");
				addMalException(mb, msg);
				freeException(msg);
			}
		}
		_t = defConstant(mb, tpe, &cst);
	} else {
		cst.vtype = TYPE_void;
		cst.bat = true;
		cst.val.bval = bat_nil;
		_t = defConstant(mb, newBatType(TYPE_void), &cst);
		getVarType(mb, _t) = tpe;
	}
	if (_t >= 0) {
		q = pushArgument(mb, q, _t);
	}
	return q;
}

InstrPtr
pushNilBat(MalBlkPtr mb, InstrPtr q)
{
	int _t;
	ValRecord cst = { .bat = true, .vtype = TYPE_void, .val.bval = bat_nil };

	if (q == NULL || mb->errors)
		return q;
	_t = defConstant(mb, newBatType(TYPE_void), &cst);
	getVarType(mb, _t) = newBatType(TYPE_any);
	if (_t >= 0) {
		q = pushArgument(mb, q, _t);
	}
	return q;
}


InstrPtr
pushNilType(MalBlkPtr mb, InstrPtr q, char *tpe)
{
	int _t, idx;
	str msg;

	if (q == NULL || mb->errors)
		return q;
	idx = getAtomIndex(tpe, strlen(tpe), TYPE_any);
	if (idx < 0 || idx >= GDKatomcnt || idx >= MAXATOMS) {
		msg = createException(MAL, "pushNilType",
							  "Can not allocate type variable");
	} else {
		ValRecord cst = { .vtype = TYPE_void, .val.oval = oid_nil };

		msg = convertConstant(idx, &cst);
		if (msg == MAL_SUCCEED) {
			_t = defConstant(mb, idx, &cst);
			if (_t >= 0) {
				return pushArgument(mb, q, _t);
			}
		}
	}
	if (msg) {
		addMalException(mb, msg);
		freeException(msg);
	}
	return q;
}

InstrPtr
pushType(MalBlkPtr mb, InstrPtr q, int tpe)
{
	int _t;
	str msg;

	if (q == NULL || mb->errors)
		return q;
	ValRecord cst = { .vtype = TYPE_void, .val.oval = oid_nil };
	msg = convertConstant(tpe, &cst);
	if (msg != MAL_SUCCEED) {
		addMalException(mb, msg);
		freeException(msg);
	} else {
		_t = defConstant(mb, tpe, &cst);
		if (_t >= 0) {
			return pushArgument(mb, q, _t);
		}
	}
	return q;
}

InstrPtr
pushZero(MalBlkPtr mb, InstrPtr q, int tpe)
{
	int _t;
	str msg;

	if (q == NULL || mb->errors)
		return q;
	ValRecord cst = { .vtype = TYPE_int, .val.ival = 0 };
	msg = convertConstant(tpe, &cst);
	if (msg != MAL_SUCCEED) {
		addMalException(mb, msg);
		freeException(msg);
	} else {
		_t = defConstant(mb, tpe, &cst);
		if (_t >= 0)
			return pushArgument(mb, q, _t);
	}
	return q;
}

InstrPtr
pushValue(MalBlkPtr mb, InstrPtr q, const ValRecord *vr)
{
	int _t;
	ValRecord cst;

	if (q == NULL || mb->errors)
		return q;
	if (VALcopy(&cst, vr) == NULL) {
		str msg = createException(MAL, "pushValue", "Can not allocate variable");
		addMalException(mb, msg);
		freeException(msg);
	} else {
		int type = cst.bat?newBatType(cst.vtype):cst.vtype;
		_t = defConstant(mb, type, &cst);
		if (_t >= 0)
			return pushArgument(mb, q, _t);
	}
	return q;
}
