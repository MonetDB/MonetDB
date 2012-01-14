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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include "jaqlgencode.h"
#include "opt_prelude.h"

/* returns a bat with subset from kind bat (:oid,:chr) which are
 * referenced by the first array of the JSON structure (oid 0@0 of kind
 * bat, pointing to array, so all oids from array bat that have head oid
 * value 0@0) */
static int
dumpwalkvar(MalBlkPtr mb, tree *t, int j1, int j5)
{
	InstrPtr q;
	int a;

	/* these operations all operate within a pipe */
	assert(t && t->type == j_var);
	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, algebraRef);
	setFunctionId(q, putName("selectH", 7));
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, j1);
	q = pushOid(mb, q, 0);
	a = getArg(q, 0);
	pushInstruction(mb, q);
	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, algebraRef);
	setFunctionId(q, semijoinRef);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, j5);
	q = pushArgument(mb, q, a);
	a = getArg(q, 0);
	pushInstruction(mb, q);
	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, batRef);
	setFunctionId(q, reverseRef);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, a);
	a = getArg(q, 0);
	pushInstruction(mb, q);
	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, algebraRef);
	setFunctionId(q, semijoinRef);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, j1);
	q = pushArgument(mb, q, a);
	a = getArg(q, 0);
	pushInstruction(mb, q);
	return a;
}

/* returns a bat with in the head the oid of the values (kind bat), and
 * in the tail, the oid of the corresponding element from elems
 * (typically array bat, head oid 0@0) */
static int
dumprefvar(MalBlkPtr mb, tree *t, int elems, int j1, int j6, int j7)
{
	InstrPtr q;
	int a, b;

	assert(t && t->type == j_var);

	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, batRef);
	setFunctionId(q, mirrorRef);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, elems);
	b = getArg(q, 0);
	pushInstruction(mb, q);

	if (t->tval1 == NULL) /* just var, has no derefs, so all */
		return b;

	a = elems;
	for (t = t->tval1; t != NULL; t = t->tval1) {
		if (a != elems) { /* retrieve kinds on multiple indirections */
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, semijoinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, j1);
			q = pushArgument(mb, q, b);
			a = getArg(q, 0);
			pushInstruction(mb, q);
		}
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, uselectRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, a);
		q = pushChr(mb, q, 'o');  /* deref requires object */
		a = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, semijoinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, j6);
		q = pushArgument(mb, q, a);
		a = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, reverseRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, a);
		a = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, joinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, a);
		q = pushArgument(mb, q, b);
		b = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, semijoinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, j7);
		q = pushArgument(mb, q, a);
		a = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, uselectRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, a);
		q = pushStr(mb, q, t->sval);
		a = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, semijoinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, b);
		q = pushArgument(mb, q, a);
		b = getArg(q, 0);
		pushInstruction(mb, q);
	}
	return b;
}

/* returns bat with in the head the oids from elems that match the
 * predicate */
static int
dumppred(MalBlkPtr mb, tree *t, int elems, int j1, int j2, int j3, int j4, int j6, int j7)
{
	InstrPtr q;
	int a, b, c, d, e, f, g;

	assert(t != NULL && t->tval1->type == j_var);
	assert(t->tval2->type == j_comp);

	a = dumprefvar(mb, t->tval1, elems, j1, j6, j7);
	if (t->tval3->type != j_var) {
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, reverseRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, a);
		a = getArg(q, 0);
		pushInstruction(mb, q);
	}

	switch (t->tval3->type) {
		case j_var:
			b = dumprefvar(mb, t->tval3, elems, j1, j6, j7);
			c = -1;
			break;
		case j_num:
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, joinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, a);
			q = pushArgument(mb, q, j3);
			b = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushLng(mb, q, t->tval3->nval);
			c = getArg(q, 0);
			pushInstruction(mb, q);
			break;
		case j_dbl:
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, joinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, a);
			q = pushArgument(mb, q, j4);
			b = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushDbl(mb, q, t->tval3->dval);
			c = getArg(q, 0);
			pushInstruction(mb, q);
			break;
		case j_str:
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, joinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, a);
			q = pushArgument(mb, q, j2);
			b = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushStr(mb, q, t->tval3->sval);
			c = getArg(q, 0);
			pushInstruction(mb, q);
			break;
		case j_bool:
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, joinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, a);
			q = pushArgument(mb, q, j1);
			b = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			/* FIXME: this pre-selects equality */
			q = pushChr(mb, q, t->tval3->nval == 0 ? 'f' : 't');
			c = getArg(q, 0);
			pushInstruction(mb, q);
			break;
		default:
			assert(0);
	}
	if (c >= 0) {
		switch (t->tval2->cval) {
			case j_equals:
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, uselectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				q = pushArgument(mb, q, c);
				g = getArg(q, 0);
				pushInstruction(mb, q);
				break;
			case j_nequal:
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, uselectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				q = pushArgument(mb, q, c);
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kdifference", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				q = pushArgument(mb, q, c);
				g = getArg(q, 0);
				pushInstruction(mb, q);
				break;
			case j_greater:
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, thetaselectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				q = pushArgument(mb, q, c);
				q = pushStr(mb, q, ">");
				g = getArg(q, 0);
				pushInstruction(mb, q);
				break;
			case j_gequal:
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, thetaselectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				q = pushArgument(mb, q, c);
				q = pushStr(mb, q, ">=");
				g = getArg(q, 0);
				pushInstruction(mb, q);
				break;
			case j_less:
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, thetaselectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				q = pushArgument(mb, q, c);
				q = pushStr(mb, q, "<");
				g = getArg(q, 0);
				pushInstruction(mb, q);
				break;
			case j_lequal:
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, thetaselectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				q = pushArgument(mb, q, c);
				q = pushStr(mb, q, "<=");
				g = getArg(q, 0);
				pushInstruction(mb, q);
				break;
			default:
				assert(0);
		}
	} else {  /* var <cmp> var */
		int lv[4] = {j2, j3, j4, 0}, *lp = lv;
		/* FIXME: we need to check that a and b have at most one value
		 * per elem here, further code assumes that, because its
		 * semantically unclear what one should do with multiple values
		 * per element (e.g. $.reviews[*].rating), in fact I believe its
		 * impossible to say something useful about it */
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, newRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushType(mb, q, TYPE_oid);
		q = pushType(mb, q, TYPE_oid);
		g = getArg(q, 0);
		pushInstruction(mb, q);
		for (; *lp != 0; lp++) {
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, semijoinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, *lp);
			q = pushArgument(mb, q, a);
			f = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, semijoinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, *lp);
			q = pushArgument(mb, q, b);
			e = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, reverseRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, e);
			e = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			switch (t->tval2->cval) {
				case j_equals:
				case j_nequal: /* difference handled later */
					setFunctionId(q, joinRef);
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, f);
					q = pushArgument(mb, q, e);
					e = getArg(q, 0);
					pushInstruction(mb, q);
					break;
				case j_greater:
					setFunctionId(q, thetajoinRef);
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, f);
					q = pushArgument(mb, q, e);
					q = pushInt(mb, q, 1);
					e = getArg(q, 0);
					pushInstruction(mb, q);
					break;
				case j_gequal:
					setFunctionId(q, thetajoinRef);
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, f);
					q = pushArgument(mb, q, e);
					q = pushInt(mb, q, 2);
					e = getArg(q, 0);
					pushInstruction(mb, q);
					break;
				case j_less:
					setFunctionId(q, thetajoinRef);
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, f);
					q = pushArgument(mb, q, e);
					q = pushInt(mb, q, -1);
					e = getArg(q, 0);
					pushInstruction(mb, q);
					break;
				case j_lequal:
					setFunctionId(q, thetajoinRef);
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, f);
					q = pushArgument(mb, q, e);
					q = pushInt(mb, q, -2);
					e = getArg(q, 0);
					pushInstruction(mb, q);
					break;
				default:
					assert(0);
			}
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, semijoinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, a);
			q = pushArgument(mb, q, e);
			d = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, reverseRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, e);
			e = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, semijoinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, b);
			q = pushArgument(mb, q, e);
			e = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, reverseRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, e);
			e = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, mirrorRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, e);
			e = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, joinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, d);
			q = pushArgument(mb, q, e);
			e = getArg(q, 0);
			pushInstruction(mb, q);
			if (t->tval2->cval == j_nequal) {
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				q = pushArgument(mb, q, f);
				f = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kdifference", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, f);
				q = pushArgument(mb, q, e);
				e = getArg(q, 0);
				pushInstruction(mb, q);
			}
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, reverseRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, e);
			e = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, insertRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, g);
			q = pushArgument(mb, q, e);
			g = getArg(q, 0);
			pushInstruction(mb, q);
		}
	}
	return g;
}

int
dumptree(jc *j, MalBlkPtr mb, tree *t)
{
	InstrPtr q;
	int j1 = 0, j2 = 0, j3 = 0, j4 = 0, j5 = 0, j6 = 0, j7 = 0;
	int a, b;

	/* each iteration in this loop is a pipe (a JSON document)
	 * represented by the j1..7 vars */
	while (t != NULL) {
		switch (t->type) {
			case j_output_var:
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, putName("jaql", 4));
				setFunctionId(q, putName("setVar", 6));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushStr(mb, q, t->sval);
				q = pushArgument(mb, q, j1);
				q = pushArgument(mb, q, j2);
				q = pushArgument(mb, q, j3);
				q = pushArgument(mb, q, j4);
				q = pushArgument(mb, q, j5);
				q = pushArgument(mb, q, j6);
				q = pushArgument(mb, q, j7);
				a = getArg(q, 0);
				pushInstruction(mb, q);
				break;
			case j_output:
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, ioRef);
				setFunctionId(q, putName("stdout", 6));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				a = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, putName("json", 4));
				setFunctionId(q, printRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_void));
				q = pushArgument(mb, q, a);
				q = pushArgument(mb, q, j1);
				q = pushArgument(mb, q, j2);
				q = pushArgument(mb, q, j3);
				q = pushArgument(mb, q, j4);
				q = pushArgument(mb, q, j5);
				q = pushArgument(mb, q, j6);
				q = pushArgument(mb, q, j7);
				pushInstruction(mb, q);
				break;
			case j_json:
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, putName("json", 4));
				setFunctionId(q, putName("shred", 5));
				q = pushReturn(mb, q,
						newTmpVariable(mb, newBatType(TYPE_oid, TYPE_chr)));
				j1 = getArg(q, 0);
				setVarUDFtype(mb, j1);
				q = pushReturn(mb, q,
						newTmpVariable(mb, newBatType(TYPE_oid, TYPE_str)));
				j2 = getArg(q, 1);
				setVarUDFtype(mb, j2);
				q = pushReturn(mb, q,
						newTmpVariable(mb, newBatType(TYPE_oid, TYPE_lng)));
				j3 = getArg(q, 2);
				setVarUDFtype(mb, j3);
				q = pushReturn(mb, q,
						newTmpVariable(mb, newBatType(TYPE_oid, TYPE_dbl)));
				j4 = getArg(q, 3);
				setVarUDFtype(mb, j4);
				q = pushReturn(mb, q,
						newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid)));
				j5 = getArg(q, 4);
				setVarUDFtype(mb, j5);
				q = pushReturn(mb, q,
						newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid)));
				j6 = getArg(q, 5);
				setVarUDFtype(mb, j6);
				q = pushReturn(mb, q,
						newTmpVariable(mb, newBatType(TYPE_oid, TYPE_str)));
				j7 = getArg(q, 6);
				setVarUDFtype(mb, j7);

				q = pushStr(mb, q, t->sval);
				pushInstruction(mb, q);
				break;
			case j_var:
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, putName("jaql", 4));
				setFunctionId(q, putName("getVar", 6));
				q = pushReturn(mb, q,
						newTmpVariable(mb, newBatType(TYPE_oid, TYPE_chr)));
				j1 = getArg(q, 0);
				setVarUDFtype(mb, j1);
				q = pushReturn(mb, q,
						newTmpVariable(mb, newBatType(TYPE_oid, TYPE_str)));
				j2 = getArg(q, 1);
				setVarUDFtype(mb, j2);
				q = pushReturn(mb, q,
						newTmpVariable(mb, newBatType(TYPE_oid, TYPE_lng)));
				j3 = getArg(q, 2);
				setVarUDFtype(mb, j3);
				q = pushReturn(mb, q,
						newTmpVariable(mb, newBatType(TYPE_oid, TYPE_dbl)));
				j4 = getArg(q, 3);
				setVarUDFtype(mb, j4);
				q = pushReturn(mb, q,
						newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid)));
				j5 = getArg(q, 4);
				setVarUDFtype(mb, j5);
				q = pushReturn(mb, q,
						newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid)));
				j6 = getArg(q, 5);
				setVarUDFtype(mb, j6);
				q = pushReturn(mb, q,
						newTmpVariable(mb, newBatType(TYPE_oid, TYPE_str)));
				j7 = getArg(q, 6);
				setVarUDFtype(mb, j7);

				q = pushStr(mb, q, t->sval);
				pushInstruction(mb, q);
			case j_pipe:
				break;
			case j_filter:
				a = dumpwalkvar(mb, t->tval1, j1, j5);
				/* tval2 can be pred or cpred */
				b = dumppred(mb, t->tval2, a, j1, j2, j3, j4, j6, j7);
				/* b = matching ids from dumpwalkvar (first array) */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kdifference", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				q = pushArgument(mb, q, b);
				a = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, projectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				q = pushOid(mb, q, 0);
				a = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				a = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("sdifference", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j5);
				q = pushArgument(mb, q, a);
				j5 = getArg(q, 0);
				pushInstruction(mb, q);
				break;
			case j_transform:
				break;
			case j_expand:
				a = dumpwalkvar(mb, t->tval1, j1, j5);
				a = dumprefvar(mb, t->tval2, a, j1, j6, j7);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j1);
				q = pushArgument(mb, q, a);
				a = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, uselectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				q = pushChr(mb, q, 'a');  /* only arrays match expand */
				a = getArg(q, 0);
				pushInstruction(mb, q);
				/* construct json with these elements in the outermost
				 * array, reusing old bits */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j5);
				q = pushArgument(mb, q, a);
				a = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, projectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushOid(mb, q, 0);  /* 0@0 = outermost array */
				q = pushArgument(mb, q, a);
				b = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("selectH", 7));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j1);
				q = pushOid(mb, q, 0);
				a = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j5);
				q = pushArgument(mb, q, a);
				a = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, sunionRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				q = pushArgument(mb, q, b);
				a = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("sdifference", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j5);
				q = pushArgument(mb, q, a);
				a = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, insertRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				q = pushArgument(mb, q, b);
				j5 = getArg(q, 0);
				pushInstruction(mb, q);
				break;
			case j_cmpnd:
			case j_comp:
			case j_pred:
				break;
			case j_num:
			case j_dbl:
			case j_str:
			case j_bool:
				break;
			case j_error:
				snprintf(j->err, sizeof(j->err), "%s", t->sval);
				break;
			default:
				snprintf(j->err, sizeof(j->err), "unhandled type (1)");
				return -1;
		}
		t = t->next;
	}
	return -1;
}
