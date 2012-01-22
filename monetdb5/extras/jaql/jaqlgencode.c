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
dumpwalkvar(MalBlkPtr mb, int j1, int j5)
{
	InstrPtr q;
	int a;

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
 * comparison */
static int
dumpcomp(MalBlkPtr mb, tree *t, int elems, int j1, int j2, int j3, int j4, int j6, int j7)
{
	InstrPtr q;
	int a, b, c, d, e, f, g;

	assert(t != NULL);
	assert(t->tval1->type == j_var);
	assert(t->tval2->type == j_comp);
	assert(t->tval3->type == j_var
			|| t->tval3->type == j_num || t->tval3->type == j_dbl
			|| t->tval3->type == j_str || t->tval3->type == j_bool);

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
			setModuleId(q, algebraRef);
			setFunctionId(q, uselectRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, b);
			/* boolean conditions can only be j_equals or j_nequal */
			if (t->tval2->cval == j_equals) {
				q = pushChr(mb, q, t->tval3->nval == 0 ? 'f' : 't');
			} else {
				q = pushChr(mb, q, t->tval3->nval != 0 ? 'f' : 't');
			}
			b = getArg(q, 0);
			pushInstruction(mb, q);
			return b;
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
				setFunctionId(q, thetauselectRef);
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
				setFunctionId(q, thetauselectRef);
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
				setFunctionId(q, thetauselectRef);
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
				setFunctionId(q, thetauselectRef);
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
		setFunctionId(q, reverseRef);
		q = pushArgument(mb, q, a);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		c = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, joinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, b);
		q = pushArgument(mb, q, c);
		c = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, reverseRef);
		q = pushArgument(mb, q, c);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		d = getArg(q, 0);
		pushInstruction(mb, q);

		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, semijoinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, j1);
		q = pushArgument(mb, q, c);
		c = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, reverseRef);
		q = pushArgument(mb, q, b);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		e = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, joinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, e);
		q = pushArgument(mb, q, c);
		c = getArg(q, 0);
		pushInstruction(mb, q);

		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, semijoinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, j1);
		q = pushArgument(mb, q, d);
		d = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, reverseRef);
		q = pushArgument(mb, q, a);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		e = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, joinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, e);
		q = pushArgument(mb, q, d);
		d = getArg(q, 0);
		pushInstruction(mb, q);

		/* booleans can only be compared with j_equals and
		 * j_nequal */
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, selectRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, c);
		q = pushChr(mb, q, 't');
		e = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, selectRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, c);
		q = pushChr(mb, q, 'f');
		f = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, putName("kunion", 6););
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, e);
		q = pushArgument(mb, q, f);
		c = getArg(q, 0);
		pushInstruction(mb, q);

		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, selectRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, d);
		q = pushChr(mb, q, 't');
		e = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, selectRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, d);
		q = pushChr(mb, q, 'f');
		f = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, putName("kunion", 6););
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, e);
		q = pushArgument(mb, q, f);
		d = getArg(q, 0);
		pushInstruction(mb, q);

		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, reverseRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, d);
		e = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, joinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, c);
		q = pushArgument(mb, q, e);
		e = getArg(q, 0);
		pushInstruction(mb, q);

		if (t->tval2->cval == j_nequal) {
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, putName("kdifference", 11));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, c);
			q = pushArgument(mb, q, e);
			e = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, projectRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, e);
			q = pushNil(mb, q, TYPE_oid);
			e = getArg(q, 0);
			pushInstruction(mb, q);
		}

		g = e;

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

static int
dumppred(MalBlkPtr mb, tree *t, int elems, int j1, int j2, int j3, int j4, int j6, int j7)
{
	int a, l, r;
	InstrPtr q;

	assert(t != NULL && t->tval2->type == j_comp);

	/* comparisons only take place between tval1 = var and tval3 = val/var
	 * for the rest, only boolean logic is applied */
	if (t->tval2->cval != j_and && t->tval2->cval != j_or)
		return dumpcomp(mb, t, elems, j1, j2, j3, j4, j6, j7);

	assert(t->tval1->type == j_pred);
	assert(t->tval2->cval == j_and || t->tval2->cval == j_or);
	assert(t->tval3->type == j_pred);

	l = dumppred(mb, t->tval1, elems, j1, j2, j3, j4, j6, j7);
	r = dumppred(mb, t->tval3, elems, j1, j2, j3, j4, j6, j7);
	/* l,r = oid from elems that match in head */

	if (t->tval2->cval == j_and) {
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, putName("kintersect", 10));
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, l);
		q = pushArgument(mb, q, r);
		a = getArg(q, 0);
		pushInstruction(mb, q);
	} else { /* j_or */
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, putName("kunion", 6));
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, l);
		q = pushArgument(mb, q, r);
		a = getArg(q, 0);
		pushInstruction(mb, q);
	}

	return a;
}

static int
dumpnextid(MalBlkPtr mb, int j1)
{
	int a;
	InstrPtr q;

	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, batRef);
	setFunctionId(q, reverseRef);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, j1);
	a = getArg(q, 0);
	pushInstruction(mb, q);
	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, aggrRef);
	setFunctionId(q, maxRef);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, a);
	a = getArg(q, 0);
	pushInstruction(mb, q);
	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, calcRef);
	setFunctionId(q, putName("wrd", 3));
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, a);
	a = getArg(q, 0);
	pushInstruction(mb, q);
	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, calcRef);
	setFunctionId(q, putName("+", 1));
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, a);
	q = pushWrd(mb, q, 1);
	a = getArg(q, 0);
	pushInstruction(mb, q);
	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, calcRef);
	setFunctionId(q, oidRef);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, a);
	a = getArg(q, 0);
	pushInstruction(mb, q);

	return a;
}

/* returns a BAT which is the subset of j1 that refer to the values
 * returned from the variable and its optional calculation applied to it
 * the j{1..7} variables are updated to point to the updated BATs as
 * insertions of new values (the serialised versions of the variable) */
static int
dumpvariabletransformation(MalBlkPtr mb, tree *t, int elems,
		int *j1, int *j2, int *j3, int *j4, int *j5, int *j6, int *j7)
{
	InstrPtr q;
	int a, b, c, d, e, f, g;

	assert (t != NULL);

	switch (t->type) {
		case j_str:
		case j_num:
		case j_dbl:
			/* shortcut, simple action */
			a = dumpnextid(mb, *j1);

			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, projectRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, elems);
			if (t->type == j_num) {
				q = pushLng(mb, q, t->nval);
			} else if (t->type == j_dbl) {
				q = pushDbl(mb, q, t->dval);
			} else {
				q = pushStr(mb, q, t->sval);
			}
			b = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, projectRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, elems);
			if (t->type == j_num) {
				q = pushChr(mb, q, 'i');
				d = *j3;
			} else if (t->type == j_dbl) {
				q = pushChr(mb, q, 'd');
				d = *j4;
			} else {
				q = pushChr(mb, q, 's');
				d = *j2;
			}
			c = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, putName("markH", 5));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, b);
			q = pushArgument(mb, q, a);
			b = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, putName("kunion", 6));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, d);
			q = pushArgument(mb, q, b);
			if (t->type == j_num) {
				*j3 = getArg(q, 0);
			} else if (t->type == j_dbl) {
				*j4 = getArg(q, 0);
			} else if (t->type == j_str) {
				*j2 = getArg(q, 0);
			}
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, putName("markH", 5));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, c);
			q = pushArgument(mb, q, a);
			c = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, putName("kunion", 6));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, *j1);
			q = pushArgument(mb, q, c);
			*j1 = getArg(q, 0);
			pushInstruction(mb, q);

			return c;
		case j_var: {
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, reverseRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, elems);
			c = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, semijoinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, *j1);
			q = pushArgument(mb, q, c);
			c = getArg(q, 0);
			pushInstruction(mb, q);
			b = dumprefvar(mb, t, c, *j1, *j6, *j7);

			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, semijoinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, *j1);
			q = pushArgument(mb, q, b);
			c = getArg(q, 0);
			pushInstruction(mb, q);

			return c;
		}
		case j_operation: {
			int r, s;
			int u, v;
			int h, i, j, k, l;
			InstrPtr p;
			switch (t->tval1->type) {
				case j_var:
				case j_operation:
					b = dumpvariabletransformation(mb, t->tval1, elems,
							j1, j2, j3, j4, j5, j6, j7);

					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, algebraRef);
					setFunctionId(q, semijoinRef);
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, *j3);
					q = pushArgument(mb, q, b);
					d = getArg(q, 0);
					pushInstruction(mb, q);
					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, algebraRef);
					setFunctionId(q, semijoinRef);
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, *j4);
					q = pushArgument(mb, q, b);
					e = getArg(q, 0);
					pushInstruction(mb, q);
					break;
				default:
					assert(0);
			}
			/* d:int and e:dbl are values from val1 */

			switch (t->tval3->type) {
				case j_var:
				case j_operation:
					c = dumpvariabletransformation(mb, t->tval3, elems,
							j1, j2, j3, j4, j5, j6, j7);

					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, algebraRef);
					setFunctionId(q, semijoinRef);
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, *j3);
					q = pushArgument(mb, q, c);
					f = getArg(q, 0);
					pushInstruction(mb, q);
					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, algebraRef);
					setFunctionId(q, semijoinRef);
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, *j4);
					q = pushArgument(mb, q, c);
					g = getArg(q, 0);
					pushInstruction(mb, q);
					q = newInstruction(mb, ASSIGNsymbol);
					break;
				case j_num:
					c = -1;
					q = newInstruction(mb, ASSIGNsymbol);
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushLng(mb, q, t->tval3->nval);
					f = getArg(q, 0);
					pushInstruction(mb, q);
					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, calcRef);
					setFunctionId(q, putName("dbl", 3));
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, f);
					g = getArg(q, 0);
					pushInstruction(mb, q);
					break;
				case j_dbl:
					c = -1;
					f = -1;
					q = newInstruction(mb, ASSIGNsymbol);
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushDbl(mb, q, t->tval3->dval);
					g = getArg(q, 0);
					pushInstruction(mb, q);
					break;
				default:
					assert(0);
			}
			/* f:int and g:dbl are values from val3, bats if c != -1 */

			a = dumpnextid(mb, *j1);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, markTRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, b);
			q = pushArgument(mb, q, a);
			r = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, reverseRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, r);
			j = getArg(q, 0);
			pushInstruction(mb, q);

			if (c != -1) {
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, r);
				q = pushArgument(mb, q, d);
				s = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, s);
				k = getArg(q, 0);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, markTRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, c);
				q = pushArgument(mb, q, a);
				u = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, u);
				q = pushArgument(mb, q, f);
				v = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, v);
				l = v = getArg(q, 0);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, joinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, s);
				q = pushArgument(mb, q, v);
				s = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, s);
				v = getArg(q, 0);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, d);
				q = pushArgument(mb, q, s);
				s = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, f);
				q = pushArgument(mb, q, v);
				v = getArg(q, 0);
				pushInstruction(mb, q);
			} else {
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, d);
				q = pushArgument(mb, q, r);
				s = getArg(q, 0);
				pushInstruction(mb, q);
				v = f;
			}

			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batcalcRef);
			switch (t->tval2->cval) {
				case j_plus:
					setFunctionId(q, putName("+", 1));
					break;
				case j_min:
					setFunctionId(q, putName("-", 1));
					break;
				case j_multiply:
					setFunctionId(q, putName("*", 1));
					break;
				case j_divide:
					setFunctionId(q, putName("/", 1));
					break;
				default:
					assert(0);
			}
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, s);
			q = pushArgument(mb, q, v);
			if (v != -1) {
				h = getArg(q, 0);
				pushInstruction(mb, q);
			} else {
				h = -1;
			}
			p = copyInstruction(q); /* reuse for dbl case below */

			if (h != -1) {
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, joinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j);
				q = pushArgument(mb, q, h);
				h = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kunion", 6));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, *j3);
				q = pushArgument(mb, q, h);
				*j3 = getArg(q, 0);
				pushInstruction(mb, q);
			}

			if (c != -1) {
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, r);
				q = pushArgument(mb, q, e);
				s = getArg(q, 0);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, u);
				q = pushArgument(mb, q, g);
				v = getArg(q, 0);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, v);
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, k);
				q = pushArgument(mb, q, c);
				k = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, k);
				k = getArg(q, 0);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, d);
				q = pushArgument(mb, q, k);
				i = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batcalcRef);
				setFunctionId(q, putName("dbl", 3));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, i);
				i = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kunion", 6));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, e);
				q = pushArgument(mb, q, i);
				e = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kunion", 6));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, *j4);
				q = pushArgument(mb, q, i);
				*j4 = getArg(q, 0);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kunion", 6));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, s);
				q = pushArgument(mb, q, k);
				k = getArg(q, 0);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, s);
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, c);
				l = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, l);
				l = getArg(q, 0);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, f);
				q = pushArgument(mb, q, l);
				i = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batcalcRef);
				setFunctionId(q, putName("dbl", 3));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, i);
				i = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kunion", 6));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, g);
				q = pushArgument(mb, q, i);
				g = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kunion", 6));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, *j4);
				q = pushArgument(mb, q, i);
				*j4 = getArg(q, 0);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kunion", 6));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, v);
				q = pushArgument(mb, q, l);
				l = getArg(q, 0);
				pushInstruction(mb, q);

				s = k;
				v = l;

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, v);
				v = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, joinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, s);
				q = pushArgument(mb, q, v);
				s = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, s);
				v = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, mirrorRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, s);
				s = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, mirrorRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, v);
				v = getArg(q, 0);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, joinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, s);
				q = pushArgument(mb, q, e);
				s = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, joinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, v);
				q = pushArgument(mb, q, g);
				v = getArg(q, 0);
				pushInstruction(mb, q);
			} else {
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, e);
				q = pushArgument(mb, q, r);
				v = getArg(q, 0);
				pushInstruction(mb, q);

				if (f == -1) {
					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, batcalcRef);
					setFunctionId(q, putName("dbl", 3));
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, s);
					s = getArg(q, 0);
					pushInstruction(mb, q);
					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, algebraRef);
					setFunctionId(q, putName("kunion", 6));
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, v);
					q = pushArgument(mb, q, s);
					s = getArg(q, 0);
					pushInstruction(mb, q);
				} else {
					s = v;
				}

				v = g;
			}

			q = p;
			getArg(q, 1) = s;
			getArg(q, 2) = v;
			s = getArg(q, 0) = newTmpVariable(mb, TYPE_any);
			pushInstruction(mb, q);

			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, joinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, j);
			q = pushArgument(mb, q, s);
			i = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, putName("kunion", 6));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, *j4);
			q = pushArgument(mb, q, i);
			*j4 = getArg(q, 0);
			pushInstruction(mb, q);

			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, projectRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, j);
			q = pushChr(mb, q, 'n');
			r = getArg(q, 0);
			pushInstruction(mb, q);
			if (h != -1) {
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, projectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, h);
				q = pushChr(mb, q, 'i');
				h = getArg(q, 0);
				pushInstruction(mb, q);
			}
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, projectRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, i);
			q = pushChr(mb, q, 'd');
			i = getArg(q, 0);
			pushInstruction(mb, q);

			if (h != -1) {
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kunion", 6));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, h);
				q = pushArgument(mb, q, i);
				h = getArg(q, 0);
				pushInstruction(mb, q);
			} else {
				h = i;
			}
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, putName("kdifference", 11));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, r);
			q = pushArgument(mb, q, h);
			r = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, putName("kunion", 6));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, r);
			q = pushArgument(mb, q, h);
			a = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, sortRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, a);
			a = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, putName("kunion", 6));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, *j1);
			q = pushArgument(mb, q, a);
			*j1 = getArg(q, 0);
			pushInstruction(mb, q);

			return a;
		}
		case j_pair:
			b = dumpvariabletransformation(mb, t->tval1, elems,
					j1, j2, j3, j4, j5, j6, j7);
			/* only need to copy if tval1 is a var, otherwise we have a
			 * new copy already */
			if (t->tval1->type == j_var) {
				int *lv[] = {j2, j3, j4, j5, j6, NULL};
				int **lp = lv;

				a = dumpnextid(mb, *j1);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, markHRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				q = pushArgument(mb, q, a);
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kunion", 6));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, *j1);
				q = pushArgument(mb, q, c);
				*j1 = getArg(q, 0);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, markTRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				q = pushArgument(mb, q, a);
				d = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, d);
				d = getArg(q, 0);
				pushInstruction(mb, q);

				for (; *lp != NULL; lp++) {
					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, algebraRef);
					setFunctionId(q, semijoinRef);
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, **lp);
					q = pushArgument(mb, q, b);
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
					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, algebraRef);
					setFunctionId(q, putName("kunion", 6));
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, **lp);
					q = pushArgument(mb, q, e);
					**lp = getArg(q, 0);
					pushInstruction(mb, q);
				}

				b = c;
			}

			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, projectRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, b);
			q = pushStr(mb, q, t->sval);
			c = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, putName("kunion", 6));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, *j7);
			q = pushArgument(mb, q, c);
			*j7 = getArg(q, 0);
			pushInstruction(mb, q);

			return b;
		case j_json_obj:
			g = 1;
		case j_json_arr:
			g++;
			a = -1;
			c = -1;
			t = t->tval1;
			while (t != NULL) {
				b = dumpvariabletransformation(mb, t, elems,
						j1, j2, j3, j4, j5, j6, j7);
				if (a == -1)
					a = dumpnextid(mb, *j1);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				b = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, markHRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				q = pushArgument(mb, q, a);
				b = getArg(q, 0);
				pushInstruction(mb, q);
				if (c == -1) {
					c = b;
				} else {
					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, batRef);
					setFunctionId(q, insertRef);
					/* can't use sunion, this may have duplicates [$,$] */
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, c);
					q = pushArgument(mb, q, b);
					c = getArg(q, 0);
					pushInstruction(mb, q);
				}
				t = t->next;
			}
			if (a == -1)
				a = dumpnextid(mb, *j1);

			if (c != -1) {
				/* can have duplicates, see above */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, copyRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, g == 2 ? *j6 : *j5);
				d = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, insertRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, d);
				q = pushArgument(mb, q, c);
				if (g == 2) {
					*j6 = getArg(q, 0);
				} else {
					*j5 = getArg(q, 0);
				}
				pushInstruction(mb, q);
			}

			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, markHRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, elems);
			q = pushArgument(mb, q, a);
			d = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, projectRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, d);
			q = pushChr(mb, q, g == 2 ? 'o' : 'a');
			a = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, putName("kunion", 6));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, *j1);
			q = pushArgument(mb, q, a);
			*j1 = getArg(q, 0);
			pushInstruction(mb, q);

			return a;
		default:
			assert(0);
	}
}

int
dumptree(jc *j, MalBlkPtr mb, tree *t)
{
	InstrPtr q;
	int j1 = 0, j2 = 0, j3 = 0, j4 = 0, j5 = 0, j6 = 0, j7 = 0;
	int a, b, c, d, e, f, g;

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
				break;
			case j_filter:
				a = dumpwalkvar(mb, j1, j5);
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
				a = dumpwalkvar(mb, j1, j5);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j5);
				g = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, g);
				q = pushArgument(mb, q, a);
				g = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, g);
				a = getArg(q, 0);
				pushInstruction(mb, q);
				b = dumpvariabletransformation(mb, t->tval2, a,
						&j1, &j2, &j3, &j4, &j5, &j6, &j7);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("sdifference", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j5);
				q = pushArgument(mb, q, a);
				j5 = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("markT", 5));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				q = pushOid(mb, q, 0);
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				d = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("markH", 5));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, d);
				q = pushOid(mb, q, 0);
				d = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, joinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, c);
				q = pushArgument(mb, q, d);
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("sunion", 6));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j5);
				q = pushArgument(mb, q, c);
				j5 = getArg(q, 0);
				pushInstruction(mb, q);
				break;
			case j_expand:
				a = dumpwalkvar(mb, j1, j5);
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
			case j_sort: {
				int l[4][2] = {{j2, 's'}, {j3, 'i'}, {j4, 'd'}, {0, 0}};
				int lw;
				a = dumpwalkvar(mb, j1, j5);
				b = dumprefvar(mb, t->tval2->tval1, a, j1, j6, j7);
				/* can only sort on one type (str, lng, dbl), and can't
				 * combine these, so pick first element's type and
				 * sort all of those */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, newRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushType(mb, q, TYPE_chr);
				q = pushType(mb, q, TYPE_chr);
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, insertRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, c);
				q = pushChr(mb, q, 's');
				q = pushChr(mb, q, 's');
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, insertRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, c);
				q = pushChr(mb, q, 'i');
				q = pushChr(mb, q, 'i');
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, insertRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, c);
				q = pushChr(mb, q, 'd');
				q = pushChr(mb, q, 'd');
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j1);
				q = pushArgument(mb, q, b);
				e = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, joinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, e);
				q = pushArgument(mb, q, c);
				e = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("fetch", 5));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, e);
				q = pushInt(mb, q, 0);
				f = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, uselectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, e);
				q = pushArgument(mb, q, f);
				e = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				g = newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid));
				q = pushReturn(mb, q, g);
				q = pushNil(mb, q, newBatType(TYPE_oid, TYPE_oid));
				pushInstruction(mb, q);
				for (lw = 0; l[lw][0] != 0; lw++) {
					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, calcRef);
					setFunctionId(q, putName("==", 2));
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, f);
					q = pushChr(mb, q, l[lw][1]);
					d = getArg(q, 0);
					pushInstruction(mb, q);
					q = newAssignment(mb);
					q->barrier = BARRIERsymbol;
					pushArgument(mb, q, d);
					c = getArg(q, 0);
					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, algebraRef);
					setFunctionId(q, semijoinRef);
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, l[lw][0]);
					q = pushArgument(mb, q, e);
					d = getArg(q, 0);
					pushInstruction(mb, q);

					/* FIXME: ignore multiple sort args for now */
					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, algebraRef);
					if (t->tval2->nval == 1) {
						setFunctionId(q, sortTailRef);
					} else {
						setFunctionId(q, sortReverseTailRef);
					}
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, d);
					d = getArg(q, 0);
					pushInstruction(mb, q);
					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, batRef);
					setFunctionId(q, mirrorRef);
					q = pushReturn(mb, q, g);
					q = pushArgument(mb, q, d);
					pushInstruction(mb, q);

					q = newAssignment(mb);
					getArg(q, 0) = c;
					q->argc = q->retc = 1;
					q->barrier = EXITsymbol;
				}

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, leftjoinRef); /* need to preserve order of g */
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, g);
				q = pushArgument(mb, q, b);
				g = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, g);
				g = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, mirrorRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, g);
				g = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j5);
				f = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, leftjoinRef); /* need to preserve order of g */
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, g);
				q = pushArgument(mb, q, f);
				g = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, g);
				g = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("sdifference", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j5);
				q = pushArgument(mb, q, g);
				e = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, sunionRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, g);
				q = pushArgument(mb, q, e);
				j5 = getArg(q, 0);
				pushInstruction(mb, q);
			} break;
			case j_top:
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
				setFunctionId(q, sliceRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				q = pushWrd(mb, q, 0);
				q = pushWrd(mb, q, (wrd)(t->nval - 1));
				b = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("sdifference", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				q = pushArgument(mb, q, b);
				b = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("sdifference", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j5);
				q = pushArgument(mb, q, b);
				j5 = getArg(q, 0);
				pushInstruction(mb, q);
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
