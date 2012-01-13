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

static int
dumprefvar(MalBlkPtr mb, tree *t, int elems, int j1, int j6, int j7)
{
	InstrPtr q;
	int a;

	assert(t && t->type == j_var);
	if (t->tval1 == NULL)
		return elems;  /* just var, has no derefs, so all */
	a = elems;
	for (t = t->tval1; t != NULL; t = t->tval1) {
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
		q = pushArgument(mb, q, j1);
		q = pushArgument(mb, q, a);
		a = getArg(q, 0);
		pushInstruction(mb, q);
	}
	return a;
}

static int
dumppred(MalBlkPtr mb, tree *t, int elems, int j1, int j2, int j3, int j4, int j6, int j7)
{
	InstrPtr q;
	int a, b, c, d, e, f, g;
	tree *r;

	assert(t != NULL && t->tval1->type == j_var);
	assert(t->tval2->type == j_comp);
	a = dumprefvar(mb, t->tval1, elems, j1, j6, j7);
	switch (t->tval3->type) {
		case j_var:
			b = dumprefvar(mb, t->tval3, elems, j1, j6, j7);
			c = -1;
			break;
		case j_num:
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, semijoinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, j3);
			q = pushArgument(mb, q, a);
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
			setFunctionId(q, semijoinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, j4);
			q = pushArgument(mb, q, a);
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
			setFunctionId(q, semijoinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, j2);
			q = pushArgument(mb, q, a);
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
			setFunctionId(q, semijoinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, j1);
			q = pushArgument(mb, q, a);
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
				c = getArg(q, 0);
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
				c = getArg(q, 0);
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
				c = getArg(q, 0);
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
				c = getArg(q, 0);
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
				c = getArg(q, 0);
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
				c = getArg(q, 0);
				pushInstruction(mb, q);
				break;
			default:
				assert(0);
		}
	} else {  /* var <cmp> var */
		switch (t->tval2->cval) {
			case j_nequal:
			case j_equals:
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, joinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				q = pushArgument(mb, q, c);
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, c);
				d = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, newRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushType(mb, q, TYPE_oid);
				q = pushType(mb, q, TYPE_oid);
				g = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j2);  /* str */
				q = pushArgument(mb, q, d);
				f = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j2);  /* str */
				q = pushArgument(mb, q, c);
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
				setFunctionId(q, joinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q,joinRef);
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
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j3);  /* int */
				q = pushArgument(mb, q, d);
				f = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j3);  /* int */
				q = pushArgument(mb, q, c);
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
				setFunctionId(q, joinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, f);
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
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j4);  /* dbl */
				q = pushArgument(mb, q, d);
				f = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j4);  /* dbl */
				q = pushArgument(mb, q, c);
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
				setFunctionId(q, joinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, f);
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
				c = g;
				break;
			case j_greater:
				break;
			case j_gequal:
				break;
			case j_less:
				break;
			case j_lequal:
				break;
			default:
				assert(0);
		}
	}
	/* avoid emitting this possibly unused statement */
	if (t->tval1->tval1 != NULL) {
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, reverseRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, j6);
		d = getArg(q, 0);
		pushInstruction(mb, q);
	}
	g = c;
	for (r = t->tval1->tval1; r != NULL; r = r->tval1) {
		/* all object derefs, so match on j6 */
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, semijoinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, d);
		q = pushArgument(mb, q, g);
		f = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, reverseRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, f);
		g = getArg(q, 0);
		pushInstruction(mb, q);
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
			case j_pipe:
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
