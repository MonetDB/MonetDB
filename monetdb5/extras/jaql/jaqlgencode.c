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

typedef struct _json_var {
	const char *name;
	char preserve;
	int j1;
	int j2;
	int j3;
	int j4;
	int j5;
	int j6;
	int j7;
	int ids;
} json_var;

typedef struct _join_result {
	int hbat;
	int tbat;
	json_var *headvar;
	json_var *tailvar;
	struct _join_result *next;
} join_result;

typedef struct _jgvar {
	json_var *var;
	struct _jgvar *next;
	struct _jgvar *prev;
} jgvar;

#define MAXJAQLARG 23

static int dumpvariabletransformation(jc *j, Client cntxt, MalBlkPtr mb, tree *t, int elems, int *j1, int *j2, int *j3, int *j4, int *j5, int *j6, int *j7);
static int dumpnextid(MalBlkPtr mb, int j1);
static int matchfuncsig(jc *j, Client cntxt, tree *t, int *coltpos, enum treetype (*coltypes)[MAXJAQLARG], int (*dynaarg)[MAXJAQLARG][7]);
static void conditionalcall(int *ret, MalBlkPtr mb, tree *t, enum treetype coltypes[MAXJAQLARG], int dynaarg[MAXJAQLARG][7], int coltpos, InstrPtr q);

/* returns a bat with subset from kind bat (:oid,:bte) which are
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

/* dumps the indirection of the array lookup notation [x] or [*],
 * returns a BAT with matching ids in head, and their elems id in tail */
static int
dumparrrefvar(MalBlkPtr mb, tree *t, int elems, int j5)
{
	InstrPtr q;
	int a = 0, b = 0, c = 0, d = 0;

	/* array indirection, entries must be arrays */
	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, algebraRef);
	setFunctionId(q, uselectRef);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, elems);
	q = pushBte(mb, q, 'a');  /* deref requires array */
	a = getArg(q, 0);
	pushInstruction(mb, q);

	if (t->nval == -1 && t->tval1 == NULL) {
		/* all array members (return as a single array) */
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, mirrorRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, a);
		b = getArg(q, 0);
		pushInstruction(mb, q);
	} else if (t->nval == -1 && t->tval1 != NULL) {
		/* all array members of which objects will be dereferenced */
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
		b = getArg(q, 0);
		pushInstruction(mb, q);
	} else {
		/* xth array member */
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
		b = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, markHRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, b);
		q = pushOid(mb, q, 0);
		c = getArg(q, 0);
		pushInstruction(mb, q);

		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, kuniqueRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, a);
		d = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, putName("mark_grp", 8));
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, c);
		q = pushArgument(mb, q, d);
		q = pushOid(mb, q, 0);
		b = getArg(q, 0);
		pushInstruction(mb, q);

		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, markHRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, a);
		q = pushOid(mb, q, 0);
		d = getArg(q, 0);
		pushInstruction(mb, q);

		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, selectRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, b);
		q = pushOid(mb, q, (oid)t->nval);
		q = pushOid(mb, q, (oid)t->nval);
		b = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, semijoinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, c);
		q = pushArgument(mb, q, b);
		c = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, semijoinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, d);
		q = pushArgument(mb, q, b);
		d = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, reverseRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, d);
		d = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, joinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, d);
		q = pushArgument(mb, q, c);
		b = getArg(q, 0);
		pushInstruction(mb, q);
	}

	return b;
}

/* returns a bat with in the head the oid of the values (kind bat), and
 * in the tail, the oid of the corresponding element from elems
 * (typically array bat, head oid 0@0) */
static int
dumprefvar(MalBlkPtr mb, tree *t, int elems, int *j1, int *j5, int *j6, int *j7)
{
	InstrPtr q;
	int a = 0, b = 0, c = 0;
	char encapsulate = 0;

	assert(t && t->type == j_var);

	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, batRef);
	setFunctionId(q, mirrorRef);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, elems);
	b = getArg(q, 0);
	pushInstruction(mb, q);

	/* just var, has no derefs or anything, so all */
	if (t->tval1 == NULL)
		return b;

	a = elems;
	for (t = t->tval1; t != NULL; t = t->tval1) {
		if (t->type == j_arr_idx) {
			c = dumparrrefvar(mb, t, a, *j5);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, joinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, c);
			q = pushArgument(mb, q, b);
			b = getArg(q, 0);
			pushInstruction(mb, q);
			/* re-retrieve kinds now we've updated again */
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, semijoinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, *j1);
			q = pushArgument(mb, q, b);
			a = getArg(q, 0);
			pushInstruction(mb, q);
			if (t->tval1 != NULL && t->nval == -1)
				encapsulate = 1;
		} else {
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, uselectRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, a);
			q = pushBte(mb, q, 'o');  /* deref requires object */
			a = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, semijoinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, *j6);
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
			q = pushArgument(mb, q, *j7);
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
		/* retrieve kinds on multiple indirections */
		if (a != elems && t->tval1 != NULL) {
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, semijoinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, *j1);
			q = pushArgument(mb, q, b);
			a = getArg(q, 0);
			pushInstruction(mb, q);
		}
	}
	if (encapsulate) {
		/* we have to return the results as arrays here, since they are
		 * multi-value (x[*].y) */
		a = dumpnextid(mb, *j1);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, markTRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, elems);
		q = pushArgument(mb, q, a);
		c = getArg(q, 0);
		pushInstruction(mb, q);

		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, joinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, b);
		q = pushArgument(mb, q, c);
		b = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, reverseRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, b);
		b = getArg(q, 0);
		pushInstruction(mb, q);
		/* append to array bat */
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, putName("sunion", 6));
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, *j5);
		q = pushArgument(mb, q, b);
		*j5 = getArg(q, 0);
		pushInstruction(mb, q);

		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, reverseRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, c);
		b = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, projectRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, b);
		q = pushBte(mb, q, 'a');
		c = getArg(q, 0);
		pushInstruction(mb, q);
		/* append to kind bat */
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, putName("kunion", 6));
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, *j1);
		q = pushArgument(mb, q, c);
		*j1 = getArg(q, 0);
		pushInstruction(mb, q);
	}
	return b;
}

/* returns bat with in the head the oids from elems that match the
 * comparison */
static int
dumpcomp(jc *j, Client cntxt, MalBlkPtr mb, tree *t, int elems, int *j1, int *j2, int *j3, int *j4, int *j5, int *j6, int *j7)
{
	InstrPtr q;
	int a = 0, b = 0, c = 0, d = 0, e = 0, f = 0, g = 0;

	assert(t != NULL);
	assert(t->tval1->type == j_var || t->tval1->type == j_operation);
	assert(t->tval2->type == j_comp);
	assert(t->tval3->type == j_var || t->tval3->type == j_operation
			|| t->tval3->type == j_num || t->tval3->type == j_dbl
			|| t->tval3->type == j_str || t->tval3->type == j_bool);

	if (t->tval1->type == j_operation) {
		a = dumpvariabletransformation(j, cntxt, mb, t->tval1, elems,
				j1, j2, j3, j4, j5, j6, j7);
	} else {
		a = dumprefvar(mb, t->tval1, elems, j1, j5, j6, j7);
	}

	if (t->tval3->type != j_var && t->tval3->type != j_operation) {
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
			b = dumprefvar(mb, t->tval3, elems, j1, j5, j6, j7);
			c = -1;
			break;
		case j_operation:
			b = dumpvariabletransformation(j, cntxt, mb, t->tval3, elems,
					j1, j2, j3, j4, j5, j6, j7);
			c = -1;
			break;
		case j_num:
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, joinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, a);
			q = pushArgument(mb, q, *j3);
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
			q = pushArgument(mb, q, *j4);
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
			q = pushArgument(mb, q, *j2);
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
			q = pushArgument(mb, q, *j1);
			b = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, uselectRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, b);
			/* boolean conditions can only be j_equals or j_nequal */
			if (t->tval2->cval == j_equals) {
				q = pushBte(mb, q, t->tval3->nval == 0 ? 'f' : 't');
			} else {
				q = pushBte(mb, q, t->tval3->nval != 0 ? 'f' : 't');
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
		int lv[4] = {*j2, *j3, *j4, 0}, *lp = lv;
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
		q = pushArgument(mb, q, *j1);
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
		q = pushArgument(mb, q, *j1);
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
		q = pushBte(mb, q, 't');
		e = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, selectRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, c);
		q = pushBte(mb, q, 'f');
		f = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, insertRef;);
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
		q = pushBte(mb, q, 't');
		e = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, selectRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, d);
		q = pushBte(mb, q, 'f');
		f = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, insertRef);
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

static char
graphcontainspath(jgvar *jgraph, json_var *l, json_var *r)
{
	int found = 0;

	assert(jgraph->prev == NULL);
	for (; jgraph != NULL; jgraph = jgraph->next) {
		if (jgraph->var == l || jgraph->var == r)
			found++;
		if (found == 2)
			return 1;
	}

	return 0;
}

static jgvar *
calculatejoingraph(join_result *jrs)
{
	jgvar *gh, *gt;
	join_result *jrw;
	/* should be humongous already */
#define JRCANDSSIZ 20
	join_result *jrcands[JRCANDSSIZ];
	int jrcandslen;
	int i, j;

	/* create first bit of the join graph */
	gh = GDKzalloc(sizeof(jgvar));
	gh->var = jrs->headvar;
	gt = gh->next = GDKzalloc(sizeof(jgvar));
	gt->var = jrs->tailvar;
	gt->prev = gh;

	for (jrcandslen = 0, jrw = jrs->next;
			jrw != NULL && jrcandslen < JRCANDSSIZ;
			jrw = jrw->next, jrcandslen++)
	{
		jrcands[jrcandslen] = jrw;
	}

	/* avoid infinite loop on unconnected joins, limit to the number of
	 * possible candidates we have */
	for (j = 0; j < jrcandslen; j++) {
		for (i = 0; i < jrcandslen; i++) {
			jrw = jrcands[i];
			if (jrw == NULL)
				continue;
			if (graphcontainspath(gh, jrw->headvar, jrw->tailvar) == 1) {
				/* do nothing */
			} else if (jrw->headvar == gh->var) {
				gh->prev = GDKzalloc(sizeof(jgvar));
				gh->prev->next = gh;
				gh = gh->prev;
				gh->var = jrw->tailvar;
			} else if (jrw->headvar == gt->var) {
				gt->next = GDKzalloc(sizeof(jgvar));
				gt->next->prev = gt;
				gt = gt->next;
				gt->var = jrw->tailvar;
			} else if (jrw->tailvar == gh->var) {
				gh->prev = GDKzalloc(sizeof(jgvar));
				gh->prev->next = gh;
				gh = gh->prev;
				gh->var = jrw->headvar;
			} else if (jrw->tailvar == gt->var) {
				gt->next = GDKzalloc(sizeof(jgvar));
				gt->next->prev = gt;
				gt = gt->next;
				gt->var = jrw->headvar;
			} else {
				/* couldn't match this join to the graph (yet), so keep it */
				continue;
			}

			/* remove this entry, since we've successfully applied it
			 * somehow */
			jrcands[i] = NULL;
		}
	}

	return gh;
}

static void
dumppredjoin(MalBlkPtr mb, json_var *js, tree *t, int *j1, int *j2, int *j3, int *j4, int *j5, int *j6, int *j7)
{
	InstrPtr q;
	int a = 0, b = 0, c = 0, d = 0, l = 0, r = 0;
	tree *pred;
	json_var *vars, *ljv, *rjv;
	join_result *jrs = NULL, *jrw = NULL, *jrl, *jrr = NULL, *jrn, *jrv, *jrp;

	jgvar *jgraph = NULL;

	/* iterate through all predicates and load the set from the correct
	 * JSON variable */
	for (pred = t->tval2; pred != NULL; pred = pred->next) {
		assert(pred->tval1->type == j_var);
		assert(pred->tval2->cval == j_equals);
		assert(pred->tval3->type == j_var);

#define locate_var(X, Y) \
		X = NULL; \
		for (vars = js; vars->name != NULL; vars++) { \
			if (strcmp(vars->name, Y) == 0) { \
				X = vars; \
				break; \
			} \
		}
		locate_var(ljv, pred->tval1->sval);
		a = dumpwalkvar(mb, ljv->j1, ljv->j5);
		l = dumprefvar(mb, pred->tval1, a, &ljv->j1, &ljv->j5, &ljv->j6, &ljv->j7);

		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, reverseRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, l);
		d = getArg(q, 0);
		pushInstruction(mb, q);

		locate_var(rjv, pred->tval3->sval);
		a = dumpwalkvar(mb, rjv->j1, rjv->j5);
		r = dumprefvar(mb, pred->tval3, a, &rjv->j1, &rjv->j5, &rjv->j6, &rjv->j7);

		/* strings */
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, semijoinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, ljv->j2);
		q = pushArgument(mb, q, l);
		b = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, semijoinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, rjv->j2);
		q = pushArgument(mb, q, r);
		c = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, reverseRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, c);
		c = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, joinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, b);
		q = pushArgument(mb, q, c);
		a = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, joinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, d);
		q = pushArgument(mb, q, a);
		a = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, joinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, a);
		q = pushArgument(mb, q, r);
		a = getArg(q, 0);
		pushInstruction(mb, q);

		/* ints */
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, semijoinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, ljv->j3);
		q = pushArgument(mb, q, l);
		b = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, semijoinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, rjv->j3);
		q = pushArgument(mb, q, r);
		c = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, reverseRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, c);
		c = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, joinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, b);
		q = pushArgument(mb, q, c);
		b = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, joinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, d);
		q = pushArgument(mb, q, b);
		b = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, joinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, b);
		q = pushArgument(mb, q, r);
		b = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, insertRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, a);
		q = pushArgument(mb, q, b);
		a = getArg(q, 0);
		pushInstruction(mb, q);

		/* dbls */
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, semijoinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, ljv->j4);
		q = pushArgument(mb, q, l);
		b = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, semijoinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, rjv->j4);
		q = pushArgument(mb, q, r);
		c = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, reverseRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, c);
		c = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, joinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, b);
		q = pushArgument(mb, q, c);
		b = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, joinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, d);
		q = pushArgument(mb, q, b);
		b = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, joinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, b);
		q = pushArgument(mb, q, r);
		b = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, insertRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, a);
		q = pushArgument(mb, q, b);
		a = getArg(q, 0);
		pushInstruction(mb, q);
		/* a now contains matching oids in head l, in tail r */

		if (ljv->preserve == 1) {
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, mirrorRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, d);
			l = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, putName("outerjoin", 9));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, l);
			q = pushArgument(mb, q, a);
			a = getArg(q, 0);
			pushInstruction(mb, q);
		}
		if (rjv->preserve == 1) {
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, reverseRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, r);
			r = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, mirrorRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, r);
			r = getArg(q, 0);
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
			setFunctionId(q, putName("outerjoin", 9));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, r);
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
		}

		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, reverseRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, a);
		b = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, markHRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, a);
		a = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, markHRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, b);
		b = getArg(q, 0);
		pushInstruction(mb, q);

		if (jrs == NULL) {
			jrw = jrs = GDKzalloc(sizeof(join_result));
		} else {
			jrw = jrw->next = GDKzalloc(sizeof(join_result));
		}
		jrw->hbat = b;
		jrw->tbat = a;
		jrw->headvar = ljv;
		jrw->tailvar = rjv;
	}

	/* intersect conditions that are over the same input sets */
	for (jrw = jrs; jrw != NULL; jrw = jrw->next) {
		for (jrp = jrw, jrv = jrw->next; jrv != NULL; jrp = jrv, jrv = jrv->next) {
			if ((jrw->headvar == jrv->headvar && jrw->tailvar == jrv->tailvar) ||
					(jrw->headvar == jrv->tailvar && jrw->tailvar == jrv->headvar))
			{
				/* join over same inputs, intersect and so eliminate one join */
				if (jrw->headvar != jrv->headvar) {
					l = jrv->hbat;
					jrv->hbat = jrv->tbat;
					jrv->tbat = l;
				}

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, jrw->hbat);
				l = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, jrv->hbat);
				r = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, r);
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
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, jrw->tbat);
				l = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, jrv->tbat);
				r = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, r);
				b = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				b = getArg(q, 0);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kintersect", 10));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				q = pushArgument(mb, q, b);
				a = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, jrw->hbat);
				q = pushArgument(mb, q, a);
				b = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, markHRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				jrw->hbat = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, jrw->tbat);
				q = pushArgument(mb, q, a);
				b = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, markHRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				jrw->tbat = getArg(q, 0);
				pushInstruction(mb, q);

				jrp->next = jrv->next;
				GDKfree(jrv);
				jrv = jrp;
			}
		}
	}

	jgraph = calculatejoingraph(jrs);
	/* FIXME: at this point, there may be joins like a->c, while there
	 * is a->b->c, in which case a->c is a reduction on a and c, having
	 * effect on everything inbetween (only b in this case) */

	/* compute the joins following the joinpath
	 * we need three (connected) vars of the joinpath all the time,
	 * since that equals two join results (and hence some work to do)
	 * FIXME: we don't do joingraphs like a->b->c a->b->d (forks) */
	ljv = jgraph->var;
	rjv = jgraph->next->var;
	jgraph = jgraph->next->next; /* should always be 1 next (a == b) */
	jrl = NULL;
	for (jrp = NULL, jrw = jrs; jrw != NULL; jrp = jrw, jrw = jrw->next) {
		if (jrw->headvar == ljv && jrw->tailvar == rjv) {
			jrl = jrw;
			break;
		} else if (jrw->tailvar == ljv && jrw->headvar == rjv) {
			jrl = jrw;
			/* swap for ease of use */
			ljv = jrw->headvar;
			jrw->headvar = jrw->tailvar;
			jrw->tailvar = ljv;
			l = jrw->hbat;
			jrw->hbat = jrw->tbat;
			jrw->tbat = l;
			break;
		}
	}
	assert(jrl != NULL);
	if (jrp == NULL) {
		jrs = jrl->next;
	} else {
		jrp->next = jrl->next;
	}
	jrl->next = NULL;
	jrn = jrl;
	for (; jgraph != NULL; jgraph = jgraph->next) {
		ljv = jgraph->prev->var;
		rjv = jgraph->var;

		for (jrp = NULL, jrw = jrs; jrw != NULL; jrp = jrw, jrw = jrw->next) {
			if (jrw->headvar == ljv && jrw->tailvar == rjv) {
				jrr = jrw;
				break;
			} else if (jrw->tailvar == ljv && jrw->headvar == rjv) {
				jrr = jrw;
				/* swap for ease of use */
				rjv = jrw->headvar;
				jrw->headvar = jrw->tailvar;
				jrw->tailvar = rjv;
				r = jrw->hbat;
				jrw->hbat = jrw->tbat;
				jrw->tbat = r;
				break;
			}
		}
		assert(jrr != NULL);
		if (jrp == NULL) {
			jrs = jrr->next;
		} else {
			jrp->next = jrr->next;
		}
		jrr->next = jrn;
		jrn = jrr;

		l = jrl->tbat;
		r = jrr->hbat;

		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, reverseRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, r);
		r = getArg(q, 0);
		pushInstruction(mb, q);

		if (jrl->headvar->preserve == 1 && jrr->tailvar->preserve == 1) {
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, putName("outerjoin", 9));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, l);
			q = pushArgument(mb, q, r);
			a = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, reverseRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, l);
			c = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, reverseRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, r);
			d = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, putName("outerjoin", 9));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, d);
			q = pushArgument(mb, q, c);
			c = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, reverseRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, c);
			c = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, putName("kdifference", 11));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, c);
			q = pushArgument(mb, q, a);
			c = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, insertRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, a);
			q = pushArgument(mb, q, c);
			a = getArg(q, 0);
			pushInstruction(mb, q);
		} else if (jrl->headvar->preserve == 1) {
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, putName("outerjoin", 9));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, l);
			q = pushArgument(mb, q, r);
			a = getArg(q, 0);
			pushInstruction(mb, q);
		} else if (jrr->tailvar->preserve == 1) {
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, reverseRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, l);
			c = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, reverseRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, r);
			d = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, putName("outerjoin", 9));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, d);
			q = pushArgument(mb, q, c);
			a = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, reverseRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, a);
			a = getArg(q, 0);
			pushInstruction(mb, q);
		} else {
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, joinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, l);
			q = pushArgument(mb, q, r);
			a = getArg(q, 0);
			pushInstruction(mb, q);
		}

		/* put reduced set back in join result */
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, reverseRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, a);
		d = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, mirrorRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, d);
		d = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, markHRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, d);
		d = getArg(q, 0);
		pushInstruction(mb, q);

		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, joinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, d);
		q = pushArgument(mb, q, jrr->hbat);
		jrr->hbat = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, joinRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, d);
		q = pushArgument(mb, q, jrr->tbat);
		jrr->tbat = getArg(q, 0);
		pushInstruction(mb, q);

		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, mirrorRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, a);
		d = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, markHRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, d);
		d = getArg(q, 0);
		pushInstruction(mb, q);

		/* propagate back */
		for (jrv = jrn->next; jrv != NULL; jrv = jrv->next) {
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, joinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, d);
			q = pushArgument(mb, q, jrv->hbat);
			jrv->hbat = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, joinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, d);
			q = pushArgument(mb, q, jrv->tbat);
			jrv->tbat = getArg(q, 0);
			pushInstruction(mb, q);
		}

		jrl = jrr;
	}

	assert(jrs == NULL); /* should be empty if join graph was complete */
	jrs = jrn;

	/* for each column extract elems */
	q = newInstruction(mb, ASSIGNsymbol);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushOid(mb, q, 0);
	b = getArg(q, 0);
	pushInstruction(mb, q);
	for (vars = js; vars->name != NULL; vars++) {
		for (jrw = jrs; jrw != NULL; jrw = jrw->next) {
			a = -1;
			if (vars == jrw->tailvar) {
				a = jrw->tbat;
			} else if (vars == jrw->headvar) {
				a = jrw->hbat;
			}

			if (a != -1) {
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, putName("json", 4));
				setFunctionId(q, putName("extract", 7));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, vars->j1);
				q = pushArgument(mb, q, vars->j2);
				q = pushArgument(mb, q, vars->j3);
				q = pushArgument(mb, q, vars->j4);
				q = pushArgument(mb, q, vars->j5);
				q = pushArgument(mb, q, vars->j6);
				q = pushArgument(mb, q, vars->j7);
				q = pushArgument(mb, q, a);
				q = pushArgument(mb, q, b);
				vars->j1 = getArg(q, 0);
				vars->j2 = getArg(q, 1);
				vars->j3 = getArg(q, 2);
				vars->j4 = getArg(q, 3);
				vars->j5 = getArg(q, 4);
				vars->j6 = getArg(q, 5);
				vars->j7 = getArg(q, 6);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, vars->j5);
				l = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, selectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, l);
				q = pushArgument(mb, q, b);
				l = getArg(q, 0);
				pushInstruction(mb, q);

				vars->ids = l;

				/* remove outer array */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, deleteRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, vars->j5);
				q = pushArgument(mb, q, b);
				vars->j5 = getArg(q, 0);
				pushInstruction(mb, q);

				b = dumpnextid(mb, vars->j1);

				break;
			}
		}
		assert(a != 1);  /* join input/where check was done before */
	}

	/* create new objects */
	for (vars = js; vars->name != NULL; vars++) {
		/* names of the pairs we create (for each var) */
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, projectRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, vars->ids);
		q = pushStr(mb, q, vars->name);
		l = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, insertRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, vars->j7);
		q = pushArgument(mb, q, l);
		vars->j7 = getArg(q, 0);
		pushInstruction(mb, q);

		/* create object ref holding the pair */
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, algebraRef);
		setFunctionId(q, markTRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, vars->ids);
		q = pushArgument(mb, q, b);
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
		setModuleId(q, batRef);
		setFunctionId(q, insertRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, vars->j6);
		q = pushArgument(mb, q, l);
		vars->j6 = getArg(q, 0);
		pushInstruction(mb, q);
	}

	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, algebraRef);
	setFunctionId(q, markTRef);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, js->ids);
	q = pushArgument(mb, q, b);
	r = getArg(q, 0);
	pushInstruction(mb, q);
	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, batRef);
	setFunctionId(q, reverseRef);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, r);
	r = getArg(q, 0);
	pushInstruction(mb, q);

	/* generate kind entries for the new objects */
	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, algebraRef);
	setFunctionId(q, projectRef);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, r);
	q = pushBte(mb, q, 'o');
	l = getArg(q, 0);
	pushInstruction(mb, q);
	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, batRef);
	setFunctionId(q, insertRef);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, js->j1);
	q = pushArgument(mb, q, l);
	js->j1 = getArg(q, 0);
	pushInstruction(mb, q);

	/* generate outermost array */
	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, algebraRef);
	setFunctionId(q, projectRef);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, r);
	q = pushOid(mb, q, 0);
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
	setModuleId(q, batRef);
	setFunctionId(q, insertRef);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, js->j5);
	q = pushArgument(mb, q, l);
	js->j5 = getArg(q, 0);
	pushInstruction(mb, q);

	/* merge everything into one */
	for (vars = &js[1]; vars->name != NULL; vars++) {
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, insertRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, js->j1);
		q = pushArgument(mb, q, vars->j1);
		*j1 = js->j1 = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, insertRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, js->j2);
		q = pushArgument(mb, q, vars->j2);
		*j2 = js->j2 = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, insertRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, js->j3);
		q = pushArgument(mb, q, vars->j3);
		*j3 = js->j3 = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, insertRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, js->j4);
		q = pushArgument(mb, q, vars->j4);
		*j4 = js->j4 = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, insertRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, js->j5);
		q = pushArgument(mb, q, vars->j5);
		*j5 = js->j5 = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, insertRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, js->j6);
		q = pushArgument(mb, q, vars->j6);
		*j6 = js->j6 = getArg(q, 0);
		pushInstruction(mb, q);
		q = newInstruction(mb, ASSIGNsymbol);
		setModuleId(q, batRef);
		setFunctionId(q, insertRef);
		q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
		q = pushArgument(mb, q, js->j7);
		q = pushArgument(mb, q, vars->j7);
		*j7 = js->j7 = getArg(q, 0);
		pushInstruction(mb, q);
	}
}

static int
dumppred(jc *j, Client cntxt, MalBlkPtr mb, tree *t, int elems, int *j1, int *j2, int *j3, int *j4, int *j5, int *j6, int *j7)
{
	int a, l, r;
	InstrPtr q;

	assert(t != NULL && t->tval2->type == j_comp);

	/* comparisons only take place between tval1 = var and tval3 = val/var
	 * for the rest, only boolean logic is applied */
	if (t->tval2->cval != j_and && t->tval2->cval != j_or)
		return dumpcomp(j, cntxt, mb, t, elems, j1, j2, j3, j4, j5, j6, j7);

	assert(t->tval1->type == j_pred);
	assert(t->tval2->cval == j_and || t->tval2->cval == j_or);
	assert(t->tval3->type == j_pred);

	l = dumppred(j, cntxt, mb, t->tval1, elems, j1, j2, j3, j4, j5, j6, j7);
	r = dumppred(j, cntxt, mb, t->tval3, elems, j1, j2, j3, j4, j5, j6, j7);
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
		setModuleId(q, batRef);
		setFunctionId(q, insertRef);
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
	setModuleId(q, putName("json", 4));
	setFunctionId(q, putName("nextid", 6));
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, j1);
	a = getArg(q, 0);
	pushInstruction(mb, q);

	return a;
}

/* returns a BAT which contains the ids of j1 that refer to the values
 * returned from the variable and its optional calculation applied to it
 * the tail is the element id from the head of elems
 * the j{1..7} variables are updated to point to the updated BATs as
 * insertions of new values (the serialised versions of the variable) */
static int
dumpvariabletransformation(jc *j, Client cntxt, MalBlkPtr mb, tree *t, int elems, int *j1, int *j2, int *j3, int *j4, int *j5, int *j6, int *j7)
{
	InstrPtr q;
	int a = 0, b = 0, c = 0, d = 0, e = 0, f = 0, g = 0, h = 0;

	assert (t != NULL);

	switch (t->type) {
		case j_str:
		case j_num:
		case j_dbl:
			/* fill up elems size of this constant */
			a = dumpnextid(mb, *j1);

			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, markHRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, elems);
			q = pushArgument(mb, q, a);
			c = getArg(q, 0);
			pushInstruction(mb, q);

			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, projectRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, c);
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
			q = pushArgument(mb, q, c);
			if (t->type == j_num) {
				q = pushBte(mb, q, 'i');
				d = *j3;
			} else if (t->type == j_dbl) {
				q = pushBte(mb, q, 'd');
				d = *j4;
			} else {
				q = pushBte(mb, q, 's');
				d = *j2;
			}
			e = getArg(q, 0);
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
			setFunctionId(q, putName("kunion", 6));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, *j1);
			q = pushArgument(mb, q, e);
			*j1 = getArg(q, 0);
			pushInstruction(mb, q);

			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, markTRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, elems);
			q = pushArgument(mb, q, a);
			b = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, reverseRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, b);
			b = getArg(q, 0);
			pushInstruction(mb, q);

			return b;
		case j_var:
			b = dumprefvar(mb, t, elems, j1, j5, j6, j7);

			/* add back missing vars as null */
			a = dumpnextid(mb, *j1);

			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, mirrorRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, elems);
			e = getArg(q, 0);
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
			setFunctionId(q, putName("kdifference", 11));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, e);
			q = pushArgument(mb, q, d);
			c = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, projectRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, c);
			q = pushBte(mb, q, 'n');
			c = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, markHRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, c);
			q = pushArgument(mb, q, a);
			d = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, putName("kunion", 6));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, *j1);
			q = pushArgument(mb, q, d);
			*j1 = getArg(q, 0);
			pushInstruction(mb, q);

			/* create return mapping between original elem and newly
			 * created ids */
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, markTRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, c);
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
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, putName("kunion", 6));
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, b);
			q = pushArgument(mb, q, d);
			b = getArg(q, 0);
			pushInstruction(mb, q);

			/* return in original elem order */
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, reverseRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, b);
			b = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, leftjoinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, e);
			q = pushArgument(mb, q, b);
			b = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, reverseRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, b);
			b = getArg(q, 0);
			pushInstruction(mb, q);

			return b;
		case j_operation: {
			int r, s;
			int u = -1, v;
			int h, i, k = -1, l = -1, m;
			InstrPtr p;
			b = -1;
			switch (t->tval1->type) {
				case j_var:
				case j_operation:
					b = dumpvariabletransformation(j, cntxt, mb, t->tval1, elems,
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
			assert(b != -1);  /* to help the compiler */
			/* d:int and e:dbl are values from val1 */

			switch (t->tval3->type) {
				case j_var:
				case j_operation:
					c = dumpvariabletransformation(j, cntxt, mb, t->tval3, elems,
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
			m = getArg(q, 0);
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
				s = d;
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
				q = pushArgument(mb, q, m);
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
			q = pushArgument(mb, q, m);
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
			q = pushArgument(mb, q, m);
			q = pushBte(mb, q, 'n');
			r = getArg(q, 0);
			pushInstruction(mb, q);
			if (h != -1) {
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, projectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, h);
				q = pushBte(mb, q, 'i');
				h = getArg(q, 0);
				pushInstruction(mb, q);
			}
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, projectRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, i);
			q = pushBte(mb, q, 'd');
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

			/* prepare return, new ids (head) with elem ids (tail) in
			 * original elems order */
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, markHRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, b);
			q = pushArgument(mb, q, a);
			b = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, reverseRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, b);
			b = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, mirrorRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, c);
			c = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, leftjoinRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, b);
			q = pushArgument(mb, q, c);
			a = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, reverseRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, a);
			a = getArg(q, 0);
			pushInstruction(mb, q);

			return a;
		}
		case j_pair:
			if (t->sval == NULL) {
				tree *w;
				assert(t->tval1->type == j_var);
				for (w = t; w->tval1->tval1 != NULL; w = w->tval1)
					;
				assert(w->tval1->sval == NULL);
				GDKfree(w->tval1);
				w->tval1 = NULL;

				/* find all possible array member names */
				a = dumprefvar(mb, t->tval1, elems, j1, j5, j6, j7);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, *j1);
				q = pushArgument(mb, q, a);
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, uselectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, c);
				q = pushBte(mb, q, 'o');
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, *j6);
				q = pushArgument(mb, q, c);
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, c);
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, joinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, c);
				q = pushArgument(mb, q, a);
				a = getArg(q, 0);
				pushInstruction(mb, q);

				return a;
			}

			b = dumpvariabletransformation(j, cntxt, mb, t->tval1, elems,
					j1, j2, j3, j4, j5, j6, j7);
			/* only need to copy if tval1 is a var, otherwise we have a
			 * new copy already */
			if (t->tval1->type == j_var) {
				int *lv[] = {j2, j3, j4, j5, j6, NULL};
				int **lp = lv;

				/* jaql tool seems not to be able to return null in pairs,
				 * instead it shows empty pairs instead of null, but it
				 * however does for arrays, so we filter them out here if
				 * they weren't a real null (jaql tool crashes on that) */
				a = dumprefvar(mb, t->tval1, elems, j1, j5, j6, j7);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				a = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kintersect", 10));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, c);
				q = pushArgument(mb, q, a);
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, c);
				b = getArg(q, 0);
				pushInstruction(mb, q);

				a = dumpnextid(mb, *j1);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, *j1);
				q = pushArgument(mb, q, b);
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, markHRef);
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

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, markHRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				q = pushArgument(mb, q, a);
				b = getArg(q, 0);
				pushInstruction(mb, q);
			}

			/* add pair names */
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

			f = dumpnextid(mb, *j1);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, markHRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, elems);
			q = pushArgument(mb, q, f);
			d = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, projectRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, d);
			q = pushBte(mb, q, g == 2 ? 'o' : 'a');
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

			/* prepare return set with aligned elements */
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, algebraRef);
			setFunctionId(q, markTRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, elems);
			q = pushArgument(mb, q, f);
			d = getArg(q, 0);
			pushInstruction(mb, q);
			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, batRef);
			setFunctionId(q, reverseRef);
			q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
			q = pushArgument(mb, q, d);
			d = getArg(q, 0);
			pushInstruction(mb, q);

			c = -1;
			t = t->tval1;
			while (t != NULL) {
				b = dumpvariabletransformation(j, cntxt, mb, t, elems,
						j1, j2, j3, j4, j5, j6, j7);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				b = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, joinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, d);
				q = pushArgument(mb, q, b);
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

			if (c != -1) {
				/* can have duplicates, see above */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, copyRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, g == 2 ? *j6 : *j5);
				a = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, insertRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				q = pushArgument(mb, q, c);
				if (g == 2) {
					*j6 = getArg(q, 0);
				} else {
					*j5 = getArg(q, 0);
				}
				pushInstruction(mb, q);
			}

			return d;
		case j_func: {
			enum treetype coltypes[MAXJAQLARG];
			int dynaarg[MAXJAQLARG][7];
			int coltpos = 0;
			int funcretc = 0;
			int i;
			tree *w;

			funcretc = matchfuncsig(j, cntxt, t, &coltpos, &coltypes, &dynaarg);
			if (funcretc == 0) {
				/* can't easily return the error because this doesn't
				 * "fall" through back */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, languageRef);
				setFunctionId(q, putName("raise", 5));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushStr(mb, q, j->err);
				pushInstruction(mb, q);
				j->err[0] = '\0';
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, newRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushType(mb, q, TYPE_oid);
				q = pushType(mb, q, TYPE_oid);
				a = getArg(q, 0);
				pushInstruction(mb, q);
				return a;
			}

			b = -1;
			for (i = 0, w = t->tval1; w != NULL; w = w->next, i++) {
				assert(w->type == j_func_arg);
				assert(w->tval1 != NULL);

				switch (w->tval1->type) {
					case j_var:
						b = dumpvariabletransformation(j, cntxt, mb, w->tval1, elems,
								j1, j2, j3, j4, j5, j6, j7);
						switch (coltypes[i]) {
							case j_json:
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, batRef);
								setFunctionId(q, reverseRef);
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, b);
								b = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, putName("json", 4));
								setFunctionId(q, putName("extract", 7));
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, *j1);
								q = pushArgument(mb, q, *j2);
								q = pushArgument(mb, q, *j3);
								q = pushArgument(mb, q, *j4);
								q = pushArgument(mb, q, *j5);
								q = pushArgument(mb, q, *j6);
								q = pushArgument(mb, q, *j7);
								q = pushArgument(mb, q, b);
								q = pushOid(mb, q, (oid)0);
								dynaarg[i][0] = getArg(q, 0);
								dynaarg[i][1] = getArg(q, 1);
								dynaarg[i][2] = getArg(q, 2);
								dynaarg[i][3] = getArg(q, 3);
								dynaarg[i][4] = getArg(q, 4);
								dynaarg[i][5] = getArg(q, 5);
								dynaarg[i][6] = getArg(q, 6);
								pushInstruction(mb, q);
								break;
							case j_func_arg:
							case j_sort_arg: /* bat[:oid,:num] */
								/* check that b consists of arrays */
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, algebraRef);
								setFunctionId(q, semijoinRef);
								q = pushReturn(mb, q,
										newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, *j1);
								q = pushArgument(mb, q, b);
								c = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, algebraRef);
								setFunctionId(q, putName("antiuselect", 11));
								q = pushReturn(mb, q,
										newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, c);
								q = pushBte(mb, q, 'a');
								c = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, aggrRef);
								setFunctionId(q, countRef);
								q = pushReturn(mb, q,
										newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, c);
								c = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, calcRef);
								setFunctionId(q, putName("!=", 2));
								q->barrier = BARRIERsymbol;
								q = pushReturn(mb, q,
										newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, c);
								q = pushWrd(mb, q, 0);
								c = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, languageRef);
								setFunctionId(q, putName("raise", 5));
								q = pushReturn(mb, q,
										newTmpVariable(mb, TYPE_any));
								q = pushStr(mb, q, 
										"function requires array argument");
								pushInstruction(mb, q);
								q = newAssignment(mb);
								getArg(q, 0) = c;
								q->argc = q->retc = 1;
								q->barrier = EXITsymbol;

								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, batRef);
								setFunctionId(q, newRef);
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushType(mb, q, TYPE_oid);
								q = pushType(mb, q, TYPE_bte);
								c = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, batRef);
								setFunctionId(q, insertRef);
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, c);
								q = pushOid(mb, q, (oid)0);
								q = pushBte(mb, q, 'a');
								c = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, algebraRef);
								setFunctionId(q, putName("kdifference", 11));
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, *j1);
								q = pushArgument(mb, q, c);
								a = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, batRef);
								setFunctionId(q, insertRef);
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, c);
								q = pushArgument(mb, q, a);
								a = getArg(q, 0);
								pushInstruction(mb, q);

								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, algebraRef);
								setFunctionId(q, projectRef);
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, b);
								q = pushOid(mb, q, (oid)0);
								h = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, batRef);
								setFunctionId(q, reverseRef);
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, h);
								h = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, algebraRef);
								setFunctionId(q, putName("kdifference", 11));
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, *j5);
								q = pushArgument(mb, q, h);
								c = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, batRef);
								setFunctionId(q, insertRef);
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, h);
								q = pushArgument(mb, q, c);
								h = getArg(q, 0);
								pushInstruction(mb, q);
								/* a = j1, h = j5 */

								if (coltypes[i] == j_sort_arg) {
									q = newInstruction(mb, ASSIGNsymbol);
									setModuleId(q, putName("json", 4));
									setFunctionId(q, putName("unwraptype", 10));
									q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
									q = pushArgument(mb, q, a);
									q = pushArgument(mb, q, *j2);
									q = pushArgument(mb, q, *j3);
									q = pushArgument(mb, q, *j4);
									q = pushArgument(mb, q, h);
									q = pushArgument(mb, q, *j6);
									q = pushArgument(mb, q, *j7);
									q = pushOid(mb, q, (oid)0);
									dynaarg[i][0] = getArg(q, 0);
									pushInstruction(mb, q);
								} else {
									q = newInstruction(mb, ASSIGNsymbol);
									q = pushReturn(mb, q,
											newTmpVariable(mb, TYPE_any));
									q = pushStr(mb, q, "lng");
									dynaarg[i][0] = getArg(q, 0);
									pushInstruction(mb, q);
									dynaarg[i][3] = 0;
								}

								e = -1;
								if (dynaarg[i][1] == 0) {
									c = newBatType(TYPE_oid, TYPE_str);
									q = newInstruction(mb, ASSIGNsymbol);
									q = pushReturn(mb, q,
											newTmpVariable(mb, c));
									q = pushNil(mb, q, c);
									d = getArg(q, 0);
									pushInstruction(mb, q);

									q = newInstruction(mb, ASSIGNsymbol);
									setModuleId(q, calcRef);
									setFunctionId(q, putName("==", 2));
									q->barrier = BARRIERsymbol;
									q = pushReturn(mb, q,
											newTmpVariable(mb, TYPE_any));
									q = pushArgument(mb, q, dynaarg[i][0]);
									q = pushStr(mb, q, "str");
									e = getArg(q, 0);
									pushInstruction(mb, q);
									q = newInstruction(mb, ASSIGNsymbol);
									setModuleId(q, putName("json", 4));
									setFunctionId(q, putName("unwrap", 6));
									q = pushReturn(mb, q, c);
									q = pushArgument(mb, q, a);
									q = pushArgument(mb, q, *j2);
									q = pushArgument(mb, q, *j3);
									q = pushArgument(mb, q, *j4);
									q = pushArgument(mb, q, h);
									q = pushArgument(mb, q, *j6);
									q = pushArgument(mb, q, *j7);
									q = pushOid(mb, q, (oid)0);
									q = pushStr(mb, q, "");
									dynaarg[i][1] = getArg(q, 0);
									pushInstruction(mb, q);
									q = newAssignment(mb);
									getArg(q, 0) = e;
									q->argc = q->retc = 1;
									q->barrier = EXITsymbol;
								}

								f = -1;
								if (dynaarg[i][2] == 0) {
									c = newBatType(TYPE_oid, TYPE_dbl);
									q = newInstruction(mb, ASSIGNsymbol);
									q = pushReturn(mb, q,
											newTmpVariable(mb, c));
									q = pushNil(mb, q, c);
									d = getArg(q, 0);
									pushInstruction(mb, q);

									q = newInstruction(mb, ASSIGNsymbol);
									setModuleId(q, calcRef);
									setFunctionId(q, putName("==", 2));
									q->barrier = BARRIERsymbol;
									q = pushReturn(mb, q,
											newTmpVariable(mb, TYPE_any));
									q = pushArgument(mb, q, dynaarg[i][0]);
									q = pushStr(mb, q, "dbl");
									f = getArg(q, 0);
									pushInstruction(mb, q);
									q = newInstruction(mb, ASSIGNsymbol);
									setModuleId(q, putName("json", 4));
									setFunctionId(q, putName("unwrap", 6));
									q = pushReturn(mb, q, d);
									q = pushArgument(mb, q, a);
									q = pushArgument(mb, q, *j2);
									q = pushArgument(mb, q, *j3);
									q = pushArgument(mb, q, *j4);
									q = pushArgument(mb, q, h);
									q = pushArgument(mb, q, *j6);
									q = pushArgument(mb, q, *j7);
									q = pushOid(mb, q, (oid)0);
									q = pushDbl(mb, q, 0.0);
									dynaarg[i][2] = getArg(q, 0);
									pushInstruction(mb, q);
									q = newAssignment(mb);
									getArg(q, 0) = f;
									q->argc = q->retc = 1;
									q->barrier = EXITsymbol;
								}

								g = -1;
								if (dynaarg[i][3] == 0) {
									c = newBatType(TYPE_oid, TYPE_lng);
									q = newInstruction(mb, ASSIGNsymbol);
									q = pushReturn(mb, q,
											newTmpVariable(mb, c));
									q = pushNil(mb, q, c);
									d = getArg(q, 0);
									pushInstruction(mb, q);

									q = newInstruction(mb, ASSIGNsymbol);
									setModuleId(q, calcRef);
									setFunctionId(q, putName("==", 2));
									q->barrier = BARRIERsymbol;
									q = pushReturn(mb, q,
											newTmpVariable(mb, TYPE_any));
									q = pushArgument(mb, q, dynaarg[i][0]);
									q = pushStr(mb, q, "lng");
									g = getArg(q, 0);
									pushInstruction(mb, q);
									q = newInstruction(mb, ASSIGNsymbol);
									setModuleId(q, putName("json", 4));
									setFunctionId(q, putName("unwrap", 6));
									q = pushReturn(mb, q, d);
									q = pushArgument(mb, q, a);
									q = pushArgument(mb, q, *j2);
									q = pushArgument(mb, q, *j3);
									q = pushArgument(mb, q, *j4);
									q = pushArgument(mb, q, h);
									q = pushArgument(mb, q, *j6);
									q = pushArgument(mb, q, *j7);
									q = pushOid(mb, q, (oid)0);
									q = pushLng(mb, q, 0);
									dynaarg[i][3] = getArg(q, 0);
									pushInstruction(mb, q);
									q = newAssignment(mb);
									getArg(q, 0) = g;
									q->argc = q->retc = 1;
									q->barrier = EXITsymbol;
								}

								if (e >= 0 && f >= 0) {
									q = newInstruction(mb, ASSIGNsymbol);
									setModuleId(q, calcRef);
									setFunctionId(q, putName("or", 2));
									q = pushReturn(mb, q,
											newTmpVariable(mb, TYPE_any));
									q = pushArgument(mb, q, e);
									q = pushArgument(mb, q, f);
									f = getArg(q, 0);
									pushInstruction(mb, q);
								} else if (e >= 0) {
									f = e;
								}
								if (f >= 0 && g >= 0) {
									q = newInstruction(mb, ASSIGNsymbol);
									setModuleId(q, calcRef);
									setFunctionId(q, putName("or", 2));
									q = pushReturn(mb, q,
											newTmpVariable(mb, TYPE_any));
									q = pushArgument(mb, q, f);
									q = pushArgument(mb, q, g);
									g = getArg(q, 0);
									pushInstruction(mb, q);
								} else if (f >= 0) {
									g = f;
								}
								assert(g >= 0);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, calcRef);
								setFunctionId(q, putName("not", 3));
								q->barrier = BARRIERsymbol;
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, g);
								h = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, languageRef);
								setFunctionId(q, putName("raise", 5));
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushStr(mb, q, "function does not support maximum "
										"type from JSON array");
								pushInstruction(mb, q);
								q = newAssignment(mb);
								getArg(q, 0) = h;
								q->argc = q->retc = 1;
								q->barrier = EXITsymbol;

								if (coltypes[i] == j_func_arg)
									dynaarg[i][0] = dynaarg[i][3];

								break;
							case j_bool:
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, algebraRef);
								setFunctionId(q, semijoinRef);
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, *j1);
								q = pushArgument(mb, q, b);
								b = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, algebraRef);
								setFunctionId(q, selectRef);
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, b);
								q = pushBte(mb, q, 't');
								c = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, algebraRef);
								setFunctionId(q, selectRef);
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, b);
								q = pushBte(mb, q, 'f');
								d = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, batRef);
								setFunctionId(q, insertRef);
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, c);
								q = pushArgument(mb, q, d);
								c = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, algebraRef);
								setFunctionId(q, putName("kdifference", 11));
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, b);
								q = pushArgument(mb, q, c);
								d = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, algebraRef);
								setFunctionId(q, projectRef);
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, d);
								q = pushBte(mb, q, 'n');
								d = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, batRef);
								setFunctionId(q, insertRef);
								q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, c);
								q = pushArgument(mb, q, d);
								c = getArg(q, 0);
								pushInstruction(mb, q);
								dynaarg[i][0] = c;
								break;
							default:
								assert(0);
						}
						break;
					case j_str:
					case j_num:
					case j_dbl:
					case j_bool:
						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, batRef);
						setFunctionId(q, newRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushType(mb, q, TYPE_oid);
						if (coltypes[i] == j_str) {
							q = pushType(mb, q, TYPE_str);
						} else if (coltypes[i] == j_num) {
							q = pushType(mb, q, TYPE_lng);
						} else if (coltypes[i] == j_dbl) {
							q = pushType(mb, q, TYPE_dbl);
						} else /* j_bool */ {
							q = pushType(mb, q, TYPE_bit);
						}
						a = getArg(q, 0);
						pushInstruction(mb, q);
						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, batRef);
						setFunctionId(q, insertRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, a);
						q = pushOid(mb, q, (oid)0);
						if (coltypes[i] == j_str) {
							q = pushStr(mb, q, w->tval1->sval);
						} else if (coltypes[i] == j_num) {
							q = pushLng(mb, q, w->tval1->nval);
						} else if (coltypes[i] == j_dbl) {
							q = pushDbl(mb, q, w->tval1->dval);
						} else /* j_bool */ {
							q = pushBit(mb, q, w->tval1->nval == 1);
						}
						a = getArg(q, 0);
						pushInstruction(mb, q);
						dynaarg[i][0] = a;
						break;
					default:
						snprintf(j->err, sizeof(j->err),
								"unhandled argument type (1)");
						return -1;
				}
			}

			q = newInstruction(mb, ASSIGNsymbol);
			setModuleId(q, putName("jaqlfunc", 8));
			setFunctionId(q, putName(t->sval, strlen(t->sval)));
			if (funcretc == 7) {
				int a1, a2, a3, a4, a5, a6, a7;
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				/* only single BAT returning functions can be dynatyped */
				for (i = 0; i < coltpos; i++) {
					switch (coltypes[i]) {
						case j_var:
						case j_json:
						case j_json_arr:
							q = pushArgument(mb, q, dynaarg[i][0]);
							q = pushArgument(mb, q, dynaarg[i][1]);
							q = pushArgument(mb, q, dynaarg[i][2]);
							q = pushArgument(mb, q, dynaarg[i][3]);
							q = pushArgument(mb, q, dynaarg[i][4]);
							q = pushArgument(mb, q, dynaarg[i][5]);
							q = pushArgument(mb, q, dynaarg[i][6]);
							break;
						case j_sort_arg:
							assert(0);
						default:
							q = pushArgument(mb, q, dynaarg[i][0]);
							break;
					}
				}
				a1 = getArg(q, 0);
				a2 = getArg(q, 1);
				a3 = getArg(q, 2);
				a4 = getArg(q, 3);
				a5 = getArg(q, 4);
				a6 = getArg(q, 5);
				a7 = getArg(q, 6);
				pushInstruction(mb, q);

				a = dumpwalkvar(mb, a1, a5);
				b = dumpnextid(mb, *j1);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				a = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, putName("json", 4));
				setFunctionId(q, putName("extract", 7));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a1);
				q = pushArgument(mb, q, a2);
				q = pushArgument(mb, q, a3);
				q = pushArgument(mb, q, a4);
				q = pushArgument(mb, q, a5);
				q = pushArgument(mb, q, a6);
				q = pushArgument(mb, q, a7);
				q = pushArgument(mb, q, a);
				q = pushArgument(mb, q, b);
				a1 = getArg(q, 0);
				a2 = getArg(q, 1);
				a3 = getArg(q, 2);
				a4 = getArg(q, 3);
				a5 = getArg(q, 4);
				a6 = getArg(q, 5);
				a7 = getArg(q, 6);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, kunionRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, *j1);
				q = pushArgument(mb, q, a1);
				*j1 = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, kunionRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, *j2);
				q = pushArgument(mb, q, a2);
				*j2 = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, kunionRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, *j3);
				q = pushArgument(mb, q, a3);
				*j3 = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, kunionRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, *j4);
				q = pushArgument(mb, q, a4);
				*j4 = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, copyRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, *j5);
				*j5 = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, insertRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, *j5);
				q = pushArgument(mb, q, a5);
				*j5 = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, copyRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, *j6);
				*j6 = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, insertRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, *j6);
				q = pushArgument(mb, q, a6);
				*j6 = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, kunionRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, *j7);
				q = pushArgument(mb, q, a7);
				*j7 = getArg(q, 0);

				/* return bat has b in head, 0 in tail (this is an
				 * aggregate (can it be anything else?) */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, newRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushType(mb, q, TYPE_oid);
				q = pushType(mb, q, TYPE_oid);
				a = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, insertRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				q = pushOid(mb, q, b);
				q = pushOid(mb, q, (oid)0);
				a = getArg(q, 0);
				pushInstruction(mb, q);

				return a;
			} else {
				InstrPtr r;
				r = newInstruction(mb, ASSIGNsymbol);
				a = newTmpVariable(mb, newBatType(TYPE_oid, TYPE_any));
				r = pushReturn(mb, r, a);
				r = pushNil(mb, r, newBatType(TYPE_oid, TYPE_any));
				pushInstruction(mb, r);

				conditionalcall(&a, mb, t, coltypes, dynaarg, coltpos, q);

				h = dumpnextid(mb, *j1);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, markHRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				q = pushArgument(mb, q, h);
				c = getArg(q, 0);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, putName("getTailType", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, c);
				d = getArg(q, 0);
				pushInstruction(mb, q);

				e = newBatType(TYPE_oid, TYPE_bte);
				q = newInstruction(mb, ASSIGNsymbol);
				q = pushReturn(mb, q, newTmpVariable(mb, e));
				q = pushNil(mb, q, e);
				e = getArg(q, 0);
				pushInstruction(mb, q);

				f = newBatType(TYPE_oid, TYPE_lng);
				q = newInstruction(mb, ASSIGNsymbol);
				q = pushReturn(mb, q, newTmpVariable(mb, f));
				q = pushNil(mb, q, f);
				f = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, calcRef);
				setFunctionId(q, putName("==", 2));
				q->barrier = BARRIERsymbol;
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, d);
				q = pushStr(mb, q, "lng");
				g = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, putName("jaql", 4));
				setFunctionId(q, putName("cast", 4));
				q = pushReturn(mb, q, f);
				q = pushArgument(mb, q, c);
				q = pushType(mb, q, TYPE_lng);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, projectRef);
				q = pushReturn(mb, q, e);
				q = pushArgument(mb, q, f);
				q = pushBte(mb, q, 'i');
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, kunionRef);
				q = pushReturn(mb, q, *j3);
				q = pushArgument(mb, q, *j3);
				q = pushArgument(mb, q, f);
				pushInstruction(mb, q);
				q = newAssignment(mb);
				getArg(q, 0) = g;
				q->argc = q->retc = 1;
				q->barrier = EXITsymbol;

				f = newBatType(TYPE_oid, TYPE_dbl);
				q = newInstruction(mb, ASSIGNsymbol);
				q = pushReturn(mb, q, newTmpVariable(mb, f));
				q = pushNil(mb, q, f);
				f = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, calcRef);
				setFunctionId(q, putName("==", 2));
				q->barrier = BARRIERsymbol;
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, d);
				q = pushStr(mb, q, "dbl");
				g = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, putName("jaql", 4));
				setFunctionId(q, putName("cast", 4));
				q = pushReturn(mb, q, f);
				q = pushArgument(mb, q, c);
				q = pushType(mb, q, TYPE_dbl);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, projectRef);
				q = pushReturn(mb, q, e);
				q = pushArgument(mb, q, f);
				q = pushBte(mb, q, 'd');
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, kunionRef);
				q = pushReturn(mb, q, *j4);
				q = pushArgument(mb, q, *j4);
				q = pushArgument(mb, q, f);
				pushInstruction(mb, q);
				q = newAssignment(mb);
				getArg(q, 0) = g;
				q->argc = q->retc = 1;
				q->barrier = EXITsymbol;

				f = newBatType(TYPE_oid, TYPE_str);
				q = newInstruction(mb, ASSIGNsymbol);
				q = pushReturn(mb, q, newTmpVariable(mb, f));
				q = pushNil(mb, q, f);
				f = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, calcRef);
				setFunctionId(q, putName("==", 2));
				q->barrier = BARRIERsymbol;
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, d);
				q = pushStr(mb, q, "str");
				g = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, putName("jaql", 4));
				setFunctionId(q, putName("cast", 4));
				q = pushReturn(mb, q, f);
				q = pushArgument(mb, q, c);
				q = pushType(mb, q, TYPE_str);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, projectRef);
				q = pushReturn(mb, q, e);
				q = pushArgument(mb, q, f);
				q = pushBte(mb, q, 's');
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, kunionRef);
				q = pushReturn(mb, q, *j2);
				q = pushArgument(mb, q, *j2);
				q = pushArgument(mb, q, f);
				pushInstruction(mb, q);
				q = newAssignment(mb);
				getArg(q, 0) = g;
				q->argc = q->retc = 1;
				q->barrier = EXITsymbol;

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, kunionRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, *j1);
				q = pushArgument(mb, q, e);
				*j1 = getArg(q, 0);
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
				setFunctionId(q, markHRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				q = pushArgument(mb, q, h);
				a = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, joinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				q = pushArgument(mb, q, b);
				a = getArg(q, 0);
				pushInstruction(mb, q);

				return a;
			}
		}
		default:
			assert(0);
			return -1;
	}
}

static void
dumpgetvar(MalBlkPtr mb, const char *v, int *j1, int *j2, int *j3, int *j4, int *j5, int *j6, int *j7)
{
	InstrPtr q;

	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, putName("jaql", 4));
	setFunctionId(q, putName("getVar", 6));
	q = pushReturn(mb, q,
			newTmpVariable(mb, newBatType(TYPE_oid, TYPE_bte)));
	*j1 = getArg(q, 0);
	setVarUDFtype(mb, *j1);
	q = pushReturn(mb, q,
			newTmpVariable(mb, newBatType(TYPE_oid, TYPE_str)));
	*j2 = getArg(q, 1);
	setVarUDFtype(mb, *j2);
	q = pushReturn(mb, q,
			newTmpVariable(mb, newBatType(TYPE_oid, TYPE_lng)));
	*j3 = getArg(q, 2);
	setVarUDFtype(mb, *j3);
	q = pushReturn(mb, q,
			newTmpVariable(mb, newBatType(TYPE_oid, TYPE_dbl)));
	*j4 = getArg(q, 3);
	setVarUDFtype(mb, *j4);
	q = pushReturn(mb, q,
			newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid)));
	*j5 = getArg(q, 4);
	setVarUDFtype(mb, *j5);
	q = pushReturn(mb, q,
			newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid)));
	*j6 = getArg(q, 5);
	setVarUDFtype(mb, *j6);
	q = pushReturn(mb, q,
			newTmpVariable(mb, newBatType(TYPE_oid, TYPE_str)));
	*j7 = getArg(q, 6);
	setVarUDFtype(mb, *j7);

	q = pushStr(mb, q, v);
	pushInstruction(mb, q);
}

static json_var *
bindjsonvars(MalBlkPtr mb, tree *t)
{
	int i;
	tree *w;
	json_var *res;

	assert(t != NULL && t->type == j_join_input);

	for (w = t, i = 0; w != NULL; w = w->next, i++)
		;
	
	res = GDKmalloc(sizeof(json_var) * (i + 1));
	for (w = t, i = 0; w != NULL; w = w->next, i++) {
		res[i].name = w->tval2->sval; /* always _IDENT */
		res[i].preserve = w->nval;
		dumpgetvar(mb, w->tval1->sval,
				&res[i].j1, &res[i].j2, &res[i].j3, &res[i].j4,
				&res[i].j5, &res[i].j6, &res[i].j7);
	}
	res[i].name = NULL;

	return res;
}

static void
changetmplrefsjoin(tree *t, char *except)
{
	tree *w;

	for (w = t; w != NULL; w = w->next) {
		if (w->type == j_var && (except == NULL || strcmp(w->sval, except) != 0))
		{
			/* inject an indirection to match the join output */
			tree *n = GDKzalloc(sizeof(tree));
			n->type = j_var;
			n->tval1 = w->tval1;
			w->tval1 = n;
			n->sval = w->sval;
			w->sval = GDKstrdup("$");
			if (except != NULL) {
				if (w->tval1->tval1 == NULL || w->tval1->tval1->type != j_arr_idx) {
					/* tval1->tval1->type should never be a j_var, but
					 * for the sake of the else case, just put [*] in
					 * front of it here ... it can't match anything
					 * since the base var here is defined as an array */
					n = GDKzalloc(sizeof(tree));
					n->type = j_arr_idx;
					n->nval = -1;
					n->tval1 = w->tval1;
					w->tval1 = n;
				} else {
					assert(w->tval1->tval1->type == j_arr_idx);
					n = w->tval1->tval1;
					w->tval1->tval1 = n->tval1;
					n->tval1 = w->tval1;
					w->tval1 = n;
				}
			}
			continue;
		}
		if (w->tval1 != NULL)
			changetmplrefsjoin(w->tval1, except);
		if (w->tval2 != NULL)
			changetmplrefsjoin(w->tval2, except);
		if (w->tval3 != NULL)
			changetmplrefsjoin(w->tval3, except);
	}
}

static void
changetmplrefsgroup(tree *t, char *groupkeyvar, tree *groupexpr)
{
	tree *w;

	for (w = t; w != NULL; w = w->next) {
		if (w->type == j_var && strcmp(w->sval, groupkeyvar) == 0) {
			/* inject [0].groupexpr indirection to match the group output */
			tree *l, *m, *n = GDKzalloc(sizeof(tree));
			l = n;
			n->type = j_arr_idx;
			n->nval = 0;
			for (m = groupexpr->tval1; m != NULL; m = m->tval1) {
				n = n->tval1 = GDKzalloc(sizeof(tree));
				n->type = m->type;
				n->sval = m->sval != NULL ? GDKstrdup(m->sval) : NULL;
				n->nval = m->nval;
			}
			n->tval1 = w->tval1;
			w->tval1 = l;
			continue;
		}
		if (w->tval1 != NULL)
			changetmplrefsgroup(w->tval1, groupkeyvar, groupexpr);
		if (w->tval2 != NULL)
			changetmplrefsgroup(w->tval2, groupkeyvar, groupexpr);
		if (w->tval3 != NULL)
			changetmplrefsgroup(w->tval3, groupkeyvar, groupexpr);
	}
}

static int
dumpvalsfromarr(MalBlkPtr mb, enum treetype tpe,
		int j1, int j2, int j3, int j4, int j5)
{
	InstrPtr q;
	int a;

	a = dumpwalkvar(mb, j1, j5);

	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, algebraRef);
	setFunctionId(q, selectRef);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushArgument(mb, q, a);
	switch (tpe) {
		case j_str:
			q = pushBte(mb, q, 's');
			break;
		case j_num:
			q = pushBte(mb, q, 'i');
			break;
		case j_dbl:
			q = pushBte(mb, q, 'd');
			break;
		default:
			assert(0);
	}
	a = getArg(q, 0);
	pushInstruction(mb, q);

	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, algebraRef);
	setFunctionId(q, semijoinRef);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	switch (tpe) {
		case j_str:
			q = pushArgument(mb, q, j2);
			break;
		case j_num:
			q = pushArgument(mb, q, j3);
			break;
		case j_dbl:
			q = pushArgument(mb, q, j4);
			break;
		default:
			assert(0);
	}
	q = pushArgument(mb, q, a);
	a = getArg(q, 0);
	pushInstruction(mb, q);

	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, algebraRef);
	setFunctionId(q, projectRef);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
	q = pushOid(mb, q, 0);
	q = pushArgument(mb, q, a);
	a = getArg(q, 0);
	pushInstruction(mb, q);
	return a;
}

static void
dumpjsonshred(MalBlkPtr mb, char *json,
		int *j1, int *j2, int *j3, int *j4, int *j5, int *j6, int *j7)
{
	InstrPtr q;

	q = newInstruction(mb, ASSIGNsymbol);
	setModuleId(q, putName("json", 4));
	setFunctionId(q, putName("shred", 5));
	q = pushReturn(mb, q,
			newTmpVariable(mb, newBatType(TYPE_oid, TYPE_bte)));
	*j1 = getArg(q, 0);
	setVarUDFtype(mb, *j1);
	q = pushReturn(mb, q,
			newTmpVariable(mb, newBatType(TYPE_oid, TYPE_str)));
	*j2 = getArg(q, 1);
	setVarUDFtype(mb, *j2);
	q = pushReturn(mb, q,
			newTmpVariable(mb, newBatType(TYPE_oid, TYPE_lng)));
	*j3 = getArg(q, 2);
	setVarUDFtype(mb, *j3);
	q = pushReturn(mb, q,
			newTmpVariable(mb, newBatType(TYPE_oid, TYPE_dbl)));
	*j4 = getArg(q, 3);
	setVarUDFtype(mb, *j4);
	q = pushReturn(mb, q,
			newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid)));
	*j5 = getArg(q, 4);
	setVarUDFtype(mb, *j5);
	q = pushReturn(mb, q,
			newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid)));
	*j6 = getArg(q, 5);
	setVarUDFtype(mb, *j6);
	q = pushReturn(mb, q,
			newTmpVariable(mb, newBatType(TYPE_oid, TYPE_str)));
	*j7 = getArg(q, 6);
	setVarUDFtype(mb, *j7);

	q = pushStr(mb, q, json);
	pushInstruction(mb, q);
}

static int
matchfuncsig(jc *j, Client cntxt, tree *t, int *coltpos, enum treetype (*coltypes)[MAXJAQLARG], int (*dynaarg)[MAXJAQLARG][7])
{
	Symbol s;
	InstrPtr f;
	tree *w;
	int i, funcretc = 0;

	/* lookup the function we need */
	s = findSymbol(cntxt->nspace,
			putName("jaqlfunc", 8),
			putName(t->sval, strlen(t->sval)));
	if (s == NULL) {
		snprintf(j->err, sizeof(j->err), "no such function: %s",
				t->sval);
		return 0;
	}

	/* check what arguments were provided */
	for (w = t->tval1; w != NULL; w = w->next) {
		assert(w->type == j_func_arg);
		assert(w->tval1 != NULL);

		if (*coltpos >= MAXJAQLARG) {
			snprintf(j->err, sizeof(j->err), "too many arguments");
			return 0;
		}

		switch (w->tval1->type) {
			case j_str:
			case j_num:
			case j_dbl:
			case j_bool:
			case j_var:
				(*coltypes)[*coltpos] = w->tval1->type;
				break;
			case j_json:
				if (*w->tval1->sval == '[') {
					(*coltypes)[*coltpos] = j_json;
				} else { /* must be '{' */
					(*coltypes)[*coltpos] = j_str;
				}
				break;
			default:
				snprintf(j->err, sizeof(j->err),
						"unhandled function argument type (1)");
				return 0;
		}

		(*coltpos)++;
	}

	for (i = 0; i < *coltpos; i++) {
		/* initialise as "not in use" */
		(*dynaarg)[i][1] = -1;
		(*dynaarg)[i][2] = -1;
		(*dynaarg)[i][3] = -1;
	}

	do {
		if (idcmp(s->name, t->sval) == 0) {
			char match = 0;
			int itype;
			int argoff = 0;
			int orgoff, odyn1 = -1, odyn2 = -1, odyn3 = -1;
			enum treetype ocoltype = j_invalid;

			/* Function resolution is done based on the
			 * input arguments only; we cannot consider the
			 * return type, because we don't know what is
			 * expected from us.  We go by the assumption
			 * here that functions are constructed in such a
			 * way that their return value is not considered
			 * while looking for their uniqueness (like in
			 * Java). */
			f = getSignature(s);
			for (i = 0; i < *coltpos; i++) {
				match = 0;
				orgoff = argoff;
				odyn1 = (*dynaarg)[i][1];
				odyn2 = (*dynaarg)[i][2];
				odyn3 = (*dynaarg)[i][3];
				ocoltype = (*coltypes)[i];

				if (f->argc - f->retc - argoff < 1)
					break;
				itype = getArgType(s->def, f, f->retc + argoff);
				if (!isaBatType(itype) ||
						getHeadType(itype) != TYPE_oid)
					break;

				switch ((*coltypes)[i]) {
					case j_var:
					case j_json:
					case j_json_arr:
					case j_sort_arg:
					case j_func_arg:
						/* out of laziness, we only check
						 * the first argument to be a BAT,
						 * and of the right type */
						if (f->argc - f->retc - argoff >= 7 &&
								getTailType(itype) == TYPE_bte)
						{
							match = 1;
							argoff += 7;
							break;
						}

						switch (getTailType(itype)) {
							case TYPE_str:
								(*dynaarg)[i][1] = 0;
								(*coltypes)[i] = j_sort_arg;
								match = 1;
								argoff += 1;
								break;
							case TYPE_lng:
								(*dynaarg)[i][3] = 0;
								(*coltypes)[i] = j_sort_arg;
								match = 1;
								argoff += 1;
								break;
							case TYPE_dbl:
								(*dynaarg)[i][2] = 0;
								(*coltypes)[i] = j_sort_arg;
								match = 1;
								argoff += 1;
								break;
							case TYPE_bit:
								(*coltypes[i]) = j_bool;
								match = 1;
								argoff += 1;
								break;
							case TYPE_any:
								(*coltypes[i]) = j_func_arg;
								match = 1;
								argoff += 1;
								break;
								/* other types just don't match */
						}
						break;
					case j_str:
						if (getTailType(itype) == TYPE_str)
							match = 1;
						argoff += 1;
						break;
					case j_num:
						if (getTailType(itype) == TYPE_lng)
							match = 1;
						argoff += 1;
						break;
					case j_dbl:
						if (getTailType(itype) == TYPE_dbl)
							match = 1;
						argoff += 1;
						break;
					case j_bool:
						if (getTailType(itype) == TYPE_bit)
							match = 1;
						argoff += 1;
						break;
					default:
						assert(0);
				}
				if (match == 0)
					break;
			}
			if (match != 1 || f->argc - f->retc - argoff != 0 ||
					(f->retc != 7 && f->retc != 1) ||
					!isaBatType(getArgType(s->def, f, 0)))
			{
				argoff = orgoff;
				(*dynaarg)[i][1] = odyn1;
				(*dynaarg)[i][2] = odyn2;
				(*dynaarg)[i][3] = odyn3;
				(*coltypes)[i] = ocoltype;
			} else {
				funcretc = f->retc;
			}
		}
		s = s->peer;
	} while (s != NULL);
	if (funcretc == 0) {
		char argbuf[256];
		int pos = 0;
		for (i = 0; i < *coltpos; i++) {
			if (i > 0)
				pos += snprintf(argbuf + pos,
						sizeof(argbuf) - pos, ", ");
			switch ((*coltypes)[i]) {
				case j_json:
				case j_json_arr:
				case j_func_arg:
				case j_sort_arg:
					pos += snprintf(argbuf + pos,
							sizeof(argbuf) - pos, "json");
					break;
				case j_num:
					pos += snprintf(argbuf + pos,
							sizeof(argbuf) - pos, "num");
					break;
				case j_dbl:
					pos += snprintf(argbuf + pos,
							sizeof(argbuf) - pos, "dbl");
					break;
				case j_str:
					pos += snprintf(argbuf + pos,
							sizeof(argbuf) - pos, "str");
					break;
				case j_bool:
					pos += snprintf(argbuf + pos,
							sizeof(argbuf) - pos, "bool");
					break;
				default:
					pos += snprintf(argbuf + pos,
							sizeof(argbuf) - pos, "unknown");
					break;
			}
		}
		snprintf(j->err, sizeof(j->err), "no such function "
				"with matching signature for: %s(%s)",
				t->sval, argbuf);
		return 0;
	}

	return funcretc;
}

static void
conditionalcall(int *ret, MalBlkPtr mb, tree *t,
		enum treetype coltypes[MAXJAQLARG],
		int dynaarg[MAXJAQLARG][7], int coltpos, InstrPtr q)
{
	InstrPtr r;
	int a = 0, b = 0, i = 0;
	for (i = 0; i < coltpos; i++) {
		switch (coltypes[i]) {
			case j_json:
			case j_json_arr:
				q = pushArgument(mb, q, dynaarg[i][0]);
				q = pushArgument(mb, q, dynaarg[i][1]);
				q = pushArgument(mb, q, dynaarg[i][2]);
				q = pushArgument(mb, q, dynaarg[i][3]);
				q = pushArgument(mb, q, dynaarg[i][4]);
				q = pushArgument(mb, q, dynaarg[i][5]);
				q = pushArgument(mb, q, dynaarg[i][6]);
				break;
			case j_sort_arg:
				a = dynaarg[i][0];
				if (dynaarg[i][1] > 0) {
					r = newInstruction(mb, ASSIGNsymbol);
					setModuleId(r, calcRef);
					setFunctionId(r, putName("==", 2));
					r->barrier = BARRIERsymbol;
					r = pushReturn(mb, r, newTmpVariable(mb, TYPE_any));
					r = pushArgument(mb, r, a);
					r = pushStr(mb, r, "str");
					b = getArg(r, 0);
					pushInstruction(mb, r);

					r = copyInstruction(q);
					r = pushArgument(mb, r, dynaarg[i][1]);

					conditionalcall(ret,
							mb, t, &coltypes[i + 1], &dynaarg[i + 1],
							coltpos - (i + 1), r);

					r = newAssignment(mb);
					getArg(r, 0) = b;
					r->argc = r->retc = 1;
					r->barrier = EXITsymbol;
				}

				if (dynaarg[i][2] > 0) {
					r = newInstruction(mb, ASSIGNsymbol);
					setModuleId(r, calcRef);
					setFunctionId(r, putName("==", 2));
					r->barrier = BARRIERsymbol;
					r = pushReturn(mb, r, newTmpVariable(mb, TYPE_any));
					r = pushArgument(mb, r, a);
					r = pushStr(mb, r, "dbl");
					b = getArg(r, 0);
					pushInstruction(mb, r);

					r = copyInstruction(q);
					r = pushArgument(mb, r, dynaarg[i][2]);

					conditionalcall(ret,
							mb, t, &coltypes[i + 1], &dynaarg[i + 1],
							coltpos - (i + 1), r);

					r = newAssignment(mb);
					getArg(r, 0) = b;
					r->argc = r->retc = 1;
					r->barrier = EXITsymbol;
				}

				if (dynaarg[i][3] > 0) {
					r = newInstruction(mb, ASSIGNsymbol);
					setModuleId(r, calcRef);
					setFunctionId(r, putName("==", 2));
					r->barrier = BARRIERsymbol;
					r = pushReturn(mb, r, newTmpVariable(mb, TYPE_any));
					r = pushArgument(mb, r, a);
					r = pushStr(mb, r, "lng");
					b = getArg(r, 0);
					pushInstruction(mb, r);

					r = copyInstruction(q);
					r = pushArgument(mb, r, dynaarg[i][3]);

					conditionalcall(ret,
							mb, t, &coltypes[i + 1], &dynaarg[i + 1],
							coltpos - (i + 1), r);

					r = newAssignment(mb);
					getArg(r, 0) = b;
					r->argc = r->retc = 1;
					r->barrier = EXITsymbol;
				}
				return;
			default:
				q = pushArgument(mb, q, dynaarg[i][0]);
				break;
		}
	}

	q = pushReturn(mb, q, *ret);
	pushInstruction(mb, q);
}


int
dumptree(jc *j, Client cntxt, MalBlkPtr mb, tree *t)
{
	InstrPtr q;
	int j1 = 0, j2 = 0, j3 = 0, j4 = 0, j5 = 0, j6 = 0, j7 = 0;
	int ro1 = 0, ro2 = 0, ro3 = 0, ro4 = 0, ro5 = 0, ro6 = 0, ro7 = 0;
	int a = 0, b = 0, c = 0, d = 0, e = 0, f = 0, g = 0;

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
				if (j->explain & 64) {
					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, putName("jaql", 4));
					setFunctionId(q, putName("exportResult", 12));
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
				} else {
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
				}
				break;
			case j_json:
				dumpjsonshred(mb, t->sval, &j1, &j2, &j3, &j4, &j5, &j6, &j7);
				break;
			case j_var: {
				int *bats[8] = {&j1, &j2, &j3, &j4, &j5, &j6, &j7, NULL};
				int **bat;
				/* j_var at top level is always _IDENT */
				dumpgetvar(mb, t->sval, &j1, &j2, &j3, &j4, &j5, &j6, &j7);
				/* vars are the only read-only BATs we have */
				ro1 = ro2 = ro3 = ro4 = ro5 = ro6 = ro7 = 1;
				/* force this in the MAL environment as well as
				 * assertion check */
				for (bat = bats; *bat != NULL; bat++) {
					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, batRef);
					setFunctionId(q, putName("setAccess", 9));
					q = pushReturn(mb, q, **bat);
					q = pushArgument(mb, q, **bat);
					q = pushStr(mb, q, "r");
					pushInstruction(mb, q);
				}
			} break;
			case j_filter:
				a = dumpwalkvar(mb, j1, j5);
				b = dumppred(j, cntxt, mb, t->tval2, a,
						&j1, &j2, &j3, &j4, &j5, &j6, &j7);
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
				b = dumpvariabletransformation(j, cntxt, mb, t->tval2, a,
						&j1, &j2, &j3, &j4, &j5, &j6, &j7);

				/* remove old array entries */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, projectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				q = pushOid(mb, q, 0);
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
				j5 = getArg(q, 0);
				pushInstruction(mb, q);

				/* construct new array members, respecting the old
				 * element ids order */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				d = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, joinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, g);
				q = pushArgument(mb, q, d);
				e = getArg(q, 0);
				pushInstruction(mb, q);

				/* and insert them (j5 is result of sdiff above) */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, insertRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, e);
				q = pushArgument(mb, q, j5);
				j5 = getArg(q, 0);
				pushInstruction(mb, q);
				break;
			case j_expand:
				a = dumpwalkvar(mb, j1, j5);
				c = dumprefvar(mb, t->tval2, a, &j1, &j5, &j6, &j7);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j1);
				q = pushArgument(mb, q, c);
				a = getArg(q, 0);
				pushInstruction(mb, q);

				/* immediately cleanup j1 */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kdifference", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j1);
				q = pushArgument(mb, q, c);
				j1 = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, c);
				b = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kdifference", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j1);
				q = pushArgument(mb, q, b);
				j1 = getArg(q, 0);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, uselectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, a);
				q = pushBte(mb, q, 'a');  /* only arrays match expand */
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
				d = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, projectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushOid(mb, q, 0);  /* 0@0 = outermost array */
				q = pushArgument(mb, q, d);
				b = getArg(q, 0);
				pushInstruction(mb, q);

				/* remove old arrays and 0@0 array from j5 */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("sdifference", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j5);
				q = pushArgument(mb, q, d);
				j5 = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kdifference", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j5);
				q = pushArgument(mb, q, b);
				j5 = getArg(q, 0);
				pushInstruction(mb, q);

				/* append to top-level array */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, insertRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				q = pushArgument(mb, q, j5);
				j5 = getArg(q, 0);
				pushInstruction(mb, q);
				break;
			case j_unroll:
				a = dumpwalkvar(mb, j1, j5);
				b = dumprefvar(mb, t->tval2, a, &j1, &j5, &j6, &j7);
				e = dumpnextid(mb, j1);

				/* we only want the arrays from here */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j1);
				q = pushArgument(mb, q, b);
				b = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, selectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, b);
				q = pushBte(mb, q, 'a');
				c = getArg(q, 0);
				pushInstruction(mb, q);

				/* get parent(s), we only do objects for the moment */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, joinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j6);
				q = pushArgument(mb, q, c);
				c = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j6);
				q = pushArgument(mb, q, c);
				c = getArg(q, 0);
				pushInstruction(mb, q);

				/* get elements from the to be expanded array */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, semijoinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j5);
				q = pushArgument(mb, q, b);
				d = getArg(q, 0);
				pushInstruction(mb, q);

				/* generate kind elems for result (again, just objects) */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, markHRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, d);
				q = pushArgument(mb, q, e);
				f = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, projectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, f);
				q = pushBte(mb, q, 'o');
				f = getArg(q, 0);
				pushInstruction(mb, q);
				/* cleanup and append to kinds */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kdifference", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j1);
				q = pushArgument(mb, q, c);
				j1 = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kunion", 6));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j1);
				q = pushArgument(mb, q, f);
				j1 = getArg(q, 0);
				pushInstruction(mb, q);

				/* generate the outer array */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, projectRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, f);
				q = pushOid(mb, q, 0); /* probably not correct */
				f = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, f);
				f = getArg(q, 0);
				pushInstruction(mb, q);
				/* cleanup and append */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kdifference", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j5);
				q = pushArgument(mb, q, f);
				j5 = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kunion", 6));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, f);
				q = pushArgument(mb, q, j5);
				j5 = getArg(q, 0);
				pushInstruction(mb, q);

				/* construct the objects themselves */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, markTRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, d);
				q = pushArgument(mb, q, e);
				f = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, f);
				f = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, c);
				g = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, joinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, f);
				q = pushArgument(mb, q, g);
				g = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, joinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, g);
				q = pushArgument(mb, q, c);
				g = getArg(q, 0);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, markTRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, d);
				q = pushArgument(mb, q, e);
				f = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, f);
				f = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("sdifference", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, g);
				q = pushArgument(mb, q, f);
				g = getArg(q, 0);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, markHRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, d);
				q = pushArgument(mb, q, e);
				f = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("sunion", 6));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, g);
				q = pushArgument(mb, q, f);
				g = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, sortHTRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, g);
				g = getArg(q, 0);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, batRef);
				setFunctionId(q, reverseRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, d);
				f = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, joinRef);
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, f);
				q = pushArgument(mb, q, j7);
				f = getArg(q, 0);
				pushInstruction(mb, q);
				
				/* cleanup and append */
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kdifference", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j6);
				q = pushArgument(mb, q, c);
				j6 = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kunion", 6));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j6);
				q = pushArgument(mb, q, g);
				j6 = getArg(q, 0);
				pushInstruction(mb, q);

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kdifference", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j7);
				q = pushArgument(mb, q, d);
				j7 = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kdifference", 11));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j1);
				q = pushArgument(mb, q, d);
				j1 = getArg(q, 0);
				pushInstruction(mb, q);
				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, algebraRef);
				setFunctionId(q, putName("kunion", 6));
				q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
				q = pushArgument(mb, q, j7);
				q = pushArgument(mb, q, f);
				j7 = getArg(q, 0);
				pushInstruction(mb, q);

				break;
			case j_join: {
				json_var *js = bindjsonvars(mb, t->tval1);

				/* first calculate the join output, based on the input
				 * and predicates */
				dumppredjoin(mb, js, t, &j1, &j2, &j3, &j4, &j5, &j6 ,&j7);

				/* then transform the output with a modified into clause */
				changetmplrefsjoin(t->tval3, NULL);

				/* transform this node into a transform one, and force
				 * re-iteration so we simulate a pipe */
				t->type = j_transform;
				freetree(t->tval1);
				freetree(t->tval2);
				t->tval1 = GDKzalloc(sizeof(tree));
				t->tval1->type = j_var;
				t->tval1->sval = GDKstrdup("$");
				t->tval2 = t->tval3;
				t->tval3 = NULL;

				continue;
			}
			case j_group: {
				int lv[] = {j2, j3, j4, -1};
				int *lp = lv;
				tree *w;
				if (j1 != 0) { /* group from single input, previous pipe component */
					assert(t->tval1 == NULL ||
							(t->tval1->next == NULL && t->tval1->tval1 == NULL));
					w = t->tval1;
					if (w == NULL) {
						/* simple "into" query: equivalent of a single group */
						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, algebraRef);
						setFunctionId(q, putName("selectH", 7));
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, j5);
						q = pushOid(mb, q, (oid)0);
						a = getArg(q, 0);
						pushInstruction(mb, q);
						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, algebraRef);
						setFunctionId(q, putName("kdifference", 11));
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, j5);
						q = pushArgument(mb, q, a);
						j5 = getArg(q, 0);
						pushInstruction(mb, q);
						b = dumpnextid(mb, j1);
						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, algebraRef);
						setFunctionId(q, projectRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, b);
						q = pushArgument(mb, q, a);
						a = getArg(q, 0);
						pushInstruction(mb, q);
						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, batRef);
						setFunctionId(q, insertRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, a);
						q = pushOid(mb, q, (oid)0);
						q = pushArgument(mb, q, b);
						a = getArg(q, 0);
						pushInstruction(mb, q);
						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, batRef);
						setFunctionId(q, insertRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, a);
						q = pushArgument(mb, q, j5);
						j5 = getArg(q, 0);
						pushInstruction(mb, q);

						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, batRef);
						setFunctionId(q, newRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushType(mb, q, TYPE_oid);
						q = pushType(mb, q, TYPE_bte);
						c = getArg(q, 0);
						pushInstruction(mb, q);
						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, batRef);
						setFunctionId(q, insertRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, c);
						q = pushArgument(mb, q, j1);
						j1 = getArg(q, 0);
						pushInstruction(mb, q);
						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, batRef);
						setFunctionId(q, insertRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, j1);
						q = pushArgument(mb, q, b);
						q = pushBte(mb, q, 'a');
						j1 = getArg(q, 0);
						pushInstruction(mb, q);

						t->type = j_transform;
						t->tval1 = GDKzalloc(sizeof(tree));
						t->tval1->type = j_var;
						t->tval1->sval = GDKstrdup("$");
						continue;
					}
					/* recall:
					 *   sval = varname of group key
					 *   tval1 = source input (if co-group)
					 *   tval2 = var from source to group on
					 *   tval3 = name of result (default $) (only into)
					 */
					a = dumpwalkvar(mb, j1, j5);
					b = dumprefvar(mb, w->tval2, a, &j1, &j5, &j6, &j7);
					/* b should point to all "groups" now */

					for (; *lp != -1; lp++) {
						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, algebraRef);
						setFunctionId(q, semijoinRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, *lp);
						q = pushArgument(mb, q, b);
						d = getArg(q, 0);
						pushInstruction(mb, q);
						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, algebraRef);
						setFunctionId(q, tuniqueRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, d);
						e = getArg(q, 0);
						pushInstruction(mb, q);
						f = dumpnextid(mb, j1);
						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, algebraRef);
						setFunctionId(q, markHRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, e);
						q = pushArgument(mb, q, f);
						f = e = getArg(q, 0);
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
						q = pushArgument(mb, q, d);
						q = pushArgument(mb, q, e);
						e = getArg(q, 0);
						pushInstruction(mb, q);
						/* e = kindid:grouparrayid */

						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, batRef);
						setFunctionId(q, reverseRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, b);
						d = getArg(q, 0);
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
						setModuleId(q, batRef);
						setFunctionId(q, reverseRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, e);
						e = getArg(q, 0);
						pushInstruction(mb, q);
						/* e = grouparrayid:elementid */

						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, algebraRef);
						setFunctionId(q, projectRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushOid(mb, q, (oid)0);
						q = pushArgument(mb, q, e);
						d = getArg(q, 0);
						pushInstruction(mb, q);
						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, algebraRef);
						setFunctionId(q, putName("sdifference", 11));
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, j5);
						q = pushArgument(mb, q, d);
						j5 = getArg(q, 0);
						pushInstruction(mb, q);

						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, batRef);
						setFunctionId(q, insertRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, j5);
						q = pushArgument(mb, q, e);
						j5 = getArg(q, 0);
						pushInstruction(mb, q);

						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, batRef);
						setFunctionId(q, reverseRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, f);
						e = getArg(q, 0);
						pushInstruction(mb, q);
						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, algebraRef);
						setFunctionId(q, projectRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushOid(mb, q, (oid)0);
						q = pushArgument(mb, q, e);
						e = getArg(q, 0);
						pushInstruction(mb, q);
						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, batRef);
						setFunctionId(q, insertRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, j5);
						q = pushArgument(mb, q, e);
						j5 = getArg(q, 0);
						pushInstruction(mb, q);

						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, algebraRef);
						setFunctionId(q, projectRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, f);
						q = pushBte(mb, q, 'a');
						e = getArg(q, 0);
						pushInstruction(mb, q);
						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, algebraRef);
						setFunctionId(q, kunionRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, j1);
						q = pushArgument(mb, q, e);
						j1 = getArg(q, 0);
						pushInstruction(mb, q);
					}

					/* because all groups are contained in an array now, the
					 * groupkeyvar is represented by "(sval)[0](tval2)",
					 * e.g. $[0].a if the walk alias is $ and the var to
					 * group on is $.a */
					changetmplrefsgroup(t->tval2,
							t->tval1->sval, t->tval1->tval2);

					/* transform this node into a transform one, and force
					 * re-iteration so we simulate a pipe */
					t->type = j_transform;
					freetree(t->tval1);
					t->tval1 = GDKzalloc(sizeof(tree));
					t->tval1->type = j_var;
					t->tval1->sval = GDKstrdup("$");

					continue;
				} else { /* co-group */
					json_var *js;
					tree *w, *preds, *pw;
					int i;

					/* first compute with join */
					for (i = 0, w = t->tval1; w != NULL; w = w->next, i++)
						;
					js = GDKmalloc(sizeof(json_var) * (i + 1));
					for (i = 0, w = t->tval1; w != NULL; w = w->next, i++) {
						js[i].name = w->tval3->sval; /* always _IDENT */
						js[i].preserve = 0;
						dumpgetvar(mb, w->tval1->sval,
								&js[i].j1, &js[i].j2, &js[i].j3, &js[i].j4,
								&js[i].j5, &js[i].j6, &js[i].j7);
					}
					js[i].name = NULL;

					preds = GDKzalloc(sizeof(tree));
					preds->type = j_join;
					pw = preds->tval2 = GDKzalloc(sizeof(tree));
					GDKfree(t->tval1->tval2->sval);
					t->tval1->tval2->sval = GDKstrdup(t->tval1->tval3->sval);
					for (w = t->tval1; w != NULL && w->next != NULL; w = w->next) {
						GDKfree(w->next->tval2->sval);
						w->next->tval2->sval = GDKstrdup(w->next->tval3->sval);
						pw->tval1 = w->tval2;
						pw->tval2 = GDKzalloc(sizeof(tree));
						pw->tval2->type = j_comp;
						pw->tval2->cval = j_equals;
						pw->tval3 = w->next->tval2;
						if (w->next->next != NULL)
							pw = pw->next = GDKzalloc(sizeof(tree));
					}

					dumppredjoin(mb, js, preds,
							&j1, &j2, &j3, &j4, &j5, &j6, &j7);

					for (pw = preds->tval2; pw != NULL; pw = pw->next)
						pw->tval1 = pw->tval3 = NULL;
					freetree(preds);
					GDKfree(js);

					/* demote to single-input group, but mangle the
					 * transforms and the groupkey */
					changetmplrefsjoin(t->tval2, t->tval1->sval);
					w = t->tval1->tval2;
					t->tval1->tval2 = make_varname(GDKstrdup("$"));
					t->tval1->tval2->tval1 = w;
					freetree(t->tval1->next);
					t->tval1->next = NULL;
					freetree(t->tval1->tval1);
					t->tval1->tval1 = NULL;
					continue; /* reevaluate this group */
				}
				assert(0);
			} break;
			case j_sort: {
				int l[4][2] = {{j2, 's'}, {j3, 'i'}, {j4, 'd'}, {0, 0}};
				int lw;
				tree *rpreds = NULL, *w;

				/* build backwards list of sort predicates, such that we
				 * can resort with stable sort back to the first and
				 * most significant sort predicate */
				for (w = t->tval2; w != NULL; w = w->next) {
					/* misuse tval3 to build backwards chain */
					w->tval3 = rpreds;
					rpreds = w;
				}
				t->tval2 = rpreds;

				a = dumpwalkvar(mb, j1, j5);
				
				for (w = rpreds; w != NULL; w = w->tval3) {
					/* avoid double free upon cleanup */
					w->next = NULL;

					b = dumprefvar(mb, w->tval1, a, &j1, &j5, &j6, &j7);
					/* can only sort on one type (str, lng, dbl), and can't
					 * combine these, so pick first element's type and
					 * sort all of those */
					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, batRef);
					setFunctionId(q, newRef);
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushType(mb, q, TYPE_bte);
					q = pushType(mb, q, TYPE_bte);
					c = getArg(q, 0);
					pushInstruction(mb, q);
					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, batRef);
					setFunctionId(q, insertRef);
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, c);
					q = pushBte(mb, q, 's');
					q = pushBte(mb, q, 's');
					c = getArg(q, 0);
					pushInstruction(mb, q);
					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, batRef);
					setFunctionId(q, insertRef);
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, c);
					q = pushBte(mb, q, 'i');
					q = pushBte(mb, q, 'i');
					c = getArg(q, 0);
					pushInstruction(mb, q);
					q = newInstruction(mb, ASSIGNsymbol);
					setModuleId(q, batRef);
					setFunctionId(q, insertRef);
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushArgument(mb, q, c);
					q = pushBte(mb, q, 'd');
					q = pushBte(mb, q, 'd');
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
						q = pushBte(mb, q, l[lw][1]);
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
						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, batRef);
						setFunctionId(q, reverseRef);
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, d);
						d = getArg(q, 0);
						pushInstruction(mb, q);

						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, algebraRef);
						if (w->nval == 1) {
							setFunctionId(q, putName("ssort", 5));
						} else {
							setFunctionId(q, putName("ssort_rev", 9));
						}
						q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
						q = pushArgument(mb, q, d);
						d = getArg(q, 0);
						pushInstruction(mb, q);
						q = newInstruction(mb, ASSIGNsymbol);
						setModuleId(q, batRef);
						setFunctionId(q, reverseRef);
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
				}
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
			case j_func: {
				enum treetype coltypes[MAXJAQLARG];
				int dynaarg[MAXJAQLARG][7];
				int coltpos = 0;
				int i, funcretc = 0;
				tree *w;

				if (j1 != 0) {
					/* treat pipe as first input of type json array */
					coltypes[coltpos++] = j_json_arr;
				}
				funcretc = matchfuncsig(j, cntxt, t,
						&coltpos, &coltypes, &dynaarg);
				if (funcretc == 0)
 					break;

				i = 0;
				if (j1 != 0) {
					/* treat pipe as first input */
					switch (coltypes[i]) {
						case j_json_arr:
							dynaarg[i][0] = j1;
							dynaarg[i][1] = j2;
							dynaarg[i][2] = j3;
							dynaarg[i][3] = j4;
							dynaarg[i][4] = j5;
							dynaarg[i][5] = j6;
							dynaarg[i][6] = j7;
							break;
						case j_bool:
							a = dumpvalsfromarr(mb, coltypes[i],
									j1, j2, j3, j4, j5);
							dynaarg[i][0] = a;
							break;
						case j_sort_arg:
							q = newInstruction(mb, ASSIGNsymbol);
							setModuleId(q, putName("json", 4));
							setFunctionId(q, putName("unwraptype", 10));
							q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
							q = pushArgument(mb, q, j1);
							q = pushArgument(mb, q, j2);
							q = pushArgument(mb, q, j3);
							q = pushArgument(mb, q, j4);
							q = pushArgument(mb, q, j5);
							q = pushArgument(mb, q, j6);
							q = pushArgument(mb, q, j7);
							q = pushOid(mb, q, (oid)0);
							a = getArg(q, 0);
							dynaarg[i][0] = a;
							pushInstruction(mb, q);

							e = -1;
							if (dynaarg[i][1] == 0) {
								c = newBatType(TYPE_oid, TYPE_str);
								q = newInstruction(mb, ASSIGNsymbol);
								q = pushReturn(mb, q,
										newTmpVariable(mb, c));
								q = pushNil(mb, q, c);
								d = getArg(q, 0);
								pushInstruction(mb, q);

								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, calcRef);
								setFunctionId(q, putName("==", 2));
								q->barrier = BARRIERsymbol;
								q = pushReturn(mb, q,
										newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, a);
								q = pushStr(mb, q, "str");
								e = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, putName("json", 4));
								setFunctionId(q, putName("unwrap", 6));
								q = pushReturn(mb, q, c);
								q = pushArgument(mb, q, j1);
								q = pushArgument(mb, q, j2);
								q = pushArgument(mb, q, j3);
								q = pushArgument(mb, q, j4);
								q = pushArgument(mb, q, j5);
								q = pushArgument(mb, q, j6);
								q = pushArgument(mb, q, j7);
								q = pushOid(mb, q, (oid)0);
								q = pushStr(mb, q, "");
								dynaarg[i][1] = getArg(q, 0);
								pushInstruction(mb, q);
								q = newAssignment(mb);
								getArg(q, 0) = e;
								q->argc = q->retc = 1;
								q->barrier = EXITsymbol;
							}

							f = -1;
							if (dynaarg[i][2] == 0) {
								c = newBatType(TYPE_oid, TYPE_dbl);
								q = newInstruction(mb, ASSIGNsymbol);
								q = pushReturn(mb, q,
										newTmpVariable(mb, c));
								q = pushNil(mb, q, c);
								d = getArg(q, 0);
								pushInstruction(mb, q);

								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, calcRef);
								setFunctionId(q, putName("==", 2));
								q->barrier = BARRIERsymbol;
								q = pushReturn(mb, q,
										newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, a);
								q = pushStr(mb, q, "dbl");
								f = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, putName("json", 4));
								setFunctionId(q, putName("unwrap", 6));
								q = pushReturn(mb, q, d);
								q = pushArgument(mb, q, j1);
								q = pushArgument(mb, q, j2);
								q = pushArgument(mb, q, j3);
								q = pushArgument(mb, q, j4);
								q = pushArgument(mb, q, j5);
								q = pushArgument(mb, q, j6);
								q = pushArgument(mb, q, j7);
								q = pushOid(mb, q, (oid)0);
								q = pushDbl(mb, q, 0.0);
								dynaarg[i][2] = getArg(q, 0);
								pushInstruction(mb, q);
								q = newAssignment(mb);
								getArg(q, 0) = f;
								q->argc = q->retc = 1;
								q->barrier = EXITsymbol;
							}

							g = -1;
							if (dynaarg[i][3] == 0) {
								c = newBatType(TYPE_oid, TYPE_lng);
								q = newInstruction(mb, ASSIGNsymbol);
								q = pushReturn(mb, q,
										newTmpVariable(mb, c));
								q = pushNil(mb, q, c);
								d = getArg(q, 0);
								pushInstruction(mb, q);

								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, calcRef);
								setFunctionId(q, putName("==", 2));
								q->barrier = BARRIERsymbol;
								q = pushReturn(mb, q,
										newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, a);
								q = pushStr(mb, q, "lng");
								g = getArg(q, 0);
								pushInstruction(mb, q);
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, putName("json", 4));
								setFunctionId(q, putName("unwrap", 6));
								q = pushReturn(mb, q, d);
								q = pushArgument(mb, q, j1);
								q = pushArgument(mb, q, j2);
								q = pushArgument(mb, q, j3);
								q = pushArgument(mb, q, j4);
								q = pushArgument(mb, q, j5);
								q = pushArgument(mb, q, j6);
								q = pushArgument(mb, q, j7);
								q = pushOid(mb, q, (oid)0);
								q = pushLng(mb, q, 0);
								dynaarg[i][3] = getArg(q, 0);
								pushInstruction(mb, q);
								q = newAssignment(mb);
								getArg(q, 0) = g;
								q->argc = q->retc = 1;
								q->barrier = EXITsymbol;
							}

							if (e >= 0 && f >= 0) {
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, calcRef);
								setFunctionId(q, putName("or", 2));
								q = pushReturn(mb, q,
										newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, e);
								q = pushArgument(mb, q, f);
								f = getArg(q, 0);
								pushInstruction(mb, q);
							} else if (e >= 0) {
								f = e;
							}
							if (f >= 0 && g >= 0) {
								q = newInstruction(mb, ASSIGNsymbol);
								setModuleId(q, calcRef);
								setFunctionId(q, putName("or", 2));
								q = pushReturn(mb, q,
										newTmpVariable(mb, TYPE_any));
								q = pushArgument(mb, q, f);
								q = pushArgument(mb, q, g);
								g = getArg(q, 0);
								pushInstruction(mb, q);
							} else if (f >= 0) {
								g = f;
							}
							assert(g >= 0);
							q = newInstruction(mb, ASSIGNsymbol);
							setModuleId(q, calcRef);
							setFunctionId(q, putName("not", 3));
							q->barrier = BARRIERsymbol;
							q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
							q = pushArgument(mb, q, g);
							b = getArg(q, 0);
							pushInstruction(mb, q);
							q = newInstruction(mb, ASSIGNsymbol);
							setModuleId(q, languageRef);
							setFunctionId(q, putName("raise", 5));
							q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
							q = pushStr(mb, q, "function does not support maximum "
									"type from JSON array");
							pushInstruction(mb, q);
							q = newAssignment(mb);
							getArg(q, 0) = b;
							q->argc = q->retc = 1;
							q->barrier = EXITsymbol;

							break;
						case j_func_arg:
							a = dumpwalkvar(mb, j1, j5);
							q = newInstruction(mb, ASSIGNsymbol);
							setModuleId(q, algebraRef);
							setFunctionId(q, projectRef);
							q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
							q = pushOid(mb, q, 0);
							q = pushArgument(mb, q, a);
							a = getArg(q, 0);
							pushInstruction(mb, q);
							dynaarg[i][0] = a;
							break;
						default:
							assert(0);
					}
					i++;
				}
				for (w = t->tval1; w != NULL; w = w->next, i++) {
					int a1, a2, a3, a4, a5, a6, a7;

					assert(w->type == j_func_arg);
					assert(w->tval1 != NULL);

					switch (w->tval1->type) {
						case j_json:
							dumpjsonshred(mb, w->tval1->sval,
									&a1, &a2, &a3, &a4, &a5, &a6, &a7);
							switch (coltypes[i]) {
								case j_json:
									dynaarg[i][0] = a1;
									dynaarg[i][1] = a2;
									dynaarg[i][2] = a3;
									dynaarg[i][3] = a4;
									dynaarg[i][4] = a5;
									dynaarg[i][5] = a6;
									dynaarg[i][6] = a7;
									break;
								case j_bool:
									a = dumpvalsfromarr(mb, coltypes[i],
											a1, a2, a3, a4, a5);
									dynaarg[i][0] = a;
									break;
								case j_sort_arg:
									q = newInstruction(mb, ASSIGNsymbol);
									setModuleId(q, putName("json", 4));
									setFunctionId(q, putName("unwraptype", 10));
									q = pushReturn(mb, q,
											newTmpVariable(mb, TYPE_any));
									q = pushArgument(mb, q, a1);
									q = pushArgument(mb, q, a2);
									q = pushArgument(mb, q, a3);
									q = pushArgument(mb, q, a4);
									q = pushArgument(mb, q, a5);
									q = pushArgument(mb, q, a6);
									q = pushArgument(mb, q, a7);
									q = pushOid(mb, q, (oid)0);
									a = getArg(q, 0);
									dynaarg[i][0] = a;
									pushInstruction(mb, q);

									if (dynaarg[i][1] == 0) {
										c = newBatType(TYPE_oid, TYPE_str);
										q = newInstruction(mb, ASSIGNsymbol);
										q = pushReturn(mb, q,
												newTmpVariable(mb, c));
										q = pushNil(mb, q, c);
										d = getArg(q, 0);
										pushInstruction(mb, q);
										q = newInstruction(mb, ASSIGNsymbol);

										setModuleId(q, calcRef);
										setFunctionId(q, putName("==", 2));
										q->barrier = BARRIERsymbol;
										q = pushReturn(mb, q,
												newTmpVariable(mb, TYPE_any));
										q = pushArgument(mb, q, a);
										q = pushStr(mb, q, "str");
										b = getArg(q, 0);
										pushInstruction(mb, q);
										q = newInstruction(mb, ASSIGNsymbol);
										setModuleId(q, putName("json", 4));
										setFunctionId(q, putName("unwrap", 6));
										q = pushReturn(mb, q, c);
										q = pushArgument(mb, q, a1);
										q = pushArgument(mb, q, a2);
										q = pushArgument(mb, q, a3);
										q = pushArgument(mb, q, a4);
										q = pushArgument(mb, q, a5);
										q = pushArgument(mb, q, a6);
										q = pushArgument(mb, q, a7);
										q = pushOid(mb, q, (oid)0);
										q = pushStr(mb, q, "");
										dynaarg[i][1] = getArg(q, 0);
										pushInstruction(mb, q);
										q = newAssignment(mb);
										getArg(q, 0) = b;
										q->argc = q->retc = 1;
										q->barrier = EXITsymbol;
									}

									if (dynaarg[i][2] == 0) {
										c = newBatType(TYPE_oid, TYPE_dbl);
										q = newInstruction(mb, ASSIGNsymbol);
										q = pushReturn(mb, q,
												newTmpVariable(mb, c));
										q = pushNil(mb, q, c);
										d = getArg(q, 0);
										pushInstruction(mb, q);
										q = newInstruction(mb, ASSIGNsymbol);

										q = newInstruction(mb, ASSIGNsymbol);
										setModuleId(q, calcRef);
										setFunctionId(q, putName("==", 2));
										q->barrier = BARRIERsymbol;
										q = pushReturn(mb, q,
												newTmpVariable(mb, TYPE_any));
										q = pushArgument(mb, q, a);
										q = pushStr(mb, q, "dbl");
										b = getArg(q, 0);
										pushInstruction(mb, q);
										q = newInstruction(mb, ASSIGNsymbol);
										setModuleId(q, putName("json", 4));
										setFunctionId(q, putName("unwrap", 6));
										q = pushReturn(mb, q, d);
										q = pushArgument(mb, q, a1);
										q = pushArgument(mb, q, a2);
										q = pushArgument(mb, q, a3);
										q = pushArgument(mb, q, a4);
										q = pushArgument(mb, q, a5);
										q = pushArgument(mb, q, a6);
										q = pushArgument(mb, q, a7);
										q = pushOid(mb, q, (oid)0);
										q = pushDbl(mb, q, 0.0);
										dynaarg[i][2] = getArg(q, 0);
										pushInstruction(mb, q);
										q = newAssignment(mb);
										getArg(q, 0) = b;
										q->argc = q->retc = 1;
										q->barrier = EXITsymbol;
									}

									if (dynaarg[i][3] == 0) {
										c = newBatType(TYPE_oid, TYPE_lng);
										q = newInstruction(mb, ASSIGNsymbol);
										q = pushReturn(mb, q,
												newTmpVariable(mb, c));
										q = pushNil(mb, q, c);
										d = getArg(q, 0);
										pushInstruction(mb, q);
										q = newInstruction(mb, ASSIGNsymbol);

										q = newInstruction(mb, ASSIGNsymbol);
										setModuleId(q, calcRef);
										setFunctionId(q, putName("==", 2));
										q->barrier = BARRIERsymbol;
										q = pushReturn(mb, q,
												newTmpVariable(mb, TYPE_any));
										q = pushArgument(mb, q, a);
										q = pushStr(mb, q, "lng");
										b = getArg(q, 0);
										pushInstruction(mb, q);
										q = newInstruction(mb, ASSIGNsymbol);
										setModuleId(q, putName("json", 4));
										setFunctionId(q, putName("unwrap", 6));
										q = pushReturn(mb, q, d);
										q = pushArgument(mb, q, a1);
										q = pushArgument(mb, q, a2);
										q = pushArgument(mb, q, a3);
										q = pushArgument(mb, q, a4);
										q = pushArgument(mb, q, a5);
										q = pushArgument(mb, q, a6);
										q = pushArgument(mb, q, a7);
										q = pushOid(mb, q, (oid)0);
										q = pushLng(mb, q, 0);
										dynaarg[i][3] = getArg(q, 0);
										pushInstruction(mb, q);
										q = newAssignment(mb);
										getArg(q, 0) = b;
										q->argc = q->retc = 1;
										q->barrier = EXITsymbol;
									}
									break;
								case j_func_arg:
									a = dumpwalkvar(mb, a1, a5);
									q = newInstruction(mb, ASSIGNsymbol);
									setModuleId(q, algebraRef);
									setFunctionId(q, projectRef);
									q = pushReturn(mb, q,
											newTmpVariable(mb, TYPE_any));
									q = pushOid(mb, q, 0);
									q = pushArgument(mb, q, a);
									a = getArg(q, 0);
									pushInstruction(mb, q);
									dynaarg[i][0] = a;
									break;
								default:
									assert(0);
							}
							break;
						case j_str:
						case j_num:
						case j_dbl:
						case j_bool:
							q = newInstruction(mb, ASSIGNsymbol);
							setModuleId(q, batRef);
							setFunctionId(q, newRef);
							q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
							q = pushType(mb, q, TYPE_oid);
							if (coltypes[i] == j_str) {
								q = pushType(mb, q, TYPE_str);
							} else if (coltypes[i] == j_num) {
								q = pushType(mb, q, TYPE_lng);
							} else if (coltypes[i] == j_dbl) {
								q = pushType(mb, q, TYPE_dbl);
							} else /* j_bool */ {
								q = pushType(mb, q, TYPE_bit);
							}
							a = getArg(q, 0);
							pushInstruction(mb, q);
							q = newInstruction(mb, ASSIGNsymbol);
							setModuleId(q, batRef);
							setFunctionId(q, insertRef);
							q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
							q = pushArgument(mb, q, a);
							q = pushOid(mb, q, (oid)0);
							if (coltypes[i] == j_str) {
								q = pushStr(mb, q, w->tval1->sval);
							} else if (coltypes[i] == j_num) {
								q = pushLng(mb, q, w->tval1->nval);
							} else if (coltypes[i] == j_dbl) {
								q = pushDbl(mb, q, w->tval1->dval);
							} else /* j_bool */ {
								q = pushBit(mb, q, w->tval1->nval == 1);
							}
							a = getArg(q, 0);
							pushInstruction(mb, q);
							dynaarg[i][0] = a;
							break;
						case j_var: /* TODO */
							/* j_var is actually impossible at this level */
						default:
							snprintf(j->err, sizeof(j->err),
									"unhandled argument type (1)");
							return -1;
					}
				}

				q = newInstruction(mb, ASSIGNsymbol);
				setModuleId(q, putName("jaqlfunc", 8));
				setFunctionId(q, putName(t->sval, strlen(t->sval)));
				if (funcretc == 7) {
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					q = pushReturn(mb, q, newTmpVariable(mb, TYPE_any));
					/* only single BAT returning functions can be dynatyped */
					j1 = getArg(q, 0);
					j2 = getArg(q, 1);
					j3 = getArg(q, 2);
					j4 = getArg(q, 3);
					j5 = getArg(q, 4);
					j6 = getArg(q, 5);
					j7 = getArg(q, 6);
					for (i = 0; i < coltpos; i++) {
						switch (coltypes[i]) {
							case j_json:
							case j_json_arr:
								q = pushArgument(mb, q, dynaarg[i][0]);
								q = pushArgument(mb, q, dynaarg[i][1]);
								q = pushArgument(mb, q, dynaarg[i][2]);
								q = pushArgument(mb, q, dynaarg[i][3]);
								q = pushArgument(mb, q, dynaarg[i][4]);
								q = pushArgument(mb, q, dynaarg[i][5]);
								q = pushArgument(mb, q, dynaarg[i][6]);
								break;
							case j_sort_arg:
								assert(0);
							default:
								q = pushArgument(mb, q, dynaarg[i][0]);
								break;
						}
					}
					pushInstruction(mb, q);
				} else {
					InstrPtr r;
					r = newInstruction(mb, ASSIGNsymbol);
					a = newTmpVariable(mb, newBatType(TYPE_oid, TYPE_any));
					r = pushReturn(mb, r, a);
					r = pushNil(mb, r, newBatType(TYPE_oid, TYPE_any));
					pushInstruction(mb, r);

					conditionalcall(&a, mb, t->tval1,
							coltypes, dynaarg, coltpos, q);

					/* this is for top-level (in a JAQL pipe), so it
					 * should always return a JSON struct, hence create
					 * an array with values from the BAT we have
					 * returned */
					r = newInstruction(mb, ASSIGNsymbol);
					setModuleId(r, putName("json", 4));
					setFunctionId(r, putName("wrap", 4));
					r = pushReturn(mb, r, newTmpVariable(mb, TYPE_any));
					r = pushReturn(mb, r, newTmpVariable(mb, TYPE_any));
					r = pushReturn(mb, r, newTmpVariable(mb, TYPE_any));
					r = pushReturn(mb, r, newTmpVariable(mb, TYPE_any));
					r = pushReturn(mb, r, newTmpVariable(mb, TYPE_any));
					r = pushReturn(mb, r, newTmpVariable(mb, TYPE_any));
					r = pushReturn(mb, r, newTmpVariable(mb, TYPE_any));
					r = pushArgument(mb, r, a);
					j1 = getArg(r, 0);
					j2 = getArg(r, 1);
					j3 = getArg(r, 2);
					j4 = getArg(r, 3);
					j5 = getArg(r, 4);
					j6 = getArg(r, 5);
					j7 = getArg(r, 6);
					pushInstruction(mb, r);
				}
			} break;
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
