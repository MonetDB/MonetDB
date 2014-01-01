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
 *
*/
/*
 * @- OID origin tracking.
 * This simple module extends the MAL plan with properties that
 * identify the source of values in the underlying SQL persistent tables.
 * It stops when it can not be unique pinpointed anymore.
 * The information is kept with the variables as horigin and torigin.
 * [still incomplete]
 */
#include "monetdb_config.h"
#include "opt_origin.h"

int 
OPToriginImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	InstrPtr p=0;
	int actions = 0;
	char buf[BUFSIZ];
	ValRecord val;
	VarPtr h, t;

	(void) cntxt;
	(void) pci;
	(void) stk;		/* to fool compilers */

	for (i = 1; i < mb->stop; i++) {
		p= getInstrPtr(mb,i);

		val.vtype = TYPE_int;
		if( getModuleId(p)== sqlRef && getFunctionId(p)== bindRef  &&
			isVarConstant(mb,getArg(p,2)) && isVarConstant(mb,getArg(p,3)) && isVarConstant(mb,getArg(p,4)) ) {
			snprintf(buf, BUFSIZ, "%s_%s_%s", getVarConstant(mb,getArg(p,2)).val.sval, getVarConstant(mb,getArg(p,3)).val.sval,getVarConstant(mb,getArg(p,4)).val.sval);
			actions++;
			VALset(&val, TYPE_str, GDKstrdup(buf));
			varSetProp(mb, getArg(p,0), horiginProp, op_eq, &val);
#ifdef _DEBUG_OPT_ORIGIN_
			mnstr_printf(cntxt->fdout,"#%s\n",buf);
			printInstruction(cntxt->fdout,mb, 0, p, LIST_MAL_ALL);
#endif
		}
		if ( getModuleId(p) == algebraRef) {
			if ( getFunctionId(p) == kdifferenceRef ||
				 getFunctionId(p) == kunionRef ||
				 getFunctionId(p) == thetauselectRef 
				) {
				h= varGetProp(mb, getArg(p,1), horiginProp);
				if ( h ) {
					VALset(&val, TYPE_str, GDKstrdup(h->value.val.sval));
					varSetProp(mb, getArg(p,0), horiginProp, op_eq, &val);
				}
			}
			if ( getFunctionId(p) == markTRef) {
				h= varGetProp(mb, getArg(p,1), horiginProp);
				if ( h ){
					VALset(&val, TYPE_str, GDKstrdup(h->value.val.sval));
					varSetProp(mb, getArg(p,0), horiginProp, op_eq, &val);
				}
			}
			if ( getFunctionId(p) == leftjoinRef ||
				 getFunctionId(p) == joinRef) {
				h= varGetProp(mb, getArg(p,1), horiginProp);
				t= varGetProp(mb, getArg(p,2), toriginProp);
				if ( h ){
					VALset(&val, TYPE_str, GDKstrdup(h->value.val.sval));
					varSetProp(mb, getArg(p,0), horiginProp, op_eq, &val);
				}
				if ( t ){
					VALset(&val, TYPE_str, GDKstrdup(t->value.val.sval));
					varSetProp(mb, getArg(p,0), toriginProp, op_eq, &val);
				}
			}
		}
		if ( getModuleId(p) == groupRef) {
			if ( getFunctionId(p) == newRef) {
				h= varGetProp(mb, getArg(p,1), horiginProp);
				if ( h ){
					VALset(&val, TYPE_str, GDKstrdup(h->value.val.sval));
					varSetProp(mb, getArg(p,0), horiginProp, op_eq, &val);
					VALset(&val, TYPE_str, GDKstrdup(h->value.val.sval));
					varSetProp(mb, getArg(p,1), horiginProp, op_eq, &val);
				}
			}
		}
		if ( getModuleId(p) == batRef) {
			if ( getFunctionId(p) == mirrorRef) {
				h= varGetProp(mb, getArg(p,1), horiginProp);
				if ( h ){
					VALset(&val, TYPE_str, GDKstrdup(h->value.val.sval));
					varSetProp(mb, getArg(p,0), horiginProp, op_eq, &val);
					VALset(&val, TYPE_str, GDKstrdup(h->value.val.sval));
					varSetProp(mb, getArg(p,0), toriginProp, op_eq, &val);
				}
			}
			if ( getFunctionId(p) == reverseRef) {
				h= varGetProp(mb, getArg(p,1), horiginProp);
				t= varGetProp(mb, getArg(p,1), toriginProp);
				if ( h ){
					VALset(&val, TYPE_str, GDKstrdup(h->value.val.sval));
					varSetProp(mb, getArg(p,0), toriginProp, op_eq, &val);
				}
				if ( t ) {
					VALset(&val, TYPE_str, GDKstrdup(t->value.val.sval));
					varSetProp(mb, getArg(p,0), horiginProp, op_eq, &val);
				}
			}
		}
	}
	return actions;
}
