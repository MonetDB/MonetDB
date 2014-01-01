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

#include "monetdb_config.h"
#include "opt_qep.h"

static QEP
QEPnew(int p, int c){
	QEP qep;
	qep = (QEP) GDKmalloc( sizeof(struct QEPrecord));
	if (qep == NULL)
		return NULL;
	qep->mb= NULL;
	qep->p = NULL;
	qep->plimit = p;
	if( p )
		qep->parents = (QEP*) GDKzalloc( sizeof(QEP) * p);
	qep->climit = c;
	if( c)
		qep->children = (QEP *) GDKzalloc( sizeof(QEP) * c);
	return qep;
}
static QEP
QEPnewNode(MalBlkPtr mb,InstrPtr p){
	QEP q;
	q= QEPnew(p->retc,p->argc-p->retc+1);
	q->mb= mb;
	q->p = p;
	return q;
}
static QEP
QEPexpandChildren(QEP qep, int extra){
	int i;
	/*extend node */
	qep->children = (QEP*) GDKrealloc( (char*) qep->children, 
				sizeof(QEP) * (qep->climit + extra));
	for(i=qep->climit;i <qep->climit + extra; i++)
		qep->children[i]=0;
	qep->climit = qep->climit + extra;
	return qep;
}

#if 0
static QEP
QEPfree(QEP qep){
	GDKfree(qep->children);
	GDKfree(qep->parents);
	GDKfree(qep);
	return NULL;
}
#endif

/* Extract a child from the qep, to be inserted somewhere else */
#if 0
static QEP
QEPdelete(QEP qep, int pos){
	int i;
	QEP q= NULL;

	for(i=0; i<qep->climit && qep->children[i]; i++){
		if(pos-- == 0) q = qep->children[i];
		if( pos <0 ) qep->children[i]= qep->children[i+1];
		if( i< qep->climit-1) q->children[i]= NULL;
	}
	return q;
}
#endif
static QEP
QEPappend(QEP qep, QEP child){
	int i;
	for( i=0; i< qep->climit-1; i++)
		if( qep->children[i] == NULL)
			break;
	if(qep->climit== 0 || qep->children[i]!= NULL )
		qep= QEPexpandChildren(qep,MAXCHILD);
	qep->children[i]= child;
	if( child) 
		child->parents[0]= qep;
	return qep;
}
#if 0
static QEP
QEPinsert(QEP qep, int pos, QEP child){
	int i;
	QEP q= NULL, qn; 
	for( i=0; i< qep->climit; i++){
		if( pos-- == 0){
			q= qep->children[i];
			qep->children[i]= child;
			child->parents[0] =qep;
		}
		if( pos < 0 && i<qep->climit-1){
			qn= qep->children[i+1];
			qep->children[i+1] = q;
			q= qn;
		}
	}
	if( q != NULL)
		qep = QEPappend(qep,q);
	return qep;
}
#endif
/*
 * The core of the work is focused on building the tree using a flow analysis. 
 * Building the tree means that we should not allow the same variable can not be used twice.
*/
#define LEAFNODE 2
#define TOPNODE 3

static QEP
QEPbuild(MalBlkPtr mb){
	QEP qroot= NULL, q= NULL, *vq;
	InstrPtr p;
	int i, j, k, *status;

	vq= (QEP*) GDKmalloc( mb->vtop * sizeof(QEP));
	if (vq == NULL)
		return NULL;
	status= (int*) GDKmalloc( mb->vtop * sizeof(int));
	if (status == NULL){
		GDKfree(vq);
		return NULL;
	}
	for(i=0; i<mb->vtop; i++) {
		status[i]= 0;
		vq[i] = 0;
	}

	for(i=1; i< mb->stop-1; i++){
		p= getInstrPtr(mb,i);
		q= QEPnewNode(mb,p);
		for( k=p->retc; k<p->argc; k++) 
		if( ! isVarConstant(mb, getArg(p,k)) ){
			status[getArg(p,k)]= LEAFNODE;
			if( vq[getArg(p,k)] )
				QEPappend(q, vq[getArg(p,k)]);
		}
		for( k=0; k<p->retc; k++){
			if(	vq[getArg(p,k)] == 0)
				vq[getArg(p,k)] = q;
			status[getArg(p,k)]= TOPNODE;
		}

	}
/* We may end up with multiple variables not yet bound to a QEP. */

	qroot= QEPnew(MAXPARENT,mb->stop);
	for(i=1; i< mb->stop-1; i++){
		p= getInstrPtr(mb,i);
	
		k=0;
		if( p->barrier){
			k++;
			q= QEPnewNode(mb,p);
		} else
		for( j=0; j< p->retc; j++)
		if( status[getArg(p,j)] == TOPNODE){
			q= vq[getArg(p,j)];
			k++;
			break;
		}
		if(k)
			QEPappend(qroot,q);
	}
	GDKfree(vq);
	GDKfree(status);
	return qroot;
}
/*
 * It may be handy to dump the graph for inspection
 * or to prepare for the dot program.
*/
static void
QEPdump(stream *f, QEP qep, int indent){
	int i,inc = 0;
	str s;
	if( qep->p){
		for(i=0;i<indent; i++) mnstr_printf(f," ");
		s= instruction2str(qep->mb, 0,qep->p, LIST_MAL_STMT | LIST_MAPI);
		mnstr_printf(f,"%s\n",s);
		GDKfree(s);
		inc = 4;
	}
	for(i=0; i< qep->climit; i++)
	if( qep->children[i])
		QEPdump(f,qep->children[i], indent+ inc);
}

int
OPTdumpQEPImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p){
	QEP qep;
	(void) cntxt;
	(void) stk;
	(void) p;

	qep= QEPbuild(mb);
	QEPdump(cntxt->fdout,qep,0);
	return 1;
}
