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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f opt_statistics
 * @a Martin Kersten
 * @v 0.1
 * @+ Optimizer Statistics
 * The optimizers statistics are collected in a small catalog.
 * It provides a basis for off-line analysis of their contribution
 * and a source of information of dynamic optimization decisions.
 *
 * The command is stored in the inspection module, because it
 * otherwise interferes with the general optimizer phase.
 */
#include "monetdb_config.h"
#include "opt_statistics.h"
/*
 * @
 * Upon loading the module it should inspect the scenario table
 * for any unresolved references to the MALoptimizer and set the
 * callback function.
 * A few global tables are maintained with optimizer statistics.
 * They provide the basis for dynamic optimization and offline analysis
 */
#define QOTnames	0
#define QOTcalls	1
#define QOTactions	2
#define QOTtimings	3

static BAT *qotStat[4] = { NULL };
static MT_Lock qotlock;

static BAT *
QOT_create(str hnme, str tnme, int ht, int tt)
{
	BAT *b;
	char buf[128];

	snprintf(buf, 128, "stat_%s_%s", hnme, tnme);
	b = BATdescriptor(BBPindex(buf));
	if (b)
		return b;

	b = BATnew(ht, tt, 256);
	if (b == NULL)
		return NULL;

	BATkey(b, TRUE);
	BBPrename(b->batCacheid, buf);
	BATmode(b, PERSISTENT);
	return b;
}

static void QOTstatisticsInit(void){
	oid o=0;
	int i;

	if (qotStat[QOTnames]) return;
	MT_lock_init(&qotlock,"QOT statistics");

	mal_set_lock(qotlock,"QOT statistics");
	qotStat[QOTnames]= QOT_create("opt","names",TYPE_void,TYPE_str);
	BATseqbase(qotStat[QOTnames],o);
	qotStat[QOTcalls]= QOT_create("opt","calls",TYPE_void,TYPE_int);
	BATseqbase(qotStat[QOTcalls],o);
	qotStat[QOTactions]= QOT_create("opt","actions",TYPE_void,TYPE_int);
	BATseqbase(qotStat[QOTactions],o);
	qotStat[QOTtimings]= QOT_create("opt","timings",TYPE_void,TYPE_lng);
	BATseqbase(qotStat[QOTtimings],o);

	/* recover from errors */
	for ( i=0; i<4; i++)
	if ( qotStat[i] == NULL){
		for (i= 0; i<4; i++){
			if (qotStat[i] )
				BBPclear(qotStat[i]->batCacheid);
				qotStat[i] = NULL;
		}
		mal_unset_lock(qotlock,"QOT statistics");
		return;
	}
	mal_unset_lock(qotlock,"QOT statistics");
	/* save them at least once */
	QOTstatisticsExit();
}

void
QOTupdateStatistics(str nme, int actions, lng val)
{
	BATiter bi;
	BUN p;
	oid idx;
	int ival=0, *ip= &ival;
	lng lval=0, *lp= &lval;

	QOTstatisticsInit();
	mal_set_lock(qotlock,"QOT statistics");
	p = BUNfnd(BATmirror(qotStat[QOTnames]),(ptr)nme);
	if (p == BUN_NONE) {
		BUNappend(qotStat[QOTnames], nme, FALSE);
		BUNappend(qotStat[QOTcalls],  &ival, FALSE);
		BUNappend(qotStat[QOTactions], &ival, FALSE);
		BUNappend(qotStat[QOTtimings], &lval, FALSE);
		p = BUNfnd(BATmirror(qotStat[QOTnames]),(ptr)nme);
		if (p == BUN_NONE){
			mal_unset_lock(qotlock,"QOT statistics");
			return;
		}
	}
	bi = bat_iterator(qotStat[QOTnames]);
	idx = *(oid*) BUNhead(bi,p);

	p = BUNfnd(qotStat[QOTcalls],&idx);
	if (p == BUN_NONE) {
#ifdef _Q_STATISTICS_DEBUG
		mnstr_printf(GDKout,"#Could not access 'calls'\n");
#endif
		mal_unset_lock(qotlock,"QOT statistics");
		return;
	}
	bi = bat_iterator(qotStat[QOTcalls]);
	ip = (int*) BUNtail(bi,p);
	*ip = *ip+1;

	p = BUNfnd(qotStat[QOTactions],&idx);
	if (p == BUN_NONE){
#ifdef _Q_STATISTICS_DEBUG
		mnstr_printf(GDKout,"#Could not access 'actions'\n");
#endif
		mal_unset_lock(qotlock,"QOT statistics");
		return;
	}
	bi = bat_iterator(qotStat[QOTactions]);
	ip = (int*) BUNtail(bi,p);
	*ip = *ip+ actions;

	p = BUNfnd(qotStat[QOTtimings],&idx);
	if (p == BUN_NONE){
#ifdef _Q_STATISTICS_DEBUG
		mnstr_printf(GDKout, "#Could not access 'timings'\n");
#endif
		mal_unset_lock(qotlock,"QOT statistics");
		return ;
	}
	bi = bat_iterator(qotStat[QOTtimings]);
	lp = (lng*) BUNtail(bi,p);
	*lp = *lp+ val;
	mal_unset_lock(qotlock,"QOT statistics");
}

void
QOTstatisticsExit(void)
{
	bat names[5];

	if( qotStat[QOTnames] == NULL)
		return;
	mal_set_lock(qotlock,"QOT statistics");
	names[0] = 0;
	names[1] = ABS(qotStat[QOTnames]->batCacheid);
	names[2] = ABS(qotStat[QOTcalls]->batCacheid);
	names[3] = ABS(qotStat[QOTactions]->batCacheid);
	names[4] = ABS(qotStat[QOTtimings]->batCacheid);

	TMsubcommit_list(names, 5);
	mal_unset_lock(qotlock,"QOT statistics");
}

static int
QOTindex(str nme)
{
	if( nme == 0) return -1;
	if(strcmp(nme,"names") == 0) return QOTnames;
	if(strcmp(nme,"calls") == 0) return QOTcalls;
	if(strcmp(nme,"actions") == 0) return QOTactions;
	if(strcmp(nme,"timings") == 0) return QOTtimings;
	return -1;
}

str
QOTgetStatistics(int *ret, str *nme)
{
	int idx;

	QOTstatisticsInit();
	if( qotStat[QOTnames] == NULL)
		throw(ILLARG,"optimizer.getStatistics",RUNTIME_OBJECT_MISSING);
	idx= QOTindex(*nme);
	if( idx <  0 || qotStat[idx] == 0 )
		throw(ILLARG,"optimizer.getStatistics",RUNTIME_OBJECT_MISSING);
	BBPincref(*ret= qotStat[idx]->batCacheid, TRUE);
	return MAL_SUCCEED;
}

