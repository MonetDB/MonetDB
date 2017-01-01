/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
static MT_Lock qotlock MT_LOCK_INITIALIZER("qotlock");

static BAT *
QOT_create(str hnme, str tnme, int tt)
{
	BAT *b;
	char buf[128];

	snprintf(buf, 128, "stat_%s_%s", hnme, tnme);
	b = BATdescriptor(BBPindex(buf));
	if (b)
		return b;

	b = COLnew(0, tt, 256, PERSISTENT);
	if (b == NULL)
		return NULL;

	BBPrename(b->batCacheid, buf);
	BATmode(b, PERSISTENT);
	return b;
}

static void QOTstatisticsSave(void) {
	bat names[5];

	if( qotStat[QOTnames] == NULL)
		return;
	MT_lock_set(&qotlock);
	names[0] = 0;
	names[1] = qotStat[QOTnames]->batCacheid;
	names[2] = qotStat[QOTcalls]->batCacheid;
	names[3] = qotStat[QOTactions]->batCacheid;
	names[4] = qotStat[QOTtimings]->batCacheid;

	TMsubcommit_list(names, 5);
	MT_lock_unset(&qotlock);
}

static void QOTstatisticsInit(void){
	int i,j;

	if (qotStat[QOTnames]) return;
#ifdef NEED_MT_LOCK_INIT
	MT_lock_init(&qotlock,"QOT statistics");
#endif

	MT_lock_set(&qotlock);
	qotStat[QOTnames]= QOT_create("opt","names",TYPE_str);
	qotStat[QOTcalls]= QOT_create("opt","calls",TYPE_int);
	qotStat[QOTactions]= QOT_create("opt","actions",TYPE_int);
	qotStat[QOTtimings]= QOT_create("opt","timings",TYPE_lng);

	/* recover from errors */
	for ( i=0; i<4; i++)
	if ( qotStat[i] == NULL){
		for (j= 0; j < i; j++)
			BBPclear(qotStat[j]->batCacheid);
		MT_lock_unset(&qotlock);
		return;
	}
	MT_lock_unset(&qotlock);
	/* save them at least once */
	QOTstatisticsSave();
}

void
QOTupdateStatistics(str nme, int actions, lng val)
{
	BATiter bi;
	BUN p;
	int ival=0, *ip= &ival;
	lng lval=0, *lp= &lval;

	QOTstatisticsInit();
	MT_lock_set(&qotlock);
	p = BUNfnd(qotStat[QOTnames], nme);
	if (p == BUN_NONE) {
		BUNappend(qotStat[QOTnames], nme, FALSE);
		BUNappend(qotStat[QOTcalls],  &ival, FALSE);
		BUNappend(qotStat[QOTactions], &ival, FALSE);
		BUNappend(qotStat[QOTtimings], &lval, FALSE);
		p = BUNfnd(qotStat[QOTnames], nme);
		if (p == BUN_NONE){
			MT_lock_unset(&qotlock);
			return;
		}
	}

	bi = bat_iterator(qotStat[QOTcalls]);
	ip = (int*) BUNtail(bi, p);
	*ip = *ip+1;
	bi.b->tsorted = bi.b->trevsorted = 0;
	bi.b->tkey = 0;

	bi = bat_iterator(qotStat[QOTactions]);
	ip = (int*) BUNtail(bi, p);
	*ip = *ip+ actions;
	bi.b->tsorted = bi.b->trevsorted = 0;
	bi.b->tkey = 0;

	bi = bat_iterator(qotStat[QOTtimings]);
	lp = (lng*) BUNtail(bi, p);
	*lp = *lp+ val;
	bi.b->tsorted = bi.b->trevsorted = 0;
	bi.b->tkey = 0;
	MT_lock_unset(&qotlock);
}

void
QOTstatisticsExit(void)
{
	QOTstatisticsSave();
	qotStat[QOTnames] = NULL;
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
QOTgetStatistics(bat *ret, str *nme)
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

