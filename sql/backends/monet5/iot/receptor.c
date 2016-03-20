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
 * Copyright August 2008-2016 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * Martin Kersten
 * Receptor thread
 */

#include "monetdb_config.h"
#include "receptor.h"
#include <sys/stat.h>

#define _DEBUG_IOT_ if(1)
static str statusname[6] = { "<unknown>", "init", "paused", "running", "stop", "error" };

static Receptor rcAnchor = NULL;

Receptor
RCnew(Basket bskt)
{
	Receptor rc;
	char buf[BUFSIZ];

	rc = (Receptor) GDKzalloc(sizeof(RCrecord));
	if (rc == 0)
		return 0;
	// initialize the basket container director
	(void) mkdir("baskets", 0755);
	snprintf(buf,BUFSIZ,"baskets/primary");
	(void) mkdir(buf, 0755);
	snprintf(buf,BUFSIZ,"baskets/primary%c%s",DIR_SEP,bskt->schema);
	(void) mkdir(buf, 0755);
	snprintf(buf,BUFSIZ,"baskets/primary%c%s%c%s%c",DIR_SEP,bskt->schema, DIR_SEP, bskt->table,DIR_SEP);
	(void) mkdir(buf, 0755);
	rc->primary= GDKstrdup(buf);

	snprintf(buf,BUFSIZ,"baskets/secondary");
	(void) mkdir(buf, 0755);
	snprintf(buf,BUFSIZ,"baskets/secondary%c%s",DIR_SEP,bskt->schema);
	(void) mkdir(buf, 0755);
	snprintf(buf,BUFSIZ,"baskets/secondary%c%s%c%s%c",DIR_SEP,bskt->schema, DIR_SEP, bskt->table,DIR_SEP);
	(void) mkdir(buf, 0755);
	rc->secondary= GDKstrdup(buf);

	rc->basket = bskt;
	rc->status=0;
	if (rcAnchor)
		rcAnchor->prv = rc;
	rc->nxt = rcAnchor;
	rc->prv = NULL;
	rcAnchor = rc;
	return rc;
}

str
RCstart(int idx)
{
	(void) RCnew(baskets + idx);
	return 0;
}

str
RCpause(int idx)
{
	(void) idx;
	return 0;
}

str
RCresume(int idx)
{
	(void) idx;
	return 0;
}

str
RCstop(int idx)
{
	(void) idx;
	return 0;
}

/*
 * The receptor thread manages all baskets in round robin fashion.
 */
str
RCreceptor(Receptor rc)
{
	assert(rc);
	_DEBUG_IOT_ fprintf(stderr, "#Start receptor thread starts\n");

	while (rc && rc->status != BSKTSTOP) {
		MT_sleep_ms(5000);
	}
	MT_join_thread(rc->pid);
	return MAL_SUCCEED;
}

str
RCdump(void *ret)
{
	Receptor rc = rcAnchor;
	(void) ret;
	for (; rc; rc = rc->nxt)
		mnstr_printf(GDKout, "#receptor %s %s status=%s \n", rc->primary, rc->secondary, statusname[rc->status]);
	return MAL_SUCCEED;
}

/* provide a tabular view for inspection */
str
RCtable( bat *primaryId, bat *secondaryId, bat *statusId)
{
	BAT *primary = NULL, *secondary = NULL, *status = NULL;
	Receptor rc = rcAnchor;

	primary = BATnew(TYPE_void, TYPE_str, BATTINY, TRANSIENT);
	if (primary == 0)
		goto wrapup;
	BATseqbase(primary, 0);
	secondary = BATnew(TYPE_void, TYPE_str, BATTINY, TRANSIENT);
	if (secondary == 0)
		goto wrapup;
	BATseqbase(secondary, 0);
	status = BATnew(TYPE_void, TYPE_str, BATTINY, TRANSIENT);
	if (status == 0)
		goto wrapup;
	BATseqbase(status, 0);

	for (; rc; rc = rc->nxt){
		BUNappend(primary, rc->primary, FALSE);
		BUNappend(secondary, rc->secondary, FALSE);
		BUNappend(status, statusname[rc->status], FALSE);
	}

	BBPkeepref(*primaryId = primary->batCacheid);
	BBPkeepref(*secondaryId = secondary->batCacheid);
	BBPkeepref(*statusId = status->batCacheid);
	return MAL_SUCCEED;
wrapup:
	if (primary)
		BBPunfix(primary->batCacheid);
	if (secondary)
		BBPunfix(secondary->batCacheid);
	if (status)
		BBPunfix(status->batCacheid);
	throw(MAL, "receptor.baskets", MAL_MALLOC_FAIL);
}
