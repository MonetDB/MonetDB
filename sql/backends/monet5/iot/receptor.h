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
 * Copyright August 2008-2015 MonetDB B.V.
 * All Rights Reserved.
 */

/* author: M Kersten
 * The receptor thread watches over a specific directory where
 * binary copies of the event baskets are stored. 
 * Currently it simply periodicly checks for a changed basket
 * Or its timeout
 */
#ifndef _RECEPTOR_
#define _RECEPTOR_
#include "mal_interpreter.h"
#include "tablet.h"
#include "mtime.h"
#include "basket.h"

#define _DEBUG_RECEPTOR_

typedef struct RECEPTOR {
	MT_Id pid;
	str primary;		/* path to .../primary/schema/table/.basket */
	str secondary;		/* path to .../secondary/schema/table/.basket */
	Basket basket;   	/* foreign key to a basket */
	int status;
	struct RECEPTOR *prv,*nxt;
} RCrecord, *Receptor;

#ifdef WIN32
#ifndef LIBDATACELL
#define iot_export extern __declspec(dllimport)
#else
#define iot_export extern __declspec(dllexport)
#endif
#else
#define iot_export extern
#endif

iot_export str RCstart(int ret);
iot_export str RCpause(int ret);
iot_export str RCresume(int ret);
iot_export str RCstop(int ret);
iot_export str RCtable( bat *primaryId, bat *secondaryId, bat *statusId);
iot_export str RCdump(void *ret);
iot_export Receptor RCnew(Basket bskt);
iot_export str RCreceptor(Receptor rc);

#endif

