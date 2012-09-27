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

#ifndef _GDK_BBP_H_
#define _GDK_BBP_H_

#define BBPLOADED	1	/* set if bat in memory */
#define BBPSWAPPED	2	/* set if dirty bat is not in memory */
#define BBPTMP          4	/* set if non-persistent bat has image on disk */
#define BBPDELETED	16	/* set if bat persistent at last commit is now transient */
#define BBPEXISTING	32	/* set if bat was already persistent at end of last commit */
#define BBPNEW		64	/* set if bat has become persistent since last commit */
#define BBPPERSISTENT	(BBPEXISTING|BBPNEW)	/* mask for currently persistent bats */
#define BBPSTATUS	127

#define BBPUNLOADING	128	/* set while we are unloading */
#define BBPLOADING	256	/* set while we are loading */
#define BBPSAVING       512	/* set while we are saving */
#define BBPRENAMED	1024	/* set when bat is renamed in this transaction */
#define BBPDELETING	2048	/* set while we are deleting (special case in module unload) */
#define BBPUNSTABLE	(BBPUNLOADING|BBPDELETING)	/* set while we are unloading */
#define BBPWAITING      (BBPUNLOADING|BBPLOADING|BBPSAVING|BBPDELETING)

#define BBPTRIM_ALL	(((size_t)1) << (sizeof(size_t)*8 - 2))	/* very large positive size_t */
#define BBPLASTUSED(x)  ((x) & 0x7fffffff)	/* stamp is always a positive int */

gdk_export int BBPin;		/* BATs swapped into BBP  */
gdk_export int BBPout;		/* BATs swapped out of BBP */
gdk_export bat BBPsize;		/* current occupied size of BBP array */

/* global calls */
gdk_export int BBPdir(int cnt, bat *subcommit);

/* update interface */
gdk_export void BBPclear(bat bid);
gdk_export int BBPreclaim(BAT *b);
gdk_export int BBPsave(BAT *b);
gdk_export int BBPrename(bat bid, const char *nme);

/* query interface */
gdk_export bat BBPindex(const char *nme);
gdk_export BAT *BBPdescriptor(bat b);

/* swapping interface */
gdk_export int BBPsync(int cnt, bat *subcommit);
gdk_export int BBPincref(bat b, int logical);
gdk_export void BBPkeepref(bat i);
gdk_export void BBPreleaseref(bat i);
gdk_export int BBPdecref(bat b, int logical);
gdk_export void BBPshare(bat b);

/* (strncmp(s, "tmp_", 4) == 0 || strncmp(s, "tmpr_", 5) == 0) */
#define BBPtmpcheck(s) ((s)[0] == 't' && (s)[1] == 'm' && (s)[2] == 'p' && \
			((s)[3] == '_' || ((s)[3] == 'r' && (s)[4] == '_')))

#define BBP_status_set(bid, mode, nme)		\
	do {					\
		BBP_status(bid) = mode;		\
	} while (0)

#define BBP_status_on(bid, flags, nme)					\
		BBP_status_set(bid, BBP_status(bid) | flags, nme);

#define BBP_status_off(bid, flags, nme)					\
		BBP_status_set(bid, BBP_status(bid) & ~(flags), nme);

#define BBP_unload_inc(bid, nme)			\
	do {						\
		MT_lock_set(&GDKunloadLock, nme);	\
		BBPunloadCnt++;				\
		MT_lock_unset(&GDKunloadLock, nme);	\
	} while (0)

#define BBP_unload_dec(bid, nme)					\
	do {								\
		MT_lock_set(&GDKunloadLock, nme);			\
		if (--BBPunloadCnt == 0)				\
			MT_cond_signal(&GDKunloadCond, nme);		\
		assert(BBPunloadCnt >= 0);				\
		MT_lock_unset(&GDKunloadLock, nme);			\
	} while (0)
#define BBPswappable(b) ((b) && (b)->batCacheid && BBP_refs((b)->batCacheid) == 0)
#define BBPtrimmable(b) (BBPswappable(b) && isVIEW(b) == 0 && (BBP_status((b)->batCacheid)&BBPWAITING) == 0)

#endif /* _GDK_BBP_H_ */
