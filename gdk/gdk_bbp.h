/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _GDK_BBP_H_
#define _GDK_BBP_H_

#define BBPLOADED	1	/* set if bat in memory */
#define BBPSWAPPED	2	/* set if dirty bat is not in memory */
#define BBPTMP		4	/* set if non-persistent bat has image on disk */

/* These 4 symbols indicate what the persistence state is of a bat.
 * - If the bat was persistent at the last commit (or at startup
 *   before the first commit), BBPEXISTING or BBPDELETED is set.
 * - If the bat is to be persistent after the next commit, BBPEXISTING
 *   or BBPNEW is set (i.e. (status&BBPPERSISTENT) != 0).
 * - If the bat was transient at the last commit (or didn't exist),
 *   BBPNEW is set, or none of these flag values is set.
 * - If the bat is to be transient at the next commit, BBPDELETED is
 *   set, or none of these flag values is set.
 * BATmode() switches between BBPDELETED and BBPEXISTING (bat was
 * persistent at last commit), or between BBPNEW and 0 (bat was
 * transient or didn't exist at last commit).
 * Committing a bat switches from BBPNEW to BBPEXISTING, or turns off
 * BBPDELETED.
 * In any case, only at most one of BBPDELETED, BBPEXISTING, and
 * BBPNEW may be set at any one time.
 *
 * In short,
 * BBPEXISTING -- bat was and should remain persistent;
 * BBPDELETED -- bat was persistent at last commit and should be transient;
 * BBPNEW -- bat was transient at last commit and should be persistent;
 * none of the above -- bat was and should remain transient.
 */
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
#define BBPHOT		4096	/* bat is "hot", i.e. is still in active use */
#define BBPSYNCING	8192	/* bat between creating backup and saving */

#define BBPUNSTABLE	(BBPUNLOADING|BBPDELETING)	/* set while we are unloading */
#define BBPWAITING      (BBPUNLOADING|BBPLOADING|BBPSAVING|BBPDELETING|BBPSYNCING)

#define BBPTRIM_ALL	(((size_t)1) << (sizeof(size_t)*8 - 2))	/* very large positive size_t */

gdk_export bat getBBPsize(void); /* current occupied size of BBP array */
gdk_export lng getBBPlogno(void); /* two lng of extra info in BBP.dir */
gdk_export lng getBBPtransid(void);

/* global calls */
gdk_export gdk_return BBPaddfarm(const char *dirname, uint32_t rolemask, bool logerror);

/* update interface */
gdk_export int BBPreclaim(BAT *b);
gdk_export gdk_return BBPsave(BAT *b);
gdk_export int BBPrename(bat bid, const char *nme);

/* query interface */
gdk_export bat BBPindex(const char *nme);
gdk_export BAT *BBPdescriptor(bat b);

/* swapping interface */
gdk_export gdk_return BBPsync(int cnt, bat *restrict subcommit, BUN *restrict sizes, lng logno, lng transid);
gdk_export int BBPfix(bat b);
gdk_export int BBPunfix(bat b);
gdk_export int BBPretain(bat b);
gdk_export int BBPrelease(bat b);
gdk_export void BBPkeepref(bat i);
gdk_export void BBPshare(bat b);
gdk_export void BBPcold(bat i);

#define BBP_status_set(bid, mode)			\
	ATOMIC_SET(&BBP_record(bid).status, mode)

#define BBP_status_on(bid, flags)			\
	ATOMIC_OR(&BBP_record(bid).status, flags)

#define BBP_status_off(bid, flags)			\
	ATOMIC_AND(&BBP_record(bid).status, ~(flags))

#define BBPswappable(b) ((b) && (b)->batCacheid && BBP_refs((b)->batCacheid) == 0)
#define BBPtrimmable(b) (BBPswappable(b) && isVIEW(b) == 0 && (BBP_status((b)->batCacheid)&BBPWAITING) == 0)

#endif /* _GDK_BBP_H_ */
