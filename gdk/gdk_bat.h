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

#ifndef _GDK_BAT_H_
#define _GDK_BAT_H_

gdk_export BUN void_replace_bat(BAT *b, BAT *u, bit force);
gdk_export int void_inplace(BAT *b, oid id, const void *val, bit force);
gdk_export BAT *BATattach(int tt, const char *heapfile);

extern int default_ident(char *s);
extern oid MAXoid(BAT *i);

#endif /* _GDK_BAT_H_ */
/*
 * @+ BAT Unit Manipulation
 * Binary units (tuples) are the elements stored in BATs. We
 * discuss here BUN insert, replace and delete.
 * Below are help macros that actually move the BUNs
 * around and adapt search accelerator structures.
 */
#define hashins(h,i,v,n) HASHins_any(h,i,v)
#define hashdel(h,i,v,n) HASHdel(h,i,v,n)

/*
 * @+ BAT permissions, persistency and memory mapped heaps
 * The way large heaps are memory mapped is dependent both on the BAT
 * persistency status (persistent or not) as well as their update
 * permissions (readonly,append-only,writable).
 *
 * Let us recall the two main memory mapped file modes used to store
 * heaps:
 * @multitable @columnfractions .12 .8
 * @item STORE_MMAP
 * @tab files must be readonly, because you never know the exact saved status.
 *       HEAPsave consists of the rather efficient msync(X).
 * @item STORE_PRIV
 * @tab files modify pages in swap area, and can be writable.
 *       HEAPsave actually does a full write(X.new), while the mapped
 *       file stays in X
 * @end multitable
 * Notice that PRIV storage is only required for persistent BATs that
 * are already committed on disk. The crash-consistent state of
 * transient BATs is irrelevant as they disappear after a crash. Even
 * the crash-consistency of persistent BATs that did not make their
 * first commit is not relevant as they also will disappear.
 *
 * Also, some heaps may be in use with STORE_MMAP even if they are
 * appendable, as we suppose our code is bug-free and we know we won't
 * modify the already committed parts of the mapped file pages. For
 * string-heaps append-bats may mmap the heap if doubles are not being
 * eliminated anymore (i.e. when the contents of the builtin hash
 * table at the start of the string heap are not crucial anymore).
 */
#define ATOMappendpriv(t,h) ((BATatoms[t].atomHeapCheck != HEAP_check || !HEAP_mmappable(h)) && \
			     (ATOMstorage(t) != TYPE_str || GDK_ELIMDOUBLES(h)))
