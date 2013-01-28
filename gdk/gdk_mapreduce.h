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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _GDK_MAPREDUCE_H_
#define _GDK_MAPREDUCE_H_

typedef struct {
	MT_Sema *sema;			/* micro scheduler handle */
	void (*cmd) (void *);		/* the function to be executed */
} MRtask;

gdk_export void MRschedule(int taskcnt, void **arg, void (*cmd) (void *p));

#endif /* _GDK_MAPREDUCE_H_ */
