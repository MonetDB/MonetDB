/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#ifndef _GDK_MAPREDUCE_H_
#define _GDK_MAPREDUCE_H_

gdk_export void MRschedule(int taskcnt, void **arg, void (*cmd) (void *p));

#endif /* _GDK_MAPREDUCE_H_ */
