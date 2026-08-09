/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _REL_PROP_H_
#define _REL_PROP_H_

sql_export prop * prop_create( allocator *sa, prop_kind kind, prop *pre );
extern prop * prop_copy( allocator *sa, prop *p);
extern prop * prop_remove(allocator *sa, prop *plist, prop *p);
extern prop * find_prop( prop *p, prop_kind kind);
extern void * find_prop_and_get(prop *p, prop_kind kind);
extern const char * propkind2string( prop *p);
extern char * propvalue2string(allocator *sa, prop *p);
extern void free_props(allocator *sa, prop *p);

#endif /* _REL_PROP_H_ */
