/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _MAL_ARGUMENTS_H
#define _MAL_ARGUMENTS_H

#include "mal_client.h"

#if !defined(NDEBUG) && defined(__GNUC__)
/* for ease of programming and debugging (assert reporting a useful
 * location), we use a GNU C extension to check the type of arguments,
 * and of course only when assertions are enabled */
#define getArgReference_TYPE(s, pci, k, TYPE)					\
	({															\
		assert((s)->stk[(pci)->argv[k]].vtype == TYPE_##TYPE);	\
		(TYPE *) getArgReference((s), (pci), (k));				\
	})
#define getArgReference_msk(s, pci, k)				\
	({												\
		ValRecord *v = &(s)->stk[(pci)->argv[k]];	\
		assert(v->vtype == TYPE_msk);				\
		&v->val.mval;								\
	})
#define getArgReference_bit(s, pci, k)				\
	({												\
		ValRecord *v = &(s)->stk[(pci)->argv[k]];	\
		assert(v->vtype == TYPE_bit);				\
		(bit *) &v->val.btval;						\
	})
#define getArgReference_sht(s, pci, k)				\
	({												\
		ValRecord *v = &(s)->stk[(pci)->argv[k]];	\
		assert(v->vtype == TYPE_sht);				\
		&v->val.shval;								\
	})
#define getArgReference_bat(s, pci, k)				\
	({												\
		ValRecord *v = &(s)->stk[(pci)->argv[k]];	\
		assert(v->bat);								\
		&v->val.bval;								\
	})
#define getArgReference_int(s, pci, k)				\
	({												\
		ValRecord *v = &(s)->stk[(pci)->argv[k]];	\
		assert(v->vtype == TYPE_int);				\
		&v->val.ival;								\
	})
#define getArgReference_bte(s, pci, k)				\
	({												\
		ValRecord *v = &(s)->stk[(pci)->argv[k]];	\
		assert(v->vtype == TYPE_bte);				\
		&v->val.btval;								\
	})
#define getArgReference_oid(s, pci, k)							\
	({															\
		ValRecord *v = &(s)->stk[(pci)->argv[k]];				\
		assert(v->vtype == TYPE_oid || v->vtype == TYPE_void);	\
		&v->val.oval;											\
	})
#define getArgReference_ptr(s, pci, k)				\
	({												\
		ValRecord *v = &(s)->stk[(pci)->argv[k]];	\
		assert(v->vtype == TYPE_ptr);				\
		&v->val.pval;								\
	})
#define getArgReference_flt(s, pci, k)				\
	({												\
		ValRecord *v = &(s)->stk[(pci)->argv[k]];	\
		assert(v->vtype == TYPE_flt);				\
		&v->val.fval;								\
	})
#define getArgReference_dbl(s, pci, k)				\
	({												\
		ValRecord *v = &(s)->stk[(pci)->argv[k]];	\
		assert(v->vtype == TYPE_dbl);				\
		&v->val.dval;								\
	})
#define getArgReference_lng(s, pci, k)				\
	({												\
		ValRecord *v = &(s)->stk[(pci)->argv[k]];	\
		assert(v->vtype == TYPE_lng);				\
		&v->val.lval;								\
	})
#ifdef HAVE_HGE
#define getArgReference_hge(s, pci, k)				\
	({												\
		ValRecord *v = &(s)->stk[(pci)->argv[k]];	\
		assert(v->vtype == TYPE_hge);				\
		&v->val.hval;								\
	})
#endif
#define getArgReference_str(s, pci, k)				\
	({												\
		ValRecord *v = &(s)->stk[(pci)->argv[k]];	\
		assert(v->vtype == TYPE_str);				\
		&v->val.sval;								\
	})
#else
#define getArgReference_TYPE(s, pci, k, TYPE)	((TYPE *) getArgReference(s, pci, k))
#define getArgReference_msk(s, pci, k)	(&(s)->stk[(pci)->argv[k]].val.mval)
#define getArgReference_bit(s, pci, k)	((bit *) &(s)->stk[(pci)->argv[k]].val.btval)
#define getArgReference_sht(s, pci, k)	(&(s)->stk[(pci)->argv[k]].val.shval)
#define getArgReference_bat(s, pci, k)	(&(s)->stk[(pci)->argv[k]].val.bval)
#define getArgReference_int(s, pci, k)	(&(s)->stk[(pci)->argv[k]].val.ival)
#define getArgReference_bte(s, pci, k)	(&(s)->stk[(pci)->argv[k]].val.btval)
#define getArgReference_oid(s, pci, k)	(&(s)->stk[(pci)->argv[k]].val.oval)
#define getArgReference_ptr(s, pci, k)	(&(s)->stk[(pci)->argv[k]].val.pval)
#define getArgReference_flt(s, pci, k)	(&(s)->stk[(pci)->argv[k]].val.fval)
#define getArgReference_dbl(s, pci, k)	(&(s)->stk[(pci)->argv[k]].val.dval)
#define getArgReference_lng(s, pci, k)	(&(s)->stk[(pci)->argv[k]].val.lval)
#ifdef HAVE_HGE
#define getArgReference_hge(s, pci, k)	(&(s)->stk[(pci)->argv[k]].val.hval)
#endif
#define getArgReference_str(s, pci, k)	(&(s)->stk[(pci)->argv[k]].val.sval)
#endif

#endif
