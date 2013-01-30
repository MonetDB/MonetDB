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

/* This file provides interfaces to perform certain atomic operations
 * on variables.  Atomic in this sense means that an operation
 * performed in one thread shows up in another thread either
 * completely or not at all.
 *
 * If the symbol ATOMIC_LOCK is defined, a variable of type MT_Lock
 * must be declared and initialized.  The latter can be done using the
 * macro ATOMIC_INIT which expands to nothing if ATOMIC_LOCK is not
 * defined.
 *
 * The following operations are defined:
 * ATOMIC_GET -- return the value of a variable;
 * ATOMIC_SET -- set the value of a variable;
 * ATOMIC_ADD -- add a value to a variable, return original value;
 * ATOMIC_SUB -- subtract a value from a variable, return original value;
 * ATOMIC_INT -- increment a variable's value, return new value;
 * ATOMIC_DEC -- decrement a variable's value, return new value;
 * ATOMIC_CAS -- compare-and-set: compare the variable's value with
 *               old and if it matches, replace with new, return
 *               original value.
 * As written, these interfaces work on variables of type ATOMIC_TYPE
 * (int or lng depending on architecture).  There are also versions of
 * these interfaces specifically for int, and on 64-bit architectures,
 * for lng.  In addition, all but add and sub are also defined for sht
 * (Windows restriction).  To get the type-specific interface, append
 * _sht, _int, or _lng to the above names.
 */

#ifndef _GDK_ATOMIC_H_
#define _GDK_ATOMIC_H_

#if defined(_MSC_VER)

#include <intrin.h>

#define ATOMIC_GET_sht(var, lck, fcn)		var
#define ATOMIC_SET_sht(var, val, lck, fcn)	(var = (val))
#define ATOMIC_INC_sht(var, lck, fcn)		_InterlockedIncrement16(&(var))
#define ATOMIC_DEC_sht(var, lck, fcn)		_InterlockedDecrement16(&(var))
#define ATOMIC_CAS_sht(var, old, new, lck, fcn)	_InterlockedCompareExchange16(&(var), new, old)

#pragma intrinsic(_InterlockedIncrement16)
#pragma intrinsic(_InterlockedDecrement16)
#pragma intrinsic(_InterlockedCompareExchange16)

#define ATOMIC_GET_int(var, lck, fcn)		var
#define ATOMIC_SET_int(var, val, lck, fcn)	(var = (val))
#define ATOMIC_ADD_int(var, val, lck, fcn)	_InterlockedExchangeAdd(&(var), (val))
#define ATOMIC_SUB_int(var, val, lck, fcn)	_InterlockedExchangeAdd(&(var), -(val))
#define ATOMIC_INC_int(var, lck, fcn)		_InterlockedIncrement(&(var))
#define ATOMIC_DEC_int(var, lck, fcn)		_InterlockedDecrement(&(var))
#define ATOMIC_CAS_int(var, old, new, lck, fcn)	_InterlockedCompareExchange(&(var), new, old)

#pragma intrinsic(_InterlockedExchangeAdd)
#pragma intrinsic(_InterlockedIncrement)
#pragma intrinsic(_InterlockedDecrement)
#pragma intrinsic(_InterlockedCompareExchange)

#if SIZEOF_SSIZE_T == SIZEOF_LNG
#define ATOMIC_GET_lng(var, lck, fcn)		var
#define ATOMIC_SET_lng(var, val, lck, fcn)	(var = (val))
#define ATOMIC_ADD_lng(var, val, lck, fcn)	_InterlockedExchangeAdd64(&(var), val)
#define ATOMIC_SUB_lng(var, val, lck, fcn)	_InterlockedExchangeAdd64(&(var), -(val))
#define ATOMIC_INC_lng(var, lck, fcn)		_InterlockedIncrement64(&(var))
#define ATOMIC_DEC_lng(var, lck, fcn)		_InterlockedDecrement64(&(var))
#define ATOMIC_CAS_lng(var, old, new, lck, fcn)	_InterlockedCompareExchange64(&(var), new, old)

#pragma intrinsic(_InterlockedExchangeAdd64)
#pragma intrinsic(_InterlockedIncrement64)
#pragma intrinsic(_InterlockedDecrement64)
#pragma intrinsic(_InterlockedCompareExchange64)
#endif

#define ATOMIC_INIT(lck, fcn)	((void) 0)

#elif defined(__GNUC__) || defined(__INTEL_COMPILER)

#define ATOMIC_GET_sht(var, lck, fcn)		var
#define ATOMIC_SET_sht(var, val, lck, fcn)	(var = (val))
#define ATOMIC_INC_sht(var, lck, fcn)		__sync_add_and_fetch(&(var), 1)
#define ATOMIC_DEC_sht(var, lck, fcn)		__sync_sub_and_fetch(&(var), 1)
#define ATOMIC_CAS_sht(var, old, new, lck, fcn)	__sync_val_compare_and_swap(&(var), old, new)

#define ATOMIC_GET_int(var, lck, fcn)		var
#define ATOMIC_SET_int(var, val, lck, fcn)	(var = (val))
#define ATOMIC_ADD_int(var, val, lck, fcn)	__sync_fetch_and_add(&(var), (val))
#define ATOMIC_SUB_int(var, val, lck, fcn)	__sync_fetch_and_sub(&(var), (val))
#define ATOMIC_INC_int(var, lck, fcn)		__sync_add_and_fetch(&(var), 1)
#define ATOMIC_DEC_int(var, lck, fcn)		__sync_sub_and_fetch(&(var), 1)
#define ATOMIC_CAS_int(var, old, new, lck, fcn)	__sync_val_compare_and_swap(&(var), old, new)

#if SIZEOF_SSIZE_T == SIZEOF_LNG
#define ATOMIC_GET_lng(var, lck, fcn)		var
#define ATOMIC_SET_lng(var, val, lck, fcn)	(var = (val))
#define ATOMIC_ADD_lng(var, val, lck, fcn)	__sync_fetch_and_add(&(var), (val))
#define ATOMIC_SUB_lng(var, val, lck, fcn)	__sync_fetch_and_sub(&(var), (val))
#define ATOMIC_INC_lng(var, lck, fcn)		__sync_add_and_fetch(&(var), 1)
#define ATOMIC_DEC_lng(var, lck, fcn)		__sync_sub_and_fetch(&(var), 1)
#define ATOMIC_CAS_lng(var, old, new, lck, fcn)	__sync_val_compare_and_swap(&(var), old, new)
#endif

#define ATOMIC_INIT(lck, fcn)	((void) 0)

#else

static inline short
__ATOMIC_GET_sht(volatile short *var, pthread_mutex_t *lck)
{
	short old;
	pthread_mutex_lock(lck);
	old = *var;
	pthread_mutex_unlock(lck);
	return old;
}
#define ATOMIC_GET_sht(var, lck, fcn)	__ATOMIC_GET_sht(&(var), &(lck))

static inline short
__ATOMIC_SET_sht(volatile short *var, short val, pthread_mutex_t *lck)
{
	short new;
	pthread_mutex_lock(lck);
	*var = val;
	new = *var;
	pthread_mutex_unlock(lck);
	return new;
}
#define ATOMIC_SET_sht(var, val, lck, fcn)	__ATOMIC_SET_sht(&(var), (val), &(lck))

static inline short
__ATOMIC_INC_sht(volatile short *var, pthread_mutex_t *lck)
{
	short new;
	pthread_mutex_lock(lck);
	new = ++*var;
	pthread_mutex_unlock(lck);
	return new;
}
#define ATOMIC_INC_sht(var, lck, fcn)		__ATOMIC_INC_sht(&(var), &(lck))

static inline short
__ATOMIC_DEC_sht(volatile short *var, pthread_mutex_t *lck)
{
	short new;
	pthread_mutex_lock(lck);
	new = --*var;
	pthread_mutex_unlock(lck);
	return new;
}
#define ATOMIC_DEC_sht(var, lck, fcn)		__ATOMIC_DEC_sht(&(var), &(lck))

static inline short
__ATOMIC_CAS_sht(volatile short *var, short old, short new, pthread_mutex_t *lck)
{
	short orig;
	pthread_mutex_lock(lck);
	orig = *var;
	if (*var == old)
		*var = new;
	pthread_mutex_unlock(lck);
	return orig;
}
#define ATOMIC_CAS_sht(var, old, new, lck, fcn)	__ATOMIC_CAS_sht(&(var), (old), (new), &(lck))


static inline int
__ATOMIC_GET_int(volatile int *var, pthread_mutex_t *lck)
{
	int old;
	pthread_mutex_lock(lck);
	old = *var;
	pthread_mutex_unlock(lck);
	return old;
}
#define ATOMIC_GET_int(var, lck, fcn)	__ATOMIC_GET_int(&(var), &(lck))

static inline int
__ATOMIC_SET_int(volatile int *var, int val, pthread_mutex_t *lck)
{
	int new;
	pthread_mutex_lock(lck);
	*var = val;
	new = *var;
	pthread_mutex_unlock(lck);
	return new;
}
#define ATOMIC_SET_int(var, val, lck, fcn)	__ATOMIC_SET_int(&(var), (val), &(lck))

static inline int
__ATOMIC_ADD_int(volatile int *var, int val, pthread_mutex_t *lck)
{
	int old;
	pthread_mutex_lock(lck);
	old = *var;
	*var += val;
	pthread_mutex_unlock(lck);
	return old;
}
#define ATOMIC_ADD_int(var, val, lck, fcn)	__ATOMIC_ADD_int(&(var), (val), &(lck))

static inline int
__ATOMIC_SUB_int(volatile int *var, int val, pthread_mutex_t *lck)
{
	int old;
	pthread_mutex_lock(lck);
	old = *var;
	*var -= val;
	pthread_mutex_unlock(lck);
	return old;
}
#define ATOMIC_SUB_int(var, val, lck, fcn)	__ATOMIC_SUB_int(&(var), (val), &(lck))

static inline int
__ATOMIC_INC_int(volatile int *var, pthread_mutex_t *lck)
{
	int new;
	pthread_mutex_lock(lck);
	new = ++*var;
	pthread_mutex_unlock(lck);
	return new;
}
#define ATOMIC_INC_int(var, lck, fcn)		__ATOMIC_INC_int(&(var), &(lck))

static inline int
__ATOMIC_DEC_int(volatile int *var, pthread_mutex_t *lck)
{
	int new;
	pthread_mutex_lock(lck);
	new = --*var;
	pthread_mutex_unlock(lck);
	return new;
}
#define ATOMIC_DEC_int(var, lck, fcn)		__ATOMIC_DEC_int(&(var), &(lck))

static inline int
__ATOMIC_CAS_int(volatile int *var, int old, int new, pthread_mutex_t *lck)
{
	int orig;
	pthread_mutex_lock(lck);
	orig = *var;
	if (*var == old)
		*var = new;
	pthread_mutex_unlock(lck);
	return orig;
}
#define ATOMIC_CAS_int(var, old, new, lck, fcn)	__ATOMIC_CAS_int(&(var), (old), (new), &(lck))

#if SIZEOF_SSIZE_T == SIZEOF_LNG

static inline lng
__ATOMIC_GET_lng(volatile lng *var, pthread_mutex_t *lck)
{
	lng old;
	pthread_mutex_lock(lck);
	old = *var;
	pthread_mutex_unlock(lck);
	return old;
}
#define ATOMIC_GET_lng(var, lck, fcn)	__ATOMIC_GET_lng(&(var), &(lck))

static inline lng
__ATOMIC_SET_lng(volatile lng *var, lng val, pthread_mutex_t *lck)
{
	lng new;
	pthread_mutex_lock(lck);
	*var = val;
	new = *var;
	pthread_mutex_unlock(lck);
	return new;
}
#define ATOMIC_SET_lng(var, val, lck, fcn)	__ATOMIC_SET_lng(&(var), (val), &(lck))

static inline lng
__ATOMIC_ADD_lng(volatile lng *var, lng val, pthread_mutex_t *lck)
{
	lng old;
	pthread_mutex_lock(lck);
	old = *var;
	*var += val;
	pthread_mutex_unlock(lck);
	return old;
}
#define ATOMIC_ADD_lng(var, val, lck, fcn)	__ATOMIC_ADD_lng(&(var), (val), &(lck))

static inline lng
__ATOMIC_SUB_lng(volatile lng *var, lng val, pthread_mutex_t *lck)
{
	lng old;
	pthread_mutex_lock(lck);
	old = *var;
	*var -= val;
	pthread_mutex_unlock(lck);
	return old;
}
#define ATOMIC_SUB_lng(var, val, lck, fcn)	__ATOMIC_SUB_lng(&(var), (val), &(lck))

static inline lng
__ATOMIC_INC_lng(volatile lng *var, pthread_mutex_t *lck)
{
	lng new;
	pthread_mutex_lock(lck);
	new = ++*var;
	pthread_mutex_unlock(lck);
	return new;
}
#define ATOMIC_INC_lng(var, lck, fcn)		__ATOMIC_INC_lng(&(var), &(lck))

static inline lng
__ATOMIC_DEC_lng(volatile lng *var, pthread_mutex_t *lck)
{
	lng new;
	pthread_mutex_lock(lck);
	new = --*var;
	pthread_mutex_unlock(lck);
	return new;
}
#define ATOMIC_DEC_lng(var, lck, fcn)		__ATOMIC_DEC_lng(&(var), &(lck))

static inline lng
__ATOMIC_CAS_lng(volatile lng *var, lng old, lng new, pthread_mutex_t *lck)
{
	lng orig;
	pthread_mutex_lock(lck);
	orig = *var;
	if (*var == old)
		*var = new;
	pthread_mutex_unlock(lck);
	return orig;
}
#define ATOMIC_CAS_lng(var, old, new, lck, fcn)	__ATOMIC_CAS_lng(&(var), (old), (new), &(lck))

#endif

#define ATOMIC_LOCK		/* must use locks */
#define ATOMIC_INIT(lck, fcn)	MT_lock_init(&(lck), fcn)

#endif

#if SIZEOF_SSIZE_T == SIZEOF_LNG

#define ATOMIC_TYPE		lng
#define ATOMIC_GET(var, lck, fcn)		ATOMIC_GET_lng(var, lck, fcn)
#define ATOMIC_SET(var, val, lck, fcn)		ATOMIC_SET_lng(var, val, lck, fcn)
#define ATOMIC_ADD(var, val, lck, fcn)		ATOMIC_ADD_lng(var, val, lck, fcn)
#define ATOMIC_SUB(var, val, lck, fcn)		ATOMIC_SUB_lng(var, val, lck, fcn)
#define ATOMIC_INC(var, lck, fcn)		ATOMIC_INC_lng(var, lck, fcn)
#define ATOMIC_DEC(var, lck, fcn)		ATOMIC_DEC_lng(var, lck, fcn)
#define ATOMIC_CAS(var, old, new, lck, fcn)	ATOMIC_CAS_lng(var, old, new, lck, fcn)

#else

#define ATOMIC_TYPE		int
#define ATOMIC_GET(var, lck, fcn)		ATOMIC_GET_int(var, lck, fcn)
#define ATOMIC_SET(var, val, lck, fcn)		ATOMIC_SET_int(var, val, lck, fcn)
#define ATOMIC_ADD(var, val, lck, fcn)		ATOMIC_ADD_int(var, val, lck, fcn)
#define ATOMIC_SUB(var, val, lck, fcn)		ATOMIC_SUB_int(var, val, lck, fcn)
#define ATOMIC_INC(var, lck, fcn)		ATOMIC_INC_int(var, lck, fcn)
#define ATOMIC_DEC(var, lck, fcn)		ATOMIC_DEC_int(var, lck, fcn)
#define ATOMIC_CAS(var, old, new, lck, fcn)	ATOMIC_CAS_int(var, old, new, lck, fcn)

#endif

#endif	/* _GDK_ATOMIC_H_ */
