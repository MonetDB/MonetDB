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

#ifndef _GDK_ATOMIC_H_
#define _GDK_ATOMIC_H_

#if defined(__GNUC__)
#if SIZEOF_SSIZE_T == SIZEOF_LONG_LONG
#define ATOMIC_TYPE		long long
#else
#define ATOMIC_TYPE		long
#endif
#define ATOMIC_ADD(var, val)	__sync_fetch_and_add(&var, (ATOMIC_TYPE) (val))
#define ATOMIC_INC(var)		__sync_add_and_fetch(&var, (ATOMIC_TYPE) 1)
#define ATOMIC_SUB(var, val)	__sync_fetch_and_sub(&var, (ATOMIC_TYPE) (val))
#define ATOMIC_DEC(var)		__sync_sub_and_fetch(&var, (ATOMIC_TYPE) 1)
#define ATOMIC_START(lock, func)
#define ATOMIC_END(lock, func)
#define ATOMIC_INIT(lock, func)
#define ATOMIC_COMP_SWAP(var, old, new, lock, func)	\
	__sync_val_compare_and_swap(&var, old, new)
#define ATOMIC_GET(var, lock, func)	var
#elif defined(_MSC_VER)
#include <intrin.h>
#if SIZEOF_SSIZE_T == SIZEOF___INT64
#define ATOMIC_TYPE		__int64
#define ATOMIC_ADD(var, val)	_InterlockedExchangeAdd64(&var, (__int64) (val))
#define ATOMIC_INC(var)		_InterlockedIncrement64(&var)
#define ATOMIC_SUB(var, val)	_InterlockedExchangeAdd64(&var, -(__int64) (val))
#define ATOMIC_DEC(var)		_InterlockedDecrement64(&var)
#pragma intrinsic(_InterlockedExchangeAdd64)
#pragma intrinsic(_InterlockedIncrement64)
#pragma intrinsic(_InterlockedDecrement64)
#else
#define ATOMIC_TYPE		long
#define ATOMIC_ADD(var, val)	_InterlockedExchangeAdd(&var, (long) (val))
#define ATOMIC_INC(var)		_InterlockedIncrement(&var)
#define ATOMIC_SUB(var, val)	_InterlockedExchangeAdd(&var, -(long) (val))
#define ATOMIC_DEC(var)		_InterlockedDecrement(&var)
#pragma intrinsic(_InterlockedExchangeAdd)
#pragma intrinsic(_InterlockedIncrement)
#pragma intrinsic(_InterlockedDecrement)
#endif
#define ATOMIC_START(lock, func)
#define ATOMIC_END(lock, func)
#define ATOMIC_INIT(lock, func)
#define ATOMIC_COMP_SWAP(var, old, new, lock, func)	\
	_InterlockedCompareExchange(&var, new, old)
#pragma intrinsic(_InterlockedCompareExchange)
#define ATOMIC_GET(var, lock, func)	var
#else
#define ATOMIC_LOCK		PTHREAD_MUTEX_INITIALIZER /* must use locks */
#define ATOMIC_TYPE		ssize_t
#define ATOMIC_ADD(var, val)	var += (ssize_t) (val)
#define ATOMIC_INC(var)		var++
#define ATOMIC_SUB(var, val)	var -= (ssize_t) (val)
#define ATOMIC_DEC(var)		var--
#define ATOMIC_START(lock, func)	MT_lock_set(&lock, func)
#define ATOMIC_END(lock, func)	MT_lock_unset(&lock, func)
#define ATOMIC_INIT(lock, func)	MT_lock_init(&lock, func)
static inline int
atomic_comp_swap(volatile int *var, int old, int new, MT_Lock *lock, const char *func)
{
	int orig;
	MT_lock_set(lock, func);
	orig = *var;
	if (*var == old)
		*var = new;
	MT_lock_unset(lock, func);
	return orig;
}
#define ATOMIC_COMP_SWAP(var, old, new, lock, func)	\
	atomic_comp_swap(&var, old, new, &lock, func)
static inline int
atomic_get(volatile int *var, MT_Lock *lock, const char *func)
{
	int orig;
	MT_lock_set(lock, func);
	orig = *var;
	MT_lock_unset(lock, func);
	return orig;
}
#define ATOMIC_GET(var, lock, func)	atomic_get(&var, &lock, func)
#endif

#endif	/* _GDK_ATOMIC_H_ */
