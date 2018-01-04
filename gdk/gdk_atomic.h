/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
 * ATOMIC_INC -- increment a variable's value, return new value;
 * ATOMIC_DEC -- decrement a variable's value, return new value;
 * These interfaces work on variables of type ATOMIC_TYPE
 * (int or lng depending on architecture).
 *
 * In addition, the following operations are defined:
 * ATOMIC_TAS -- test-and-set: set variable to "true" and return old value
 * ATOMIC_CLEAR -- set variable to "false"
 * These two operations are only defined on variables of type
 * ATOMIC_FLAG, and the only values defined for such a variable are
 * "false" (zero) and "true" (non-zero).  The variable can be statically
 * initialized using the ATOMIC_FLAG_INIT macro.
 */

#ifndef _GDK_ATOMIC_H_
#define _GDK_ATOMIC_H_

/* define this if you don't want to use atomic instructions */
/* #define NO_ATOMIC_INSTRUCTIONS */

#if defined(HAVE_LIBATOMIC_OPS) && !defined(NO_ATOMIC_INSTRUCTIONS)

#include <atomic_ops.h>

#define ATOMIC_TYPE			AO_t

#define ATOMIC_GET(var, lck)		AO_load_full(&var)
#define ATOMIC_SET(var, val, lck)	AO_store_full(&var, (val))
#define ATOMIC_ADD(var, val, lck)	AO_fetch_and_add(&var, (val))
#define ATOMIC_SUB(var, val, lck)	AO_fetch_and_add(&var, -(val))
#define ATOMIC_INC(var, lck)		(AO_fetch_and_add1(&var) + 1)
#define ATOMIC_DEC(var, lck)		(AO_fetch_and_sub1(&var) - 1)

#define ATOMIC_INIT(lck)		((void) 0)

#define ATOMIC_FLAG			AO_TS_t
#define ATOMIC_FLAG_INIT		{ AO_TS_INITIALIZER }
#define ATOMIC_CLEAR(var, lck)		AO_CLEAR(&var)
#define ATOMIC_TAS(var, lck)	(AO_test_and_set_full(&var) != AO_TS_CLEAR)

#else

#if defined(_MSC_VER) && !defined(__INTEL_COMPILER) && !defined(NO_ATOMIC_INSTRUCTIONS)

#include <intrin.h>

#if SIZEOF_SSIZE_T == SIZEOF_LNG

#define ATOMIC_TYPE			lng

#define ATOMIC_GET(var, lck)		var
#define ATOMIC_SET(var, val, lck)	_InterlockedExchange64(&var, (val))
#define ATOMIC_ADD(var, val, lck)	_InterlockedExchangeAdd64(&var, val)
#define ATOMIC_SUB(var, val, lck)	_InterlockedExchangeAdd64(&var, -(val))
#define ATOMIC_INC(var, lck)		_InterlockedIncrement64(&var)
#define ATOMIC_DEC(var, lck)		_InterlockedDecrement64(&var)

#pragma intrinsic(_InterlockedExchange64)
#pragma intrinsic(_InterlockedExchangeAdd64)
#pragma intrinsic(_InterlockedIncrement64)
#pragma intrinsic(_InterlockedDecrement64)
#pragma intrinsic(_InterlockedCompareExchange64)

#else

#define ATOMIC_TYPE			int

#define ATOMIC_GET(var, lck)		var
#define ATOMIC_SET(var, val, lck)	_InterlockedExchange(&var, (val))
#define ATOMIC_ADD(var, val, lck)	_InterlockedExchangeAdd(&var, (val))
#define ATOMIC_SUB(var, val, lck)	_InterlockedExchangeAdd(&var, -(val))
#define ATOMIC_INC(var, lck)		_InterlockedIncrement(&var)
#define ATOMIC_DEC(var, lck)		_InterlockedDecrement(&var)

#pragma intrinsic(_InterlockedExchange)
#pragma intrinsic(_InterlockedExchangeAdd)
#pragma intrinsic(_InterlockedIncrement)
#pragma intrinsic(_InterlockedDecrement)

#endif

#define ATOMIC_INIT(lck)		((void) 0)

#define ATOMIC_FLAG			int
#define ATOMIC_FLAG_INIT		{ 0 }
#define ATOMIC_CLEAR(var, lck)		_InterlockedExchange(&var, 0)
#define ATOMIC_TAS(var, lck)		_InterlockedCompareExchange(&var, 1, 0)
#pragma intrinsic(_InterlockedCompareExchange)

#elif (defined(__GNUC__) || defined(__INTEL_COMPILER)) && !(defined(__sun__) && SIZEOF_SIZE_T == SIZEOF_LNG) && !defined(_MSC_VER) && !defined(NO_ATOMIC_INSTRUCTIONS)

#if SIZEOF_SSIZE_T == SIZEOF_LNG
#define ATOMIC_TYPE			lng
#else
#define ATOMIC_TYPE			int
#endif

#ifdef __ATOMIC_SEQ_CST

/* the new way of doing this according to GCC */
#define ATOMIC_GET(var, lck)		__atomic_load_n(&var, __ATOMIC_SEQ_CST)
#define ATOMIC_SET(var, val, lck)	__atomic_store_n(&var, (val), __ATOMIC_SEQ_CST)
#define ATOMIC_ADD(var, val, lck)	__atomic_fetch_add(&var, (val), __ATOMIC_SEQ_CST)
#define ATOMIC_SUB(var, val, lck)	__atomic_fetch_sub(&var, (val), __ATOMIC_SEQ_CST)
#define ATOMIC_INC(var, lck)		__atomic_add_fetch(&var, 1, __ATOMIC_SEQ_CST)
#define ATOMIC_DEC(var, lck)		__atomic_sub_fetch(&var, 1, __ATOMIC_SEQ_CST)

#define ATOMIC_FLAG			char
#define ATOMIC_FLAG_INIT		{ 0 }
#define ATOMIC_CLEAR(var, lck)		__atomic_clear(&var, __ATOMIC_SEQ_CST)
#define ATOMIC_TAS(var, lck)		__atomic_test_and_set(&var, __ATOMIC_SEQ_CST)

#else

/* the old way of doing this, (still?) needed for Intel compiler on Linux */
#define ATOMIC_GET(var, lck)		var
#define ATOMIC_SET(var, val, lck)	(var = (val))
#define ATOMIC_ADD(var, val, lck)	__sync_fetch_and_add(&var, (val))
#define ATOMIC_SUB(var, val, lck)	__sync_fetch_and_sub(&var, (val))
#define ATOMIC_INC(var, lck)		__sync_add_and_fetch(&var, 1)
#define ATOMIC_DEC(var, lck)		__sync_sub_and_fetch(&var, 1)

#define ATOMIC_FLAG			int
#define ATOMIC_FLAG_INIT		{ 0 }
#define ATOMIC_CLEAR(var, lck)		__sync_lock_release(&var)
#define ATOMIC_TAS(var, lck)		__sync_lock_test_and_set(&var, 1)

#endif

#define ATOMIC_INIT(lck)		((void) 0)

#else

#if SIZEOF_SSIZE_T == SIZEOF_LNG
#define ATOMIC_TYPE			lng
#else
#define ATOMIC_TYPE			int
#endif

static inline ATOMIC_TYPE
__ATOMIC_GET(volatile ATOMIC_TYPE *var, pthread_mutex_t *lck)
{
	ATOMIC_TYPE old;
	pthread_mutex_lock(lck);
	old = *var;
	pthread_mutex_unlock(lck);
	return old;
}
#define ATOMIC_GET(var, lck)		__ATOMIC_GET(&var, &(lck).lock)

static inline ATOMIC_TYPE
__ATOMIC_SET(volatile ATOMIC_TYPE *var, ATOMIC_TYPE val, pthread_mutex_t *lck)
{
	ATOMIC_TYPE new;
	pthread_mutex_lock(lck);
	*var = val;
	new = *var;
	pthread_mutex_unlock(lck);
	return new;
}
#define ATOMIC_SET(var, val, lck)	__ATOMIC_SET(&var, (val), &(lck).lock)

static inline ATOMIC_TYPE
__ATOMIC_ADD(volatile ATOMIC_TYPE *var, ATOMIC_TYPE val, pthread_mutex_t *lck)
{
	ATOMIC_TYPE old;
	pthread_mutex_lock(lck);
	old = *var;
	*var += val;
	pthread_mutex_unlock(lck);
	return old;
}
#define ATOMIC_ADD(var, val, lck)	__ATOMIC_ADD(&var, (val), &(lck).lock)

static inline ATOMIC_TYPE
__ATOMIC_SUB(volatile ATOMIC_TYPE *var, ATOMIC_TYPE val, pthread_mutex_t *lck)
{
	ATOMIC_TYPE old;
	pthread_mutex_lock(lck);
	old = *var;
	*var -= val;
	pthread_mutex_unlock(lck);
	return old;
}
#define ATOMIC_SUB(var, val, lck)	__ATOMIC_SUB(&var, (val), &(lck).lock)

static inline ATOMIC_TYPE
__ATOMIC_INC(volatile ATOMIC_TYPE *var, pthread_mutex_t *lck)
{
	ATOMIC_TYPE new;
	pthread_mutex_lock(lck);
	new = ++*var;
	pthread_mutex_unlock(lck);
	return new;
}
#define ATOMIC_INC(var, lck)		__ATOMIC_INC(&var, &(lck).lock)

static inline ATOMIC_TYPE
__ATOMIC_DEC(volatile ATOMIC_TYPE *var, pthread_mutex_t *lck)
{
	ATOMIC_TYPE new;
	pthread_mutex_lock(lck);
	new = --*var;
	pthread_mutex_unlock(lck);
	return new;
}
#define ATOMIC_DEC(var, lck)		__ATOMIC_DEC(&var, &(lck).lock)

#define USE_PTHREAD_LOCKS		/* must use pthread locks */
#define ATOMIC_LOCK			/* must use locks for atomic access */
#define ATOMIC_INIT(lck)		MT_lock_init(&(lck), #lck)

#define ATOMIC_FLAG int
#define ATOMIC_FLAG_INIT {0}
static inline ATOMIC_FLAG
__ATOMIC_TAS(volatile ATOMIC_FLAG *var, pthread_mutex_t *lck)
{
	ATOMIC_FLAG orig;
	pthread_mutex_lock(lck);
	if ((orig = *var) == 0)
		*var = 1;
	pthread_mutex_unlock(lck);
	return orig;
}
#define ATOMIC_TAS(var, lck)		__ATOMIC_TAS(&var, &(lck).lock)

static inline void
__ATOMIC_CLEAR(volatile ATOMIC_FLAG *var, pthread_mutex_t *lck)
{
	pthread_mutex_lock(lck);
	*var = 0;
	pthread_mutex_unlock(lck);
}
#define ATOMIC_CLEAR(var, lck)		__ATOMIC_CLEAR(&var, &(lck).lock)

#endif

#endif	/* LIBATOMIC_OPS */

#endif	/* _GDK_ATOMIC_H_ */
