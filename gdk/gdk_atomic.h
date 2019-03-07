/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/* This file provides interfaces to perform certain atomic operations
 * on variables.  Atomic in this sense means that an operation
 * performed in one thread shows up in another thread either
 * completely or not at all.
 *
 * The following operations are defined:
 * ATOMIC_VAR_INIT -- initializer for the variable (not necessarily atomic!);
 * ATOMIC_INIT -- initialize the variable (not necessarily atomic!);
 * ATOMIC_GET -- return the value of a variable;
 * ATOMIC_SET -- set the value of a variable;
 * ATOMIC_ADD -- add a value to a variable, return original value;
 * ATOMIC_SUB -- subtract a value from a variable, return original value;
 * ATOMIC_INC -- increment a variable's value, return new value;
 * ATOMIC_DEC -- decrement a variable's value, return new value;
 * These interfaces work on variables of type ATOMIC_TYPE
 * (int or int64_t depending on architecture).
 *
 * In addition, the following operations are defined:
 * ATOMIC_TAS -- test-and-set: set variable to "true" and return old value
 * ATOMIC_CLEAR -- set variable to "false"
 * These two operations are only defined on variables of type
 * ATOMIC_FLAG, and the only values defined for such a variable are
 * "false" and "true".  The variable can be statically initialized
 * using the ATOMIC_FLAG_INIT macro.
 */

#ifndef _GDK_ATOMIC_H_
#define _GDK_ATOMIC_H_

/* define this if you don't want to use atomic instructions */
/* #define NO_ATOMIC_INSTRUCTIONS */

#if defined(HAVE_LIBATOMIC_OPS) && !defined(NO_ATOMIC_INSTRUCTIONS)

#include <atomic_ops.h>

#define ATOMIC_TYPE			AO_t
#define ATOMIC_VAR_INIT(val)		(val)
#define ATOMIC_INIT(var, val)		(*(var) = (val))

#define ATOMIC_GET(var)			AO_load_full(var)
#define ATOMIC_SET(var, val)		AO_store_full(var, (AO_t) (val))
#define ATOMIC_ADD(var, val)		AO_fetch_and_add(var, (AO_t) (val))
#define ATOMIC_SUB(var, val)		AO_fetch_and_add(var, (AO_t) -(val))
#define ATOMIC_INC(var)			(AO_fetch_and_add1(var) + 1)
#define ATOMIC_DEC(var)			(AO_fetch_and_sub1(var) - 1)

#define ATOMIC_FLAG			AO_TS_t
#define ATOMIC_FLAG_INIT		{ AO_TS_INITIALIZER }
#define ATOMIC_CLEAR(var)		AO_CLEAR(var)
#define ATOMIC_TAS(var)	(AO_test_and_set_full(var) != AO_TS_CLEAR)

#elif defined(HAVE_STDATOMIC_H) && !defined(__INTEL_COMPILER) && !defined(__STDC_NO_ATOMICS__) && !defined(NO_ATOMIC_INSTRUCTIONS)

#include <stdatomic.h>

#if ATOMIC_LLONG_LOCK_FREE == 2
#define ATOMIC_TYPE			atomic_ullong
#define ATOMIC_CAST			unsigned long long
#elif ATOMIC_LONG_LOCK_FREE == 2
#define ATOMIC_TYPE			atomic_ulong
#define ATOMIC_CAST			unsigned long
#elif ATOMIC_INT_LOCK_FREE == 2
#define ATOMIC_TYPE			atomic_uint
#define ATOMIC_CAST			unsigned int
#elif ATOMIC_LLONG_LOCK_FREE == 1
#define ATOMIC_TYPE			atomic_ullong
#define ATOMIC_CAST			unsigned long long
#elif ATOMIC_LONG_LOCK_FREE == 1
#define ATOMIC_TYPE			atomic_ulong
#define ATOMIC_CAST			unsigned long
#elif ATOMIC_INT_LOCK_FREE == 1
#define ATOMIC_TYPE			atomic_uint
#define ATOMIC_CAST			unsigned int
#else
#define ATOMIC_TYPE			atomic_ullong
#define ATOMIC_CAST			unsigned long long
#endif

#define ATOMIC_INIT(var, val)	atomic_init(var, (ATOMIC_CAST) (val))
#define ATOMIC_GET(var)		atomic_load(var)
#define ATOMIC_SET(var, val)	atomic_store(var, (ATOMIC_CAST) (val))
#define ATOMIC_ADD(var, val)	atomic_fetch_add(var, (ATOMIC_CAST) (val))
#define ATOMIC_SUB(var, val)	atomic_fetch_sub(var, (ATOMIC_CAST) (val))
#define ATOMIC_INC(var)		(atomic_fetch_add(var, 1) + 1)
#define ATOMIC_DEC(var)		(atomic_fetch_sub(var, 1) - 1)

#define ATOMIC_FLAG		atomic_flag
/* ATOMIC_FLAG_INIT is already defined by the include file */
#define ATOMIC_CLEAR(var)	atomic_flag_clear(var)
#define ATOMIC_TAS(var)		atomic_flag_test_and_set(var)

#elif defined(_MSC_VER) && !defined(__INTEL_COMPILER) && !defined(NO_ATOMIC_INSTRUCTIONS)

#include <intrin.h>

#if SIZEOF_SSIZE_T == 8

#define ATOMIC_TYPE		volatile int64_t
#define ATOMIC_VAR_INIT(val)	(val)
#define ATOMIC_INIT(var, val)	(*(var) = (val))

#define ATOMIC_GET(var)		(*(var))
/* should we use _InterlockedExchangeAdd64(var, 0) instead? */
#define ATOMIC_SET(var, val)	_InterlockedExchange64(var, (int64_t) (val))
#define ATOMIC_ADD(var, val)	_InterlockedExchangeAdd64(var, (int64_t) (val))
#define ATOMIC_SUB(var, val)	_InterlockedExchangeAdd64(var, -(int64_t) (val))
#define ATOMIC_INC(var)		_InterlockedIncrement64(var)
#define ATOMIC_DEC(var)		_InterlockedDecrement64(var)

#pragma intrinsic(_InterlockedExchange64)
#pragma intrinsic(_InterlockedExchangeAdd64)
#pragma intrinsic(_InterlockedIncrement64)
#pragma intrinsic(_InterlockedDecrement64)
#pragma intrinsic(_InterlockedCompareExchange64)

#else

#define ATOMIC_TYPE		volatile int
#define ATOMIC_VAR_INIT(val)	(val)
#define ATOMIC_INIT(var, val)	(*(var) = (val))

#define ATOMIC_GET(var)		(*(var))
/* should we use _InterlockedExchangeAdd(var, 0) instead? */
#define ATOMIC_SET(var, val)	_InterlockedExchange(var, (int) (val))
#define ATOMIC_ADD(var, val)	_InterlockedExchangeAdd(var, (int) (val))
#define ATOMIC_SUB(var, val)	_InterlockedExchangeAdd(var, -(int) (val))
#define ATOMIC_INC(var)		_InterlockedIncrement(var)
#define ATOMIC_DEC(var)		_InterlockedDecrement(var)

#pragma intrinsic(_InterlockedExchange)
#pragma intrinsic(_InterlockedExchangeAdd)
#pragma intrinsic(_InterlockedIncrement)
#pragma intrinsic(_InterlockedDecrement)

#endif

#define ATOMIC_FLAG		int
#define ATOMIC_FLAG_INIT	{ 0 }
#define ATOMIC_CLEAR(var)	_InterlockedExchange(var, 0)
#define ATOMIC_TAS(var)		_InterlockedCompareExchange(var, 1, 0)
#pragma intrinsic(_InterlockedCompareExchange)

#elif (defined(__GNUC__) || defined(__INTEL_COMPILER)) && !(defined(__sun__) && SIZEOF_SIZE_T == 8) && !defined(_MSC_VER) && !defined(NO_ATOMIC_INSTRUCTIONS)

/* the new way of doing this according to GCC (the old way, using
 * __sync_* primitives is not supported) */

#if SIZEOF_SSIZE_T == 8
#define ATOMIC_TYPE		int64_t
#else
#define ATOMIC_TYPE		int
#endif
#define ATOMIC_VAR_INIT(val)	(val)
#define ATOMIC_INIT(var, val)	(*(var) = (val))

#define ATOMIC_GET(var)		__atomic_load_n(var, __ATOMIC_SEQ_CST)
#define ATOMIC_SET(var, val)	__atomic_store_n(var, (ATOMIC_TYPE) (val), __ATOMIC_SEQ_CST)
#define ATOMIC_ADD(var, val)	__atomic_fetch_add(var, (ATOMIC_TYPE) (val), __ATOMIC_SEQ_CST)
#define ATOMIC_SUB(var, val)	__atomic_fetch_sub(var, (ATOMIC_TYPE) (val), __ATOMIC_SEQ_CST)
#define ATOMIC_INC(var)		__atomic_add_fetch(var, 1, __ATOMIC_SEQ_CST)
#define ATOMIC_DEC(var)		__atomic_sub_fetch(var, 1, __ATOMIC_SEQ_CST)

#define ATOMIC_FLAG		char
#define ATOMIC_FLAG_INIT	{ 0 }
#define ATOMIC_CLEAR(var)	__atomic_clear(var, __ATOMIC_SEQ_CST)
#define ATOMIC_TAS(var)		__atomic_test_and_set(var, __ATOMIC_SEQ_CST)

#else

/* emulate using mutexes */

typedef struct {
	size_t val;
	pthread_mutex_t lck;
} ATOMIC_TYPE;
#define ATOMIC_VAR_INIT(v)	{ .val = (v), .lck = PTHREAD_MUTEX_INITIALIZER }

static inline void
ATOMIC_INIT(ATOMIC_TYPE *var, size_t val)
{
	pthread_mutex_init(&var->lck, 0);
	var->val = val;
}
#define ATOMIC_INIT(var, val)	ATOMIC_INIT((var), (size_t) (val))

static inline size_t
ATOMIC_GET(ATOMIC_TYPE *var)
{
	size_t old;
	pthread_mutex_lock(&var->lck);
	old = var->val;
	pthread_mutex_unlock(&var->lck);
	return old;
}

static inline size_t
ATOMIC_SET(ATOMIC_TYPE *var, size_t val)
{
	size_t new;
	pthread_mutex_lock(&var->lck);
	new = var->val = val;
	pthread_mutex_unlock(&var->lck);
	return new;
}
#define ATOMIC_SET(var, val)	ATOMIC_SET(var, (size_t) (val))

static inline size_t
ATOMIC_ADD(ATOMIC_TYPE *var, size_t val)
{
	size_t old;
	pthread_mutex_lock(&var->lck);
	old = var->val;
	var->val += val;
	pthread_mutex_unlock(&var->lck);
	return old;
}
#define ATOMIC_ADD(var, val)	ATOMIC_ADD(var, (size_t) (val))

static inline size_t
ATOMIC_SUB(ATOMIC_TYPE *var, size_t val)
{
	size_t old;
	pthread_mutex_lock(&var->lck);
	old = var->val;
	var->val -= val;
	pthread_mutex_unlock(&var->lck);
	return old;
}
#define ATOMIC_SUB(var, val)	ATOMIC_SUB(var, (size_t) (val))

static inline size_t
ATOMIC_INC(ATOMIC_TYPE *var)
{
	size_t new;
	pthread_mutex_lock(&var->lck);
	new = var->val += 1;
	pthread_mutex_unlock(&var->lck);
	return new;
}

static inline size_t
ATOMIC_DEC(ATOMIC_TYPE *var)
{
	size_t new;
	pthread_mutex_lock(&var->lck);
	new = var->val -= 1;
	pthread_mutex_unlock(&var->lck);
	return new;
}

typedef struct {
	bool flg;
	pthread_mutex_t lck;
} ATOMIC_FLAG;
#define ATOMIC_FLAG_INIT	{ .flg = false, .lck = PTHREAD_MUTEX_INITIALIZER }

static inline bool
ATOMIC_TAS(ATOMIC_FLAG *var)
{
	bool old;
	pthread_mutex_lock(&var->lck);
	old = var->flg;
	var->flg = true;
	pthread_mutex_unlock(&var->lck);
	return old;
}

static inline void
ATOMIC_CLEAR(ATOMIC_FLAG *var)
{
	pthread_mutex_lock(&var->lck);
	var->flg = false;
	pthread_mutex_unlock(&var->lck);
}

#define USE_PTHREAD_LOCKS		/* must use pthread locks */

#endif	/* LIBATOMIC_OPS */

#endif	/* _GDK_ATOMIC_H_ */
