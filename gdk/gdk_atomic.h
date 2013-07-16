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

#ifdef HAVE_LIBATOMIC_OPS

#include <atomic_ops.h>

#define ATOMIC_TYPE			AO_t

#define ATOMIC_GET(var, lck, fcn)	AO_load_full(&var)
#define ATOMIC_SET(var, val, lck, fcn)	AO_store_full(&var, (val))
#define ATOMIC_ADD(var, val, lck, fcn)	AO_fetch_and_add(&var, (val))
#define ATOMIC_SUB(var, val, lck, fcn)	AO_fetch_and_add(&var, -(val))
#define ATOMIC_INC(var, lck, fcn)	AO_fetch_and_add1(&var)
#define ATOMIC_DEC(var, lck, fcn)	AO_fetch_and_sub1(&var)

#define ATOMIC_INIT(lck, fcn)		((void) 0)

#define ATOMIC_FLAG			AO_TS_t
#define ATOMIC_FLAG_INIT		{ AO_TS_INITIALIZER }
#define ATOMIC_CLEAR(var, lck, fcn)	AO_CLEAR(&var)
#define ATOMIC_TAS(var, lck, fcn)	(AO_test_and_set_full(&var) != AO_TS_CLEAR)

#else

#if defined(_MSC_VER) && !defined(__INTEL_COMPILER)

#include <intrin.h>

#if SIZEOF_SSIZE_T == SIZEOF_LNG

#define ATOMIC_TYPE			lng

#define ATOMIC_GET(var, lck, fcn)	var
#define ATOMIC_SET(var, val, lck, fcn)	_InterlockedExchange64(&var, (val))
#define ATOMIC_ADD(var, val, lck, fcn)	_InterlockedExchangeAdd64(&var, val)
#define ATOMIC_SUB(var, val, lck, fcn)	_InterlockedExchangeAdd64(&var, -(val))
#define ATOMIC_INC(var, lck, fcn)	_InterlockedIncrement64(&var)
#define ATOMIC_DEC(var, lck, fcn)	_InterlockedDecrement64(&var)

#pragma intrinsic(_InterlockedExchange64)
#pragma intrinsic(_InterlockedExchangeAdd64)
#pragma intrinsic(_InterlockedIncrement64)
#pragma intrinsic(_InterlockedDecrement64)
#pragma intrinsic(_InterlockedCompareExchange64)

#else

#define ATOMIC_TYPE			int

#define ATOMIC_GET(var, lck, fcn)	var
#define ATOMIC_SET(var, val, lck, fcn)	_InterlockedExchange(&var, (val))
#define ATOMIC_ADD(var, val, lck, fcn)	_InterlockedExchangeAdd(&var, (val))
#define ATOMIC_SUB(var, val, lck, fcn)	_InterlockedExchangeAdd(&var, -(val))
#define ATOMIC_INC(var, lck, fcn)	_InterlockedIncrement(&var)
#define ATOMIC_DEC(var, lck, fcn)	_InterlockedDecrement(&var)

#pragma intrinsic(_InterlockedExchange)
#pragma intrinsic(_InterlockedExchangeAdd)
#pragma intrinsic(_InterlockedIncrement)
#pragma intrinsic(_InterlockedDecrement)

#endif

#define ATOMIC_INIT(lck, fcn)		((void) 0)

#define ATOMIC_FLAG			int
#define ATOMIC_FLAG_INIT		{ 0 }
#define ATOMIC_CLEAR(var, lck, fcn)	_InterlockedExchange(&var, 0)
#define ATOMIC_TAS(var, lck, fcn)	_InterlockedCompareExchange(&var, 1, 0)
#pragma intrinsic(_InterlockedCompareExchange)

#elif (defined(__GNUC__) || defined(__INTEL_COMPILER)) && !(defined(__sun__) && SIZEOF_SIZE_T == SIZEOF_LNG) && !defined(_MSC_VER)

#if SIZEOF_SSIZE_T == SIZEOF_LNG

#define ATOMIC_TYPE			lng

#define ATOMIC_GET(var, lck, fcn)	__atomic_load_n(&var, __ATOMIC_SEQ_CST)
#define ATOMIC_SET(var, val, lck, fcn)	__atomic_store_n(&var, (val), __ATOMIC_SEQ_CST)
#define ATOMIC_ADD(var, val, lck, fcn)	__atomic_fetch_add(&var, (val), __ATOMIC_SEQ_CST)
#define ATOMIC_SUB(var, val, lck, fcn)	__atomic_fetch_sub(&var, (val), __ATOMIC_SEQ_CST)
#define ATOMIC_INC(var, lck, fcn)	__atomic_add_fetch(&var, 1, __ATOMIC_SEQ_CST)
#define ATOMIC_DEC(var, lck, fcn)	__atomic_sub_fetch(&var, 1, __ATOMIC_SEQ_CST)

#else

#define ATOMIC_TYPE			int

#define ATOMIC_GET(var, lck, fcn)	__atomic_load_n(&var, __ATOMIC_SEQ_CST)
#define ATOMIC_SET(var, val, lck, fcn)	__atomic_store_n(&var, (val), __ATOMIC_SEQ_CST)
#define ATOMIC_ADD(var, val, lck, fcn)	__atomic_fetch_add(&var, (val), __ATOMIC_SEQ_CST)
#define ATOMIC_SUB(var, val, lck, fcn)	__atomic_fetch_sub(&var, (val), __ATOMIC_SEQ_CST)
#define ATOMIC_INC(var, lck, fcn)	__atomic_add_fetch(&var, 1, __ATOMIC_SEQ_CST)
#define ATOMIC_DEC(var, lck, fcn)	__atomic_sub_fetch(&var, 1, __ATOMIC_SEQ_CST)

#endif

#define ATOMIC_INIT(lck, fcn)		((void) 0)

#define ATOMIC_FLAG			char
#define ATOMIC_FLAG_INIT		{ 0 }
#define ATOMIC_CLEAR(var, lck, fcn)	__atomic_clear(&var, __ATOMIC_SEQ_CST)
#define ATOMIC_TAS(var, lck, fcn)	__atomic_test_and_set(&var, __ATOMIC_SEQ_CST)

#else

#if SIZEOF_SSIZE_T == SIZEOF_LNG

#define ATOMIC_TYPE			lng

static inline lng
__ATOMIC_GET(volatile lng *var, pthread_mutex_t *lck)
{
	lng old;
	pthread_mutex_lock(lck);
	old = *var;
	pthread_mutex_unlock(lck);
	return old;
}
#define ATOMIC_GET(var, lck, fcn)	__ATOMIC_GET(&var, &(lck))

static inline lng
__ATOMIC_SET(volatile lng *var, lng val, pthread_mutex_t *lck)
{
	lng new;
	pthread_mutex_lock(lck);
	*var = val;
	new = *var;
	pthread_mutex_unlock(lck);
	return new;
}
#define ATOMIC_SET(var, val, lck, fcn)	__ATOMIC_SET(&var, (val), &(lck))

static inline lng
__ATOMIC_ADD(volatile lng *var, lng val, pthread_mutex_t *lck)
{
	lng old;
	pthread_mutex_lock(lck);
	old = *var;
	*var += val;
	pthread_mutex_unlock(lck);
	return old;
}
#define ATOMIC_ADD(var, val, lck, fcn)	__ATOMIC_ADD(&var, (val), &(lck))

static inline lng
__ATOMIC_SUB(volatile lng *var, lng val, pthread_mutex_t *lck)
{
	lng old;
	pthread_mutex_lock(lck);
	old = *var;
	*var -= val;
	pthread_mutex_unlock(lck);
	return old;
}
#define ATOMIC_SUB(var, val, lck, fcn)	__ATOMIC_SUB(&var, (val), &(lck))

static inline lng
__ATOMIC_INC(volatile lng *var, pthread_mutex_t *lck)
{
	lng new;
	pthread_mutex_lock(lck);
	new = ++*var;
	pthread_mutex_unlock(lck);
	return new;
}
#define ATOMIC_INC(var, lck, fcn)		__ATOMIC_INC(&var, &(lck))

static inline lng
__ATOMIC_DEC(volatile lng *var, pthread_mutex_t *lck)
{
	lng new;
	pthread_mutex_lock(lck);
	new = --*var;
	pthread_mutex_unlock(lck);
	return new;
}
#define ATOMIC_DEC(var, lck, fcn)		__ATOMIC_DEC(&var, &(lck))

#else

#define ATOMIC_TYPE			int

static inline int
__ATOMIC_GET(volatile int *var, pthread_mutex_t *lck)
{
	int old;
	pthread_mutex_lock(lck);
	old = *var;
	pthread_mutex_unlock(lck);
	return old;
}
#define ATOMIC_GET(var, lck, fcn)	__ATOMIC_GET(&var, &(lck))

static inline int
__ATOMIC_SET(volatile int *var, int val, pthread_mutex_t *lck)
{
	int new;
	pthread_mutex_lock(lck);
	*var = val;
	new = *var;
	pthread_mutex_unlock(lck);
	return new;
}
#define ATOMIC_SET(var, val, lck, fcn)	__ATOMIC_SET(&var, (val), &(lck))

static inline int
__ATOMIC_ADD(volatile int *var, int val, pthread_mutex_t *lck)
{
	int old;
	pthread_mutex_lock(lck);
	old = *var;
	*var += val;
	pthread_mutex_unlock(lck);
	return old;
}
#define ATOMIC_ADD(var, val, lck, fcn)	__ATOMIC_ADD(&var, (val), &(lck))

static inline int
__ATOMIC_SUB(volatile int *var, int val, pthread_mutex_t *lck)
{
	int old;
	pthread_mutex_lock(lck);
	old = *var;
	*var -= val;
	pthread_mutex_unlock(lck);
	return old;
}
#define ATOMIC_SUB(var, val, lck, fcn)	__ATOMIC_SUB(&var, (val), &(lck))

static inline int
__ATOMIC_INC(volatile int *var, pthread_mutex_t *lck)
{
	int new;
	pthread_mutex_lock(lck);
	new = ++*var;
	pthread_mutex_unlock(lck);
	return new;
}
#define ATOMIC_INC(var, lck, fcn)		__ATOMIC_INC(&var, &(lck))

static inline int
__ATOMIC_DEC(volatile int *var, pthread_mutex_t *lck)
{
	int new;
	pthread_mutex_lock(lck);
	new = --*var;
	pthread_mutex_unlock(lck);
	return new;
}
#define ATOMIC_DEC(var, lck, fcn)		__ATOMIC_DEC(&var, &(lck))

#endif

#define ATOMIC_LOCK		/* must use locks */
#define ATOMIC_INIT(lck, fcn)	MT_lock_init(&(lck), fcn)

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
#define ATOMIC_TAS(var, lck, fcn)		__ATOMIC_TAS(&var, &(lck))

static inline void
__ATOMIC_CLEAR(volatile ATOMIC_FLAG *var, pthread_mutex_t *lck)
{
	pthread_mutex_lock(lck);
	*var = 0;
	pthread_mutex_unlock(lck);
}
#define ATOMIC_CLEAR(var, lck, fcn)		__ATOMIC_CLEAR(&var, &(lck))

#endif

#endif	/* LIBATOMIC_OPS */

#endif	/* _GDK_ATOMIC_H_ */
