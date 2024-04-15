/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/* This file provides interfaces to perform certain atomic operations
 * on variables.  Atomic in this sense means that an operation
 * performed in one thread shows up in another thread either
 * completely or not at all.
 *
 * The following operations are defined:
 * ATOMIC_VAR_INIT -- initializer for the variable (not necessarily atomic!);
 * ATOMIC_INIT -- initialize the variable (not necessarily atomic!);
 * ATOMIC_DESTROY -- destroy the variable
 * ATOMIC_GET -- return the value of a variable;
 * ATOMIC_SET -- set the value of a variable;
 * ATOMIC_XCG -- set the value of a variable, return original value;
 * ATOMIC_CAS -- compare-and-set (see below)
 * ATOMIC_ADD -- add a value to a variable, return original value;
 * ATOMIC_SUB -- subtract a value from a variable, return original value;
 * ATOMIC_INC -- increment a variable's value, return new value;
 * ATOMIC_DEC -- decrement a variable's value, return new value;
 * These interfaces work on variables of type ATOMIC_TYPE (uint64_t).
 *
 * The compare-and-set operation is based on the C11 standard: if the
 * atomic variable equals the expected value, the atomic variable is
 * replaced by the desired value and true is returned.  Otherwise, the
 * expected value is replaced by the current value of the atomic
 * variable and false is returned.
 *
 * Some of these are also available for pointers:
 * ATOMIC_PTR_INIT
 * ATOMIC_PTR_DESTROY
 * ATOMIC_PTR_GET
 * ATOMIC_PTR_SET
 * ATOMIC_PTR_XCG
 * ATOMIC_PTR_CAS
 * To define an atomic pointer, use ATOMIC_PTR_TYPE.  This is a (void *)
 * pointer that is appropriately adapted for atomic use.
 *
 * In addition, the following operations are defined:
 * ATOMIC_TAS -- test-and-set: set variable to "true" and return old value
 * ATOMIC_CLEAR -- set variable to "false"
 * These two operations are only defined on variables of type
 * ATOMIC_FLAG, and the only values defined for such a variable are
 * "false" and "true".  The variable can be statically initialized
 * to "false" using the ATOMIC_FLAG_INIT macro.
 */

#ifndef _MATOMIC_H_
#define _MATOMIC_H_

/* the atomic type we export is always a 64 bit unsigned integer */

/* ignore __STDC_NO_ATOMICS__ if compiling using Intel compiler on
 * Windows since otherwise we can't compile this at all in C99 mode */
#if defined(HAVE_STDATOMIC_H) && (!defined(__STDC_NO_ATOMICS__) || (defined(__INTEL_COMPILER) && defined(_WINDOWS)))

#ifdef __cplusplus

#include <atomic>

#if SIZEOF_LONG_LONG == 8
#if ATOMIC_LLONG_LOCK_FREE != 2
#if ATOMIC_LLONG_LOCK_FREE != 1
#error "we need _Atomic(unsigned [long] long) to be lock free"
#endif
typedef atomic_ulong ATOMIC_TYPE __attribute__((__aligned__(8)));
typedef unsigned long ATOMIC_BASE_TYPE;
#else
typedef atomic_ullong ATOMIC_TYPE __attribute__((__aligned__(8)));
typedef unsigned long long ATOMIC_BASE_TYPE;
#endif
#elif SIZEOF_LONG == 8
#if ATOMIC_LONG_LOCK_FREE != 2
#error "we need _Atomic(unsigned long) to be lock free"
#endif
typedef atomic_ulong ATOMIC_TYPE __attribute__((__aligned__(8)));
typedef unsigned long ATOMIC_BASE_TYPE;
#else
#error "we need a 64 bit atomic type"
#endif

#else

#include <stdatomic.h>

#if SIZEOF_LONG_LONG == 8
#if ATOMIC_LLONG_LOCK_FREE != 2
#if ATOMIC_LLONG_LOCK_FREE != 1
#error "we need _Atomic(unsigned [long] long) to be lock free"
#endif
typedef atomic_ulong ATOMIC_TYPE __attribute__((__aligned__(8)));
typedef unsigned long ATOMIC_BASE_TYPE;
#else
typedef volatile atomic_ullong ATOMIC_TYPE __attribute__((__aligned__(8)));
typedef unsigned long long ATOMIC_BASE_TYPE;
#endif
#elif SIZEOF_LONG == 8
#if ATOMIC_LONG_LOCK_FREE != 2
#error "we need _Atomic(unsigned long) to be lock free"
#endif
typedef volatile atomic_ulong ATOMIC_TYPE __attribute__((__aligned__(8)));
typedef unsigned long ATOMIC_BASE_TYPE;
#else
#error "we need a 64 bit atomic type"
#endif

#define ATOMIC_INIT(var, val)	atomic_init(var, (ATOMIC_BASE_TYPE) (val))
#define ATOMIC_DESTROY(var)		((void) 0)
#define ATOMIC_GET(var)			((ATOMIC_BASE_TYPE) *(var))
#define ATOMIC_SET(var, val)	(*(var) = (ATOMIC_BASE_TYPE) (val))
#define ATOMIC_XCG(var, val)	atomic_exchange(var, (ATOMIC_BASE_TYPE) (val))
#define ATOMIC_CAS(var, exp, des)	atomic_compare_exchange_strong(var, exp, (ATOMIC_BASE_TYPE) (des))
#define ATOMIC_ADD(var, val)	atomic_fetch_add(var, (ATOMIC_BASE_TYPE) (val))
#define ATOMIC_SUB(var, val)	atomic_fetch_sub(var, (ATOMIC_BASE_TYPE) (val))
#define ATOMIC_INC(var)			(++*(var))
#define ATOMIC_DEC(var)			(--*(var))
#define ATOMIC_OR(var, val)		atomic_fetch_or(var, (ATOMIC_BASE_TYPE) (val))
#define ATOMIC_AND(var, val)	atomic_fetch_and(var, (ATOMIC_BASE_TYPE) (val))

#ifdef __INTEL_COMPILER
typedef volatile atomic_address ATOMIC_PTR_TYPE;
#else
typedef void *_Atomic volatile ATOMIC_PTR_TYPE;
#endif
#define ATOMIC_PTR_INIT(var, val)	atomic_init(var, val)
#define ATOMIC_PTR_DESTROY(var)		((void) 0)
#define ATOMIC_PTR_VAR_INIT(val)	ATOMIC_VAR_INIT(val)
#define ATOMIC_PTR_GET(var)			(*(var))
#define ATOMIC_PTR_SET(var, val)	(*(var) = (void *) (val))
#define ATOMIC_PTR_XCG(var, val)	atomic_exchange(var, (void *) (val))
#define ATOMIC_PTR_CAS(var, exp, des)	atomic_compare_exchange_strong(var, exp, (void *) (des))

typedef volatile atomic_flag ATOMIC_FLAG;
/* ATOMIC_FLAG_INIT is already defined by the include file */
#define ATOMIC_CLEAR(var)	atomic_flag_clear(var)
#define ATOMIC_TAS(var)		atomic_flag_test_and_set(var)

#endif	/* __cplusplus */

#elif defined(_MSC_VER)

typedef uint64_t ATOMIC_BASE_TYPE;

#include <intrin.h>

/* On Windows, with Visual Studio 2005, the compiler uses acquire
 * semantics for read operations on volatile variables and release
 * semantics for write operations on volatile variables.
 *
 * With Visual Studio 2003, volatile to volatile references are
 * ordered; the compiler will not re-order volatile variable access.
 * However, these operations could be re-ordered by the processor.
 *
 * See
 * https://docs.microsoft.com/en-us/windows/desktop/Sync/synchronization-and-multiprocessor-issues
 *
 * This does not go for the Intel compiler, so there we use
 * _InterlockedExchangeAdd to load the value of an atomic variable.
 * There might be a better way, but it's hard to find in the
 * documentation.
 */

typedef __declspec(align(8)) volatile ATOMIC_BASE_TYPE ATOMIC_TYPE;

#define ATOMIC_VAR_INIT(val)	(val)
#define ATOMIC_INIT(var, val)	(*(var) = (val))
#define ATOMIC_DESTROY(var)		((void) 0)

#if SIZEOF_SIZE_T == 8

#ifdef __INTEL_COMPILER
#define ATOMIC_GET(var)			((ATOMIC_BASE_TYPE) _InterlockedExchangeAdd64(var, 0))
#else
#define ATOMIC_GET(var)			(*(var))
/* should we use _InterlockedExchangeAdd64(var, 0) instead? */
#endif
#define ATOMIC_SET(var, val)	_InterlockedExchange64(var, (ATOMIC_BASE_TYPE) (val))
#define ATOMIC_XCG(var, val)	((ATOMIC_BASE_TYPE) _InterlockedExchange64(var, (ATOMIC_BASE_TYPE) (val)))
static inline bool
ATOMIC_CAS(ATOMIC_TYPE *var, ATOMIC_BASE_TYPE *exp, ATOMIC_BASE_TYPE des)
{
	ATOMIC_BASE_TYPE old;
	old = _InterlockedCompareExchange64(var, des, *exp);
	if (old == *exp)
		return true;
	*exp = old;
	return false;
}
#define ATOMIC_CAS(var, exp, des)	ATOMIC_CAS(var, exp, (ATOMIC_BASE_TYPE) (des))
#define ATOMIC_ADD(var, val)	((ATOMIC_BASE_TYPE) _InterlockedExchangeAdd64(var, (LONG64) (val)))
#define ATOMIC_SUB(var, val)	((ATOMIC_BASE_TYPE) _InterlockedExchangeAdd64(var, -(LONG64) (val)))
#define ATOMIC_INC(var)			((ATOMIC_BASE_TYPE) _InterlockedIncrement64(var))
#define ATOMIC_DEC(var)			((ATOMIC_BASE_TYPE) _InterlockedDecrement64(var))
#define ATOMIC_OR(var, val)		((ATOMIC_BASE_TYPE) _InterlockedOr64(var, (ATOMIC_BASE_TYPE) (val)))
#define ATOMIC_AND(var, val)	((ATOMIC_BASE_TYPE) _InterlockedAnd64(var, (ATOMIC_BASE_TYPE) (val)))

#else

#ifdef DECLSPEC_NOINITALL
#define ATOMIC_GET(var)			((ATOMIC_BASE_TYPE) _InlineInterlockedExchangeAdd64(var, 0))
#define ATOMIC_SET(var, val)	_InlineInterlockedExchange64(var, (ATOMIC_BASE_TYPE) (val))
#define ATOMIC_XCG(var, val)	((ATOMIC_BASE_TYPE) _InlineInterlockedExchange64(var, (ATOMIC_BASE_TYPE) (val)))
#define ATOMIC_ADD(var, val)	((ATOMIC_BASE_TYPE) _InlineInterlockedExchangeAdd64(var, (LONG64) (val)))
#define ATOMIC_SUB(var, val)	((ATOMIC_BASE_TYPE) _InlineInterlockedExchangeAdd64(var, -(LONG64) (val)))
#define ATOMIC_INC(var)			((ATOMIC_BASE_TYPE) _InlineInterlockedIncrement64(var))
#define ATOMIC_DEC(var)			((ATOMIC_BASE_TYPE) _InlineInterlockedDecrement64(var))
#define ATOMIC_OR(var, val)		((ATOMIC_BASE_TYPE) _InlineInterlockedOr64(var, (ATOMIC_BASE_TYPE) (val)))
#define ATOMIC_AND(var, val)	((ATOMIC_BASE_TYPE) _InlineInterlockedAnd64(var, (ATOMIC_BASE_TYPE) (val)))
#else
#define ATOMIC_GET(var)			((ATOMIC_BASE_TYPE) _InterlockedExchangeAdd64(var, 0))
#define ATOMIC_SET(var, val)	_InterlockedExchange64(var, (ATOMIC_BASE_TYPE) (val))
#define ATOMIC_XCG(var, val)	((ATOMIC_BASE_TYPE) _InterlockedExchange64(var, (ATOMIC_BASE_TYPE) (val)))
#define ATOMIC_ADD(var, val)	((ATOMIC_BASE_TYPE) _InterlockedExchangeAdd64(var, (LONG64) (val)))
#define ATOMIC_SUB(var, val)	((ATOMIC_BASE_TYPE) _InterlockedExchangeAdd64(var, -(LONG64) (val)))
#define ATOMIC_INC(var)			((ATOMIC_BASE_TYPE) _InterlockedIncrement64(var))
#define ATOMIC_DEC(var)			((ATOMIC_BASE_TYPE) _InterlockedDecrement64(var))
#define ATOMIC_OR(var, val)		((ATOMIC_BASE_TYPE) _InterlockedOr64(var, (ATOMIC_BASE_TYPE) (val)))
#define ATOMIC_AND(var, val)	((ATOMIC_BASE_TYPE) _InterlockedAnd64(var, (ATOMIC_BASE_TYPE) (val)))
#endif
static inline bool
ATOMIC_CAS(ATOMIC_TYPE *var, ATOMIC_BASE_TYPE *exp, ATOMIC_BASE_TYPE des)
{
	ATOMIC_BASE_TYPE old;
	old = _InterlockedCompareExchange64(var, des, *exp);
	if (old == *exp)
		return true;
	*exp = old;
	return false;
}
#define ATOMIC_CAS(var, exp, des)	ATOMIC_CAS(var, exp, (ATOMIC_BASE_TYPE) (des))

#endif

typedef PVOID volatile ATOMIC_PTR_TYPE;
#define ATOMIC_PTR_INIT(var, val)	(*(var) = (val))
#define ATOMIC_PTR_DESTROY(var)		((void) 0)
#define ATOMIC_PTR_VAR_INIT(val)	(val)
#define ATOMIC_PTR_GET(var)		(*(var))
#define ATOMIC_PTR_SET(var, val)	_InterlockedExchangePointer(var, (PVOID) (val))
#define ATOMIC_PTR_XCG(var, val)	_InterlockedExchangePointer(var, (PVOID) (val))
#pragma intrinsic(_InterlockedCompareExchangePointer)
static inline bool
ATOMIC_PTR_CAS(ATOMIC_PTR_TYPE *var, void **exp, void *des)
{
	void *old;
	old = _InterlockedCompareExchangePointer(var, des, *exp);
	if (old == *exp)
		return true;
	*exp = old;
	return false;
}
#define ATOMIC_PTR_CAS(var, exp, des)	ATOMIC_PTR_CAS(var, exp, (void *) (des))

typedef volatile int ATOMIC_FLAG;
#define ATOMIC_FLAG_INIT	{ 0 }
#define ATOMIC_CLEAR(var)	_InterlockedExchange(var, 0)
#define ATOMIC_TAS(var)		_InterlockedCompareExchange(var, 1, 0)
#pragma intrinsic(_InterlockedCompareExchange)

#elif (defined(__GNUC__) || defined(__INTEL_COMPILER))  && defined(__ATOMIC_SEQ_CST) && !(defined(__sun__) && SIZEOF_SIZE_T == 8) && !defined(_MSC_VER)

/* the new way of doing this according to GCC (the old way, using
 * __sync_* primitives is not supported) */

typedef uint64_t ATOMIC_BASE_TYPE;
typedef volatile ATOMIC_BASE_TYPE ATOMIC_TYPE __attribute__((__aligned__ (8)));

#define ATOMIC_VAR_INIT(val)	(val)
#define ATOMIC_INIT(var, val)	(*(var) = (val))
#define ATOMIC_DESTROY(var)	((void) 0)

#define ATOMIC_GET(var)			((ATOMIC_BASE_TYPE) __atomic_load_n(var, __ATOMIC_SEQ_CST))
#define ATOMIC_SET(var, val)	__atomic_store_n(var, (ATOMIC_BASE_TYPE) (val), __ATOMIC_SEQ_CST)
#define ATOMIC_XCG(var, val)	__atomic_exchange_n(var, (ATOMIC_BASE_TYPE) (val), __ATOMIC_SEQ_CST)
#define ATOMIC_CAS(var, exp, des)	__atomic_compare_exchange_n(var, exp, (ATOMIC_BASE_TYPE) (des), false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)
#define ATOMIC_ADD(var, val)	__atomic_fetch_add(var, (ATOMIC_BASE_TYPE) (val), __ATOMIC_SEQ_CST)
#define ATOMIC_SUB(var, val)	__atomic_fetch_sub(var, (ATOMIC_BASE_TYPE) (val), __ATOMIC_SEQ_CST)
#define ATOMIC_INC(var)			__atomic_add_fetch(var, 1, __ATOMIC_SEQ_CST)
#define ATOMIC_DEC(var)			__atomic_sub_fetch(var, 1, __ATOMIC_SEQ_CST)
#define ATOMIC_OR(var, val)		__atomic_fetch_or(var, (ATOMIC_BASE_TYPE) (val), __ATOMIC_SEQ_CST)
#define ATOMIC_AND(var, val)	__atomic_fetch_and(var, (ATOMIC_BASE_TYPE) (val), __ATOMIC_SEQ_CST)

typedef void *volatile ATOMIC_PTR_TYPE;
#define ATOMIC_PTR_INIT(var, val)	(*(var) = (val))
#define ATOMIC_PTR_VAR_INIT(val)	(val)
#define ATOMIC_PTR_DESTROY(var)		((void) 0)
#define ATOMIC_PTR_GET(var)		__atomic_load_n(var, __ATOMIC_SEQ_CST)
#define ATOMIC_PTR_SET(var, val)	__atomic_store_n(var, (val), __ATOMIC_SEQ_CST)
#define ATOMIC_PTR_XCG(var, val)	__atomic_exchange_n(var, (val), __ATOMIC_SEQ_CST)
#define ATOMIC_PTR_CAS(var, exp, des)	__atomic_compare_exchange_n(var, exp, (void *) (des), false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)

typedef volatile char ATOMIC_FLAG;
#define ATOMIC_FLAG_INIT	{ 0 }
#define ATOMIC_CLEAR(var)	__atomic_clear(var, __ATOMIC_SEQ_CST)
#define ATOMIC_TAS(var)		__atomic_test_and_set(var, __ATOMIC_SEQ_CST)

#else

#error "we need either stdatomics.h support or native Windows support for atomics"

#endif

#endif	/* _MATOMIC_H_ */
