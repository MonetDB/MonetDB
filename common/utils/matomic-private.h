/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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

#ifndef _MATOMIC_PRIVATE_H_
#define _MATOMIC_PRIVATE_H_

/* define this if you don't want to use atomic instructions */
/* #define NO_ATOMIC_INSTRUCTIONS */

/* the atomic type we export is always a 64 bit unsigned integer */

/* ignore __STDC_NO_ATOMICS__ if compiling using Intel compiler on
 * Windows since otherwise we can't compile this at all in C99 mode */
#if defined(HAVE_STDATOMIC_H) && (!defined(__STDC_NO_ATOMICS__) || (defined(__INTEL_COMPILER) && defined(_WINDOWS))) && !defined(NO_ATOMIC_INSTRUCTIONS)

#include <stdatomic.h>

#ifdef __INTEL_COMPILER
typedef volatile atomic_address ATOMIC_PTR_TYPE;
#else
typedef void *_Atomic volatile ATOMIC_PTR_TYPE;
#endif
#define ATOMIC_PTR_INIT(var, val)	atomic_init(var, val)
#define ATOMIC_PTR_DESTROY(var)		((void) 0)
#define ATOMIC_PTR_VAR_INIT(val)	ATOMIC_VAR_INIT(val)
#define ATOMIC_PTR_GET(var)		atomic_load(var)
#define ATOMIC_PTR_SET(var, val)	atomic_store(var, (void *) (val))
#define ATOMIC_PTR_XCG(var, val)	atomic_exchange(var, (void *) (val))
#define ATOMIC_PTR_CAS(var, exp, des)	atomic_compare_exchange_strong(var, exp, (void *) (des))

#elif defined(_MSC_VER) && !defined(NO_ATOMIC_INSTRUCTIONS)

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

#elif (defined(__GNUC__) || defined(__INTEL_COMPILER))  && defined(__ATOMIC_SEQ_CST) && !(defined(__sun__) && SIZEOF_SIZE_T == 8) && !defined(_MSC_VER) && !defined(NO_ATOMIC_INSTRUCTIONS)

/* the new way of doing this according to GCC (the old way, using
 * __sync_* primitives is not supported) */

typedef void *volatile ATOMIC_PTR_TYPE;
#define ATOMIC_PTR_INIT(var, val)	(*(var) = (val))
#define ATOMIC_PTR_VAR_INIT(val)	(val)
#define ATOMIC_PTR_DESTROY(var)		((void) 0)
#define ATOMIC_PTR_GET(var)		__atomic_load_n(var, __ATOMIC_SEQ_CST)
#define ATOMIC_PTR_SET(var, val)	__atomic_store_n(var, (val), __ATOMIC_SEQ_CST)
#define ATOMIC_PTR_XCG(var, val)	__atomic_exchange_n(var, (val), __ATOMIC_SEQ_CST)
#define ATOMIC_PTR_CAS(var, exp, des)	__atomic_compare_exchange_n(var, exp, (void *) (des), false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)

#else

/* emulate using mutexes */

typedef struct {
	void *val;
	pthread_mutex_t lck;
} ATOMIC_PTR_TYPE;
#define ATOMIC_PTR_VAR_INIT(v)	{ .val = (v), .lck = PTHREAD_MUTEX_INITIALIZER }

static inline void
ATOMIC_PTR_INIT(ATOMIC_PTR_TYPE *var, void *val)
{
	pthread_mutex_init(&var->lck, 0);
	var->val = val;
}

#define ATOMIC_PTR_DESTROY(var)	pthread_mutex_destroy(&(var)->lck)

static inline void *
ATOMIC_PTR_GET(ATOMIC_PTR_TYPE *var)
{
	void *old;
	pthread_mutex_lock(&var->lck);
	old = var->val;
	pthread_mutex_unlock(&var->lck);
	return old;
}

static inline void
ATOMIC_PTR_SET(ATOMIC_PTR_TYPE *var, void *val)
{
	pthread_mutex_lock(&var->lck);
	var->val = val;
	pthread_mutex_unlock(&var->lck);
}

static inline void *
ATOMIC_PTR_XCG(ATOMIC_PTR_TYPE *var, void *val)
{
	void *old;
	pthread_mutex_lock(&var->lck);
	old = var->val;
	var->val = val;
	pthread_mutex_unlock(&var->lck);
	return old;
}

static inline bool
ATOMIC_PTR_CAS(ATOMIC_PTR_TYPE *var, void **exp, void *des)
{
	bool ret;
	pthread_mutex_lock(&var->lck);
	if (var->val == *exp) {
		var->val = des;
		ret = true;
	} else {
		*exp = var->val;
		ret = false;
	}
	pthread_mutex_unlock(&var->lck);
	return ret;
}
#define ATOMIC_PTR_CAS(var, exp, des)	ATOMIC_PTR_CAS(var, exp, (void *) (des))

#define USE_NATIVE_LOCKS 1		/* must use pthread locks */

#endif

#endif	/* _MATOMIC_PRIVATE_H_ */
