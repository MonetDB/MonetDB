/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_system.h"

#include <assert.h>

#define NN (3)

volatile int timeout = 100; // set this to 0 during interactive debugging

/* global state protected by a lock.
 *
 * Every worker thread has a number of permits and a number of ticks.
 * It sleeps on the condition variable, whenever it wakes it will
 * try to convert one permit into a tick, that is, permit-- and tick++.
 */

MT_Lock lock = MT_LOCK_INITIALIZER(lock);
MT_Cond condvar = MT_COND_INITIALIZER(the_condvar);
struct state {
	MT_Id id;
	int ticks;
	int permits;
	bool terminate;
	bool terminated;
} states[NN] = { {0} };

/*
 * The main thread holds the lock so it can manipulate and verify the permits
 * and ticks.  It uses this function to temporarily release the lock so the
 * workers get a chance to do their work. We give them a 100ms, which should be
 * plenty.
 *
 * If we cannot retake the lock after that interval we assume a worker thread
 * has gone astray while holding the lock.
 */
static void
let_run(void)
{
	MT_lock_unset(&lock);

	MT_sleep_ms(100);

	// try to retake the lock. Make a few attempts before giving up.
	int attempts = 0;
	while (!MT_lock_try(&lock)) {
		if (timeout > 0 && ++attempts > timeout) {
			fprintf(stderr, "Can't get hold of the lock after %d attempts\n", attempts);
			fprintf(stderr, "If this is because you're running this program in a debugger,\n");
			fprintf(stderr, "try setting the timeout variable to 0.\n");
			abort();
		}
		MT_sleep_ms(10);
	}
}


static void
worker(void *arg)
{
	struct state *st = arg;
	int id = (int)(st - &states[0]);
	fprintf(stderr, "worker %d started, waiting to acquire lock\n", id);

	MT_lock_set(&lock);
	fprintf(stderr, "worker %d acquired lock\n", id);
	while (1) {
		if (st->terminate) {
			fprintf(stderr, "worker %d terminating\n", id);
			break;
		}
		if (st->permits > 0) {
			fprintf(stderr, "worker %d ticking\n", id);
			st->ticks++;
			st->permits--;
		}
		fprintf(stderr, "worker %d waiting on condvar\n", id);
		MT_cond_wait(&condvar, &lock);
		fprintf(stderr, "worker %d woke up\n", id);
	}
	st->terminated = true;
	MT_lock_unset(&lock);
}


static void
check_impl(int line, int expected_sum_ticks, int expected_max_ticks, int expected_sum_permits)
{
	int sum_ticks = 0;
	int max_ticks = -1;
	int sum_permits = 0;

	for (int i = 0; i < NN; i++) {
		sum_permits += states[i].permits;
		int ticks = states[i].ticks;
		sum_ticks += ticks;
		if (ticks > max_ticks)
			max_ticks = ticks;
	}

	fprintf(stderr, "On line %d: (sum_ticks, max_ticks, sum_permits) = (%d, %d, %d)\n",
			line,
			sum_ticks, max_ticks, sum_permits);

	bool good = true;
	good &= (sum_ticks == expected_sum_ticks);
	if (expected_max_ticks >= 0)
		good &= (max_ticks == expected_max_ticks);
	good &= (sum_permits == expected_sum_permits);
	if (good)
		return;

	if (expected_max_ticks >= 0) {
		fprintf(stderr, "MISMATCH: expected (%d, %d, %d)\n",
			expected_sum_ticks, expected_max_ticks, expected_sum_permits);
	} else {
		fprintf(stderr, "MISMATCH: expected (%d, ?, %d)\n",
			expected_sum_ticks, expected_sum_permits);
	}

	for (int i = 0; i < NN; i++) {
		fprintf(stderr, "worker %d: ticks=%d permits=%d\n", i, states[i].ticks, states[i].permits);
	}
	abort();
}

#define check(expected_sum, expected_max, expected_permits) check_impl(__LINE__, expected_sum, expected_max, expected_permits)

int
main(void)
{
	MT_thread_init();

	// All code in this function runs while we hold the lock.
	// From time to time we call let_run() to allow the worker threads to obtain it.
	MT_lock_set(&lock);

	fprintf(stderr, "-- Initially, everything is zero\n");
	check(0, 0, 0);

	fprintf(stderr, "\n-- Starting the worker threads\n");
	for (int i = 0; i < NN; i++) {
		struct state *st = &states[i];
		char name[MT_NAME_LEN];
		snprintf(name, sizeof(name), "worker%d", i);
		MT_create_thread(&st->id, worker, st, MT_THR_JOINABLE, name);
	}
	MT_sleep_ms(100);

	fprintf(stderr, "\n-- Now allow the workers to take the lock, they should enter their main loop\n");
	let_run();
	check(0, 0, 0);

	fprintf(stderr, "\n-- All threads get a permit but nothing happens because we haven't touched the condvar\n");
	for (int i = 0; i < NN; i++)
		states[i].permits = 1;
	let_run();
	check(0, 0, NN);

	fprintf(stderr, "\n-- Now we broadcast on the condvar. All should wake\n");
	MT_cond_broadcast(&condvar);
	let_run();
	check(NN, 1, 0);

	fprintf(stderr, "\n-- Now we give each of them %d permits\n", NN);
	for (int i = 0; i < NN; i++) {
		states[i].ticks = 0;
		states[i].permits = NN;
	}
	check(0, 0, NN * NN);

	// Note: counting from 1 instead of 0
	for (int i = 1; i <= NN; i++) {
		fprintf(stderr, "\n-- [%d] Signal one, don't know which one it will be\n", i);
		MT_cond_signal(&condvar);
		let_run();
		check(i, -1, NN * NN - i);
	}


	fprintf(stderr, "\n-- Telling them all to quit\n");
	for (int i = 0; i < NN; i++) {
		states[i].terminate = true;
	}
	MT_cond_broadcast(&condvar);
	let_run();

	for (int i = 0; i < NN; i++) {
		fprintf(stderr, "-- Joining worker %d\n", i);
		MT_join_thread(states[i].id);
		fprintf(stderr, "-- Joined worker %d\n", i);
	}
	fprintf(stderr, "\n-- Joined all, exiting\n");

	return 0;
}
