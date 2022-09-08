/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_system.h"

#include <assert.h>

#define NN (3)

volatile int timeout = 100; // set this to 0 during interactive debugging

/* global state protected by a lock: */

MT_Lock lock = MT_LOCK_INITIALIZER(lock);
MT_Cond condvar = MT_COND_INITIALIZER(the_condvar);
struct state {
	MT_Id id;
	int ticks;
	int permits;
	bool terminate;
	bool terminated;
} states[NN] = { {0} };


static void
let_run(void)
{
	MT_lock_unset(&lock);

	MT_sleep_ms(100);

	int attempts = 0;
	while (!MT_lock_try(&lock)) {
		if (timeout > 0 && ++attempts > timeout) {
			fprintf(stderr, "Can't get hold of the lock after %d attempts\n", attempts);
			abort();
		}
		MT_sleep_ms(10);
	}

	fprintf(stderr, "\n");
}


static void
worker(void *arg)
{
	struct state *st = arg;
	int id = (int)(st - &states[0]);
	fprintf(stderr, "worker %d starting\n", id);

	MT_lock_set(&lock);
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
		fprintf(stderr, "worker %d waiting\n", id);
		MT_cond_wait(&condvar, &lock);
		fprintf(stderr, "worker %d woke up\n", id);
	}
	st->terminated = true;
	MT_lock_unset(&lock);
}


static void clear(void)
{
	for (int i = 0; i < NN; i++) {
		struct state *st = &states[i];
		st->permits = 0;
		st->ticks = 0;
	}
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

	bool good = true;
	good &= (sum_ticks == expected_sum_ticks);
	good &= (max_ticks == expected_max_ticks);
	good &= (sum_permits == expected_sum_permits);
	if (good)
		return;

	fprintf(stderr, "\nOn line %d:\n", line);
	fprintf(stderr, "Expect sum ticks to be %d, is %d\n", expected_sum_ticks, sum_ticks);
	fprintf(stderr, "Expect max ticks to be %d, is %d\n", expected_max_ticks, max_ticks);
	fprintf(stderr, "Expect sum permits to be %d, is %d\n", expected_sum_permits, sum_permits);
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

	MT_lock_set(&lock);
	check(0, 0, 0);

	for (int i = 0; i < NN; i++) {
		struct state *st = &states[i];
		char name[MT_NAME_LEN];
		snprintf(name, sizeof(name), "worker%d", i);
		MT_create_thread(&st->id, worker, st, MT_THR_JOINABLE, name);
	}
	check(0, 0, 0);

	let_run();
	check(0, 0, 0);

	// give them all a permit and broadcast on the condvar. they should all run
	for (int i = 0; i < NN; i++)
		states[i].permits = 1;
	let_run();
	// haven't notified them yet:
	check(0, 0, 3);
	MT_cond_broadcast(&condvar);
	let_run();
	check(3, 1, 0);

	// when using signal, we need to trigger them three times
	clear();
	for (int i = 0; i < NN; i++)
		states[i].permits = 1;
	let_run();
	check(0, 0, 3);
	MT_cond_signal(&condvar);
	let_run();
	check(1, 1, 2);
	MT_cond_signal(&condvar);
	let_run();
	check(2, 1, 1);
	MT_cond_signal(&condvar);
	let_run();
	check(3, 1, 0);

	for (int i = 0; i < NN; i++) {
		states[i].terminate = true;
	}
	MT_cond_broadcast(&condvar);
	let_run();

	for (int i = 0; i < NN; i++) {
		fprintf(stderr, "joining worker %d\n", i);
		MT_join_thread(states[i].id);
	}
	fprintf(stderr, "joined all, exiting\n");

	(void)worker;
	(void)let_run;
	return 0;
}
